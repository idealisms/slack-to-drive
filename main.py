#!/usr/bin/env python3
"""Sync photos and videos from a Slack channel to Google Drive."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import logging.handlers
import os
import smtplib
import sys
import traceback
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ---------------------------------------------------------------------------
# Config (all from environment variables)
# ---------------------------------------------------------------------------
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = os.environ["SLACK_CHANNEL_ID"]
GOOGLE_DRIVE_FOLDER_ID = os.environ["GOOGLE_DRIVE_FOLDER_ID"]
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
STATE_FILE = os.environ.get("STATE_FILE", "state.json")
LOG_FILE = os.environ.get("LOG_FILE", "slack-to-drive.log")

GMAIL_USER = os.environ["GMAIL_USER"]          # e.g. you@gmail.com
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
EMAIL_TO = os.environ.get("EMAIL_TO", "admin@rockymountaincoop.com")

# Lookback window on the very first run (days)
INITIAL_LOOKBACK_DAYS = int(os.environ.get("INITIAL_LOOKBACK_DAYS", "30"))

MEDIA_MIMETYPES = {
    # Images
    "image/jpeg", "image/jpg", "image/png", "image/gif",
    "image/webp", "image/heic", "image/heif", "image/tiff",
    # Videos
    "video/mp4", "video/quicktime", "video/avi", "video/mov",
    "video/x-msvideo", "video/mpeg", "video/webm",
}

# ---------------------------------------------------------------------------
# Logging — console + rotating file (one file per day, keep 30)
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("slack_to_drive")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    # Console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Rotating file — midnight rollover, keep 30 days
    fh = logging.handlers.TimedRotatingFileHandler(
        LOG_FILE, when="midnight", backupCount=30, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


log = setup_logging()

# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def send_email(subject: str, body: str) -> None:
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = EMAIL_TO

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            smtp.sendmail(GMAIL_USER, EMAIL_TO, msg.as_string())
        log.info("Email sent to %s", EMAIL_TO)
    except Exception as e:
        log.error("Failed to send email: %s", e)


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if Path(STATE_FILE).exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_run_ts": None, "uploaded_file_ids": []}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Google Drive helpers
# ---------------------------------------------------------------------------

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds)


def hash_exists_in_drive(drive_service, md5: str, filename: str, folder_id: str) -> bool:
    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    result = drive_service.files().list(
        q=q, fields="files(id,md5Checksum)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    return any(f.get("md5Checksum") == md5 for f in result.get("files", []))


def upload_to_drive(drive_service, content: bytes, filename: str, mimetype: str, folder_id: str) -> dict:
    metadata = {"name": filename, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mimetype, resumable=True)
    return drive_service.files().create(body=metadata, media_body=media, fields="id,name", supportsAllDrives=True).execute()


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------

def sync() -> tuple[int, int, int, list[str]]:
    """Returns (new_uploads, skipped, errors, uploaded_names)."""
    state = load_state()
    uploaded_ids: set[str] = set(state.get("uploaded_file_ids", []))

    if state["last_run_ts"] is not None:
        # 1-hour overlap to catch any late-delivered messages
        oldest_ts = float(state["last_run_ts"]) - 3600
    else:
        oldest_ts = (datetime.now(timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)).timestamp()
        log.info("First run — looking back %d days", INITIAL_LOOKBACK_DAYS)

    slack = WebClient(token=SLACK_BOT_TOKEN)
    drive = get_drive_service()

    new_uploads = 0
    skipped = 0
    errors = 0
    uploaded_names: list[str] = []
    error_details: list[str] = []
    cursor = None

    log.info(
        "Fetching messages from channel %s since %s",
        SLACK_CHANNEL_ID,
        datetime.fromtimestamp(oldest_ts, tz=timezone.utc).isoformat(),
    )

    while True:
        try:
            resp = slack.conversations_history(
                channel=SLACK_CHANNEL_ID,
                oldest=str(oldest_ts),
                limit=200,
                cursor=cursor,
            )
        except SlackApiError as e:
            raise RuntimeError(f"Slack API error: {e.response['error']}") from e

        for message in resp["messages"]:
            if "files" not in message:
                continue

            for f in message["files"]:
                file_id = f.get("id", "")

                if f.get("mode") == "tombstone":   # deleted file
                    continue

                mimetype = f.get("mimetype", "")
                if mimetype not in MEDIA_MIMETYPES:
                    continue

                if file_id in uploaded_ids:
                    skipped += 1
                    continue

                filename = f.get("name", f"{file_id}.bin")
                url = f.get("url_private_download") or f.get("url_private")
                if not url:
                    log.warning("No download URL for %s, skipping", filename)
                    continue

                # Download from Slack
                try:
                    dl = requests.get(
                        url,
                        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
                        timeout=120,
                    )
                    dl.raise_for_status()
                except requests.RequestException as e:
                    msg = f"Download failed for {filename}: {e}"
                    log.error(msg)
                    error_details.append(msg)
                    errors += 1
                    continue

                # Check for duplicate by content hash
                md5 = hashlib.md5(dl.content).hexdigest()
                if hash_exists_in_drive(drive, md5, filename, GOOGLE_DRIVE_FOLDER_ID):
                    log.info("Already in Drive (hash match), skipping: %s", filename)
                    uploaded_ids.add(file_id)
                    skipped += 1
                    continue

                # Upload to Drive
                try:
                    result = upload_to_drive(drive, dl.content, filename, mimetype, GOOGLE_DRIVE_FOLDER_ID)
                    log.info("Uploaded: %s → Drive ID %s", result["name"], result["id"])
                    uploaded_ids.add(file_id)
                    uploaded_names.append(filename)
                    new_uploads += 1
                except HttpError as e:
                    msg = f"Drive upload failed for {filename}: {e}"
                    log.error(msg)
                    error_details.append(msg)
                    errors += 1

        if not resp.get("has_more"):
            break
        cursor = resp["response_metadata"]["next_cursor"]

    # Cap uploaded_ids to last 10k to prevent unbounded growth
    state["last_run_ts"] = datetime.now(timezone.utc).timestamp()
    state["uploaded_file_ids"] = list(uploaded_ids)[-10_000:]
    save_state(state)

    return new_uploads, skipped, errors, uploaded_names, error_details


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    run_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log.info("=== slack-to-drive run started at %s ===", run_time)

    try:
        new_uploads, skipped, errors, uploaded_names, error_details = sync()
        status = "ERROR" if errors > 0 else "OK"

        subject = f"[slack-to-drive] {status} — {new_uploads} uploaded, {errors} errors ({run_time})"

        lines = [
            f"Run time : {run_time}",
            f"Status   : {status}",
            f"Uploaded : {new_uploads}",
            f"Skipped  : {skipped} (already archived)",
            f"Errors   : {errors}",
            "",
        ]
        if uploaded_names:
            lines.append("Files uploaded:")
            lines.extend(f"  • {name}" for name in uploaded_names)
            lines.append("")
        if error_details:
            lines.append("Errors:")
            lines.extend(f"  ✗ {e}" for e in error_details)
            lines.append("")

        body = "\n".join(lines)
        log.info("Run complete — uploaded: %d, skipped: %d, errors: %d", new_uploads, skipped, errors)

    except Exception:
        tb = traceback.format_exc()
        log.error("Unhandled exception:\n%s", tb)
        subject = f"[slack-to-drive] FAILED — unhandled exception ({run_time})"
        body = f"Run time: {run_time}\n\nUnhandled exception:\n\n{tb}"
        errors = 1

    send_email(subject, body)

    if errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

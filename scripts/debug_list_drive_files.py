#!/usr/bin/env python3
"""
Quick debug: list files in a Drive folder using a service account JSON.
Usage:
  python scripts/debug_list_drive_files.py --credentials sa.json --folder-id FOLDER_ID
"""
import argparse
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--credentials", required=True)
    p.add_argument("--folder-id", default='1cc7nHtHuHpkrhTLjKxzStRE8wN2a-1Ia')
    args = p.parse_args()

    creds = service_account.Credentials.from_service_account_file(args.credentials, scopes=SCOPES)
    print("Service account email:", creds.service_account_email)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    q = f"'{args.folder_id}' in parents and trashed=false"
    resp = service.files().list(q=q, pageSize=200, fields="files(id,name,mimeType,modifiedTime,owners)").execute()
    files = resp.get("files", [])
    if not files:
        print("No files visible in folder (service account likely lacks access or folder-id is wrong).")
        return
    print(f"Found {len(files)} files. Showing metadata:")
    for f in files:
        print(f"- id={f['id']} name={f['name']} mimeType={f['mimeType']} modifiedTime={f.get('modifiedTime')} owners={f.get('owners')}")

if __name__ == '__main__':
    main()
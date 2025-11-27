"""
DriveClient: find the newest CSV in a folder, read it into memory (bytes) or save to file.
Uses a Google service account JSON file for credentials.

If you prefer OAuth (user account), replace credential creation with an OAuth flow.

Usage:
    DriveClient(service_account_file="sa.json").get_newest_csv_bytes(folder_id)
"""
from __future__ import annotations
import io
from typing import Optional, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


class DriveClient:
    def __init__(self, service_account_file: Optional[str] = None, scopes: Optional[list] = None):
        scopes = scopes or SCOPES
        if service_account_file:
            creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
        else:
            raise ValueError("service_account_file required for DriveClient")
        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)

    def get_newest_csv_file_metadata(self, folder_id: str) -> Optional[dict]:
        """
        Returns file resource dict for newest CSV in folder (by modifiedTime), or None.
        """
        # Query: files in folder and mimeType csv or name endswith .csv
        q = f"'{folder_id}' in parents and (mimeType='text/csv' or name contains '.csv') and trashed=false"
        # order by modifiedTime desc
        results = self.service.files().list(q=q, orderBy="modifiedTime desc", pageSize=1,
                                            fields="files(id,name,mimeType,modifiedTime,size)").execute()
        files = results.get("files", [])
        return files[0] if files else None

    def get_file_bytes(self, file_id: str) -> bytes:
        """
        Downloads a file's content and returns bytes.
        """
        fh = io.BytesIO()
        request = self.service.files().get_media(fileId=file_id)
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read()

    def get_newest_csv_bytes(self, folder_id: str) -> Tuple[Optional[str], Optional[bytes]]:
        """
        Returns (filename, bytes) of newest CSV in folder, or (None, None).
        """
        meta = self.get_newest_csv_file_metadata(folder_id)
        if not meta:
            return None, None
        content = self.get_file_bytes(meta["id"])
        return meta.get("name"), content
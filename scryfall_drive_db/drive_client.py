"""
DriveClient: find the newest CSV in a folder, read it into memory (bytes) or save to file.
Improvements:
- Lists files in the folder and prints metadata (for debugging).
- Supports Google Sheets by exporting them as CSV.
- Accepts files whose name endswith '.csv' even if mimeType is not 'text/csv'.
- Returns the first matching file by modifiedTime desc.

Usage:
    DriveClient(service_account_file="sa.json").get_newest_csv_bytes(folder_id)
"""
from __future__ import annotations
import io
from typing import Optional, Tuple, List
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


class DriveClient:
    def __init__(self, service_account_file: Optional[str] = None, scopes: Optional[list] = None):
        scopes = scopes or SCOPES
        if service_account_file:
            creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
            self.service_account_email = creds.service_account_email  # useful for debugging / sharing folder
        else:
            raise ValueError("service_account_file required for DriveClient")
        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)

    def list_files_in_folder(self, folder_id: str, page_size: int = 50) -> List[dict]:
        """
        Return a list of file metadata in the folder (ordered by modifiedTime desc).
        Useful for debugging what the service account can see.
        """
        q = f"'{folder_id}' in parents and trashed=false"
        results = self.service.files().list(q=q, orderBy="modifiedTime desc", pageSize=page_size,
                                            fields="files(id,name,mimeType,modifiedTime,owners)").execute()
        files = results.get("files", [])
        return files

    def get_newest_csv_file_metadata(self, folder_id: str) -> Optional[dict]:
        """
        Returns file resource dict for newest CSV-like file in folder, or None.

        This will consider:
          - mimeType == 'text/csv'
          - name endswith '.csv'
          - mimeType == Google Sheets ('application/vnd.google-apps.spreadsheet'),
            which we will later export as CSV.
        """
        # list a reasonable number of recently modified files and look for candidates
        files = self.list_files_in_folder(folder_id, page_size=100)
        for f in files:
            name = f.get("name", "")
            mt = f.get("mimeType", "")
            # candidate conditions:
            if mt == "text/csv":
                return f
            if name.lower().endswith(".csv"):
                return f
            if mt == "application/vnd.google-apps.spreadsheet":
                return f
        return None

    def _download_media_to_bytes(self, request) -> bytes:
        """
        Download a files().get_media() or files().export_media() request using MediaIoBaseDownload.
        """
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read()

    def get_file_bytes(self, file_id: str, mime_type: Optional[str] = None) -> bytes:
        """
        Downloads a file's content and returns bytes.
        For native files (Google Sheets) the caller should use export (see get_newest_csv_bytes).
        """
        # For regular files use get_media
        request = self.service.files().get_media(fileId=file_id)
        return self._download_media_to_bytes(request)

    def get_newest_csv_bytes(self, folder_id: str) -> Tuple[Optional[str], Optional[bytes]]:
        """
        Returns (filename, bytes) of newest CSV-like file in folder, or (None, None).
        Handles:
          - CSV files (mimeType text/csv)
          - Files whose name ends with .csv
          - Google Sheets (export as CSV)
        """
        meta = self.get_newest_csv_file_metadata(folder_id)
        if not meta:
            return None, None

        file_id = meta["id"]
        name = meta.get("name", file_id)
        mime_type = meta.get("mimeType", "")

        # If it's a Google Sheet, export as CSV
        if mime_type == "application/vnd.google-apps.spreadsheet":
            # export as CSV
            request = self.service.files().export_media(fileId=file_id, mimeType="text/csv")
            content = self._download_media_to_bytes(request)
            return name, content

        # If it's a normal CSV-like file, download
        try:
            request = self.service.files().get_media(fileId=file_id)
            content = self._download_media_to_bytes(request)
            return name, content
        except Exception:
            # fallback: try export (some files support export)
            try:
                request = self.service.files().export_media(fileId=file_id, mimeType="text/csv")
                content = self._download_media_to_bytes(request)
                return name, content
            except Exception:
                return None, None
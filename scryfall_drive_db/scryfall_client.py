"""
Simple Scryfall client for /cards/:code/:number(/:lang)
"""
from __future__ import annotations
from typing import Optional, Dict, Any
import requests
import time


class ScryfallClient:
    BASE = "https://api.scryfall.com"

    def __init__(self, session: Optional[requests.Session] = None, max_retries: int = 3, backoff: float = 1.0):
        self.session = session or requests.Session()
        self.max_retries = max_retries
        self.backoff = backoff

    def _get(self, path: str) -> Dict[str, Any]: # type: ignore
        url = f"{self.BASE}{path}"
        for attempt in range(1, self.max_retries + 1):
            resp = self.session.get(url, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                # retryable
                time.sleep(self.backoff * attempt)
                continue
            # non-retryable
            resp.raise_for_status()
        # if we exit loop, raise last
        resp.raise_for_status() # type: ignore

    def get_card(self, set_code: str, number: str, lang: Optional[str] = None) -> Dict[str, Any]:
        """
        Call /cards/{set_code}/{number} or /cards/{set_code}/{number}/{lang}
        Returns parsed JSON dict.
        """
        set_code = str(set_code)
        number = str(number)
        if lang:
            path = f"/cards/{set_code}/{number}/{lang}"
        else:
            path = f"/cards/{set_code}/{number}"
        return self._get(path)
"""
Prefetch Scryfall JSON for many (set_code, number, lang) keys in parallel.

Usage:
    from scryfall_drive_db.prefetch import prefetch_cards
    cache = prefetch_cards(scry_client, keys, max_workers=8, max_retries=3, backoff=0.25, logger=logger)

Returns:
    dict mapping (set_code, collector_number, lang) -> JSON dict (on success) or None (on failure)
"""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Tuple, Dict, Optional
import time
import logging

Key = Tuple[str, str, str]  # (set_code, collector_number, lang)


def _fetch_one(scry, key: Key, max_retries: int, backoff: float, logger: logging.Logger) -> Tuple[Key, Optional[dict]]:
    set_code, collector_number, lang = key
    attempt = 0
    while attempt <= max_retries:
        try:
            lang_param = lang or None
            js = scry.get_card(set_code, collector_number, lang_param)
            return key, js
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                logger.warning("Prefetch failed for %s/%s/%s after %d attempts: %s", set_code, collector_number, lang or "", attempt - 1, e)
                return key, None
            # small backoff before retrying
            sleep_for = backoff * attempt
            time.sleep(sleep_for)
    return key, None


def prefetch_cards(scry, keys: Iterable[Key], max_workers: int = 8, max_retries: int = 3, backoff: float = 0.25,
                   logger: Optional[logging.Logger] = None) -> Dict[Key, Optional[dict]]:
    """
    Prefetch many Scryfall lookups in parallel.

    - keys: iterable of (set_code, collector_number, lang)
    - returns: dict mapping normalized keys -> json or None

    Normalizes keys to (str(set_code).strip(), str(collector_number).strip(), (lang or "").strip()).
    """
    logger = logger or logging.getLogger("mtg_collector")
    unique_keys = []
    seen = set()
    for k in keys:
        norm = (str(k[0]).strip(), str(k[1]).strip(), (k[2] or "").strip())
        if norm not in seen:
            seen.add(norm)
            unique_keys.append(norm)

    cache: Dict[Key, Optional[dict]] = {}

    if not unique_keys:
        return cache

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_fetch_one, scry, key, max_retries, backoff, logger): key for key in unique_keys}
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                k, js = fut.result()
                cache[k] = js
            except Exception as e:
                logger.exception("Unexpected prefetch exception for %s: %s", key, e)
                cache[key] = None

    return cache
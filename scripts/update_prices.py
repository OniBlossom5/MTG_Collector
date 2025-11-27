#!/usr/bin/env python3
"""
Update prices utility.

Usage:
  python scripts/update_prices.py --db cards.db
  python scripts/update_prices.py --db cards.db --price-field usd_foil --min-interval 0.1 --chunk-size 200
  python scripts/update_prices.py --db cards.db --dry-run

Notes:
- Default price field used for updates: usd
- The script respects a global minimum interval between Scryfall requests (default 0.08s)
  to stay inside Scryfall's guidance of 50-100ms between calls (about 10 req/s).
- Default DB path: cards.db (change with --db)
"""
from __future__ import annotations
import argparse
import csv
import logging
import math
import sqlite3
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from scryfall_drive_db.scryfall_client import ScryfallClient
from scryfall_drive_db.db_manager import DBManager
from tqdm import tqdm

# Configure a simple logger
logger = logging.getLogger("price_updater")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(ch)


class RateLimiter:
    """
    Simple global rate limiter that enforces a minimum interval between calls.
    Use limiter.wait() before making a request to ensure we don't exceed the rate.
    This serializes request timing across threads; it is safe and simple.
    """
    def __init__(self, min_interval_seconds: float = 0.08):
        self.min_interval = float(min_interval_seconds)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            elapsed = now - self._last
            sleep_for = self.min_interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
                now = time.time()
            self._last = now


def select_price_from_json(js: Dict, price_field: str) -> Optional[float]:
    """
    Given Scryfall JSON and a field name (usd | usd_foil | usd_etched),
    return a float or None.
    """
    if not js:
        return None
    prices = js.get("prices") or {}
    val = prices.get(price_field)
    if val in (None, "", "null"):
        return None
    try:
        return float(val)
    except Exception:
        return None


def fetch_all_cards(db: DBManager) -> List[Dict]:
    """
    Uses DBManager.list_all() to get all cards from the cards table.
    List items include: id, set_code, collector_number, lang, name, color_identity, price_usd, location
    """
    rows = db.list_all()
    return rows


def run_update(db_path: str,
               price_field: str = "usd",
               min_interval: float = 0.08,
               chunk_size: int = 200,
               dry_run: bool = False,
               csv_out_dir: Optional[str] = None):
    db = DBManager(db_path)
    rows = fetch_all_cards(db)
    total = len(rows)
    logger.info("Loaded %d cards from DB '%s'.", total, db_path)
    if total == 0:
        return

    scry = ScryfallClient()
    limiter = RateLimiter(min_interval)

    # Prepare outputs
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(csv_out_dir) if csv_out_dir else Path.cwd()
    out_dir.mkdir(parents=True, exist_ok=True)
    downgraded_csv = out_dir / f"downgraded_{ts}.csv"
    upgraded_csv = out_dir / f"upgraded_{ts}.csv"

    downgraded_rows: List[Tuple] = []
    upgraded_rows: List[Tuple] = []
    updates: List[Tuple[Optional[float], str, int]] = []  # (new_price, new_location, id)

    # We'll write updates in batches for speed
    # Use direct sqlite3 to perform batched UPDATE statements
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        pbar = tqdm(total=total, unit="card", desc="Checking prices", dynamic_ncols=True)
        try:
            for row in rows:
                card_id = row["id"]
                set_code = row.get("set_code")
                collector_number = row.get("collector_number")
                lang = row.get("lang") or ""
                old_price = row.get("price_usd")
                old_location = row.get("location") or ""

                # ensure we respect rate-limiting for Scryfall
                limiter.wait()
                try:
                    # Scryfall call
                    js = scry.get_card(set_code, collector_number, lang or None) # type: ignore
                except Exception as e:
                    logger.warning("Scryfall fetch failed for %s/%s/%s: %s", set_code, collector_number, lang or "", e)
                    pbar.update(1)
                    continue

                new_price = select_price_from_json(js, price_field)

                # Normalize None to None, numeric otherwise
                old_val = float(old_price) if old_price is not None else None
                new_val = float(new_price) if new_price is not None else None

                # Comparison: treat None as 0 for threshold comparisons (you can change this behavior)
                old_cmp = old_val if old_val is not None else 0.0
                new_cmp = new_val if new_val is not None else 0.0

                changed = False

                # old >= 5 and new < 5 => set location to 'bulk' and update price
                if old_cmp >= 5.0 and new_cmp < 5.0:
                    new_location = "bulk"
                    updates.append((new_val, new_location, card_id))
                    downgraded_rows.append((card_id, set_code, collector_number, lang, old_val, new_val, old_location, new_location))
                    changed = True
                # old < 5 and new >= 5 => keep location 'bulk' and update price
                elif old_cmp < 5.0 and new_cmp >= 5.0:
                    # keep location as 'bulk' per your spec
                    new_location = "bulk"
                    updates.append((new_val, new_location, card_id))
                    upgraded_rows.append((card_id, set_code, collector_number, lang, old_val, new_val, old_location, new_location))
                    changed = True
                else:
                    # If price changed but didn't cross threshold, update the price only (optional)
                    # If you want to update all price changes even if threshold not crossed, uncomment:
                    # if (old_val is None and new_val is not None) or (old_val is not None and new_val is not None and not math.isclose(old_val, new_val)):
                    #     updates.append((new_val, old_location, card_id))
                    pass

                # Flush updates in chunks for performance (do not commit each row)
                if len(updates) >= chunk_size:
                    if not dry_run:
                        _apply_update_batch(conn, updates)
                    updates.clear()

                pbar.update(1)
        finally:
            pbar.close()

        # Final batch
        if updates:
            if not dry_run:
                _apply_update_batch(conn, updates)
            updates.clear()

        # Write CSVs
        _write_csv(downgraded_csv, downgraded_rows)
        _write_csv(upgraded_csv, upgraded_rows)
        logger.info("Wrote %d downgraded rows to %s", len(downgraded_rows), downgraded_csv)
        logger.info("Wrote %d upgraded rows to %s", len(upgraded_rows), upgraded_csv)

        if dry_run:
            logger.info("Dry-run mode: no DB changes were applied.")
        else:
            logger.info("DB updates applied.")
    finally:
        conn.close()


def _apply_update_batch(conn: sqlite3.Connection, updates: List[Tuple[Optional[float], str, int]]):
    """
    updates: list of (new_price (float or None), new_location (str), id(int))
    Runs a single transaction with executemany.
    """
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        sql = "UPDATE cards SET price_usd = ?, location = ? WHERE id = ?"
        params = []
        for new_price, new_location, card_id in updates:
            params.append((new_price, new_location, card_id))
        cur.executemany(sql, params)
        conn.commit()
        logger.info("Applied batch update of %d rows.", len(params))
    except Exception:
        conn.rollback()
        logger.exception("Failed to apply update batch.")
        raise


def _write_csv(path: Path, rows: List[Tuple]):
    """
    Writes rows to the path. Rows are tuples:
      (id, set_code, collector_number, lang, old_price, new_price, old_location, new_location)
    """
    if not rows:
        # create empty file with header for clarity
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["id", "set_code", "collector_number", "lang", "old_price", "new_price", "old_location", "new_location"])
        return

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["id", "set_code", "collector_number", "lang", "old_price", "new_price", "old_location", "new_location"])
        for r in rows:
            writer.writerow(list(r))


def parse_args():
    p = argparse.ArgumentParser(description="Update card prices from Scryfall and export threshold-crossing changes.")
    p.add_argument("--db", default="cards.db", help="Path to sqlite DB (default: cards.db)")
    p.add_argument("--price-field", choices=["usd", "usd_foil", "usd_etched"], default="usd",
                   help="Which Scryfall price field to use when comparing/setting price (default: usd)")
    p.add_argument("--min-interval", type=float, default=0.08,
                   help="Minimum seconds between Scryfall requests (default 0.08s ~ 80ms)")
    p.add_argument("--chunk-size", type=int, default=200, help="Batch update chunk size for DB writes (default 200)")
    p.add_argument("--dry-run", action="store_true", help="Do not apply DB updates; only write CSVs")
    p.add_argument("--csv-out-dir", default=None, help="Directory to write CSV outputs (defaults to current directory)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_update(db_path=args.db,
               price_field=args.price_field,
               min_interval=args.min_interval,
               chunk_size=args.chunk_size,
               dry_run=args.dry_run,
               csv_out_dir=args.csv_out_dir)
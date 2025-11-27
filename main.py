#!/usr/bin/env python3
"""
Main CLI with parallel Scryfall prefetch, batch inserts, and tqdm progress bar.

New CLI flags:
  --chunk-size INT        Batch insert size (default 500)
  --prefetch-workers INT  Number of parallel workers for Scryfall prefetch (default 8)
  --no-prefetch           Disable prefetch: fall back to the previous (per-row/cached) behavior

Existing defaults preserved:
  folder-id default: '1cc7nHtHuHpkrhTLjKxzStRE8wN2a-1Ia'
  location default: 'bulk'
  set-col default: 'Set code'
  num-col default: 'Collector number'
  lang-col default: 'Language'
  foil-col default: 'Foil'
  qty-col default: 'Quantity'
"""
from __future__ import annotations
import argparse
import csv
import io
import sys
import logging
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from collections import namedtuple

from scryfall_drive_db.drive_client import DriveClient
from scryfall_drive_db.scryfall_client import ScryfallClient
from scryfall_drive_db.db_manager import DBManager
from scryfall_drive_db.prefetch import prefetch_cards

from tqdm import tqdm


def setup_logging(log_file: str, verbose: bool) -> logging.Logger:
    logger = logging.getLogger("mtg_collector")
    logger.setLevel(logging.INFO)

    # File handler (INFO+)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(fh)

    # Console handler (WARNING+ unless verbose)
    ch = logging.StreamHandler(sys.stderr)
    ch_level = logging.INFO if verbose else logging.WARNING
    ch.setLevel(ch_level)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(ch)

    return logger


def detect_column(row: Dict[str, str], candidates: list) -> Optional[str]:
    lower_to_actual = {k.lower(): k for k in row.keys()}
    for cand in candidates:
        if cand.lower() in lower_to_actual:
            return lower_to_actual[cand.lower()]
    return None


def parse_quantity(value: Optional[str]) -> int:
    if value is None:
        return 1
    v = str(value).strip()
    if v == "":
        return 1
    try:
        q = int(float(v))
    except Exception:
        return 1
    return max(q, 0)


CsvRow = namedtuple("CsvRow", ["row", "set_col", "num_col", "lang_col", "foil_col", "qty_col"])


def load_csv_rows(content: bytes, set_col: str, num_col: str, lang_col: Optional[str],
                  foil_col: Optional[str], qty_col: Optional[str]) -> Tuple[List[CsvRow], int]:
    s = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(s))
    rows: List[CsvRow] = []
    first = None
    total_qty = 0
    detected_set, detected_num, detected_lang, detected_foil, detected_qty = set_col, num_col, lang_col, foil_col, qty_col

    for row in reader:
        if first is None:
            first = row
            if detected_set not in row:
                found = detect_column(row, [set_col, "set_code", "set", "setcode"])
                if found:
                    detected_set = found
            if detected_num not in row:
                found = detect_column(row, [num_col, "collector_number", "collector#","number"])
                if found:
                    detected_num = found
            if detected_lang and detected_lang not in row:
                found = detect_column(row, [detected_lang, "language", "lang"])
                if found:
                    detected_lang = found
            if detected_foil and detected_foil not in row:
                found = detect_column(row, [detected_foil, "foil", "is_foil", "etched"])
                if found:
                    detected_foil = found
            if detected_qty and detected_qty not in row:
                found = detect_column(row, [detected_qty, "quantity", "qty", "count"])
                if found:
                    detected_qty = found

        qty_val = row.get(detected_qty) if detected_qty else None
        qty = parse_quantity(qty_val)
        total_qty += max(qty, 0)
        rows.append(CsvRow(row=row, set_col=detected_set, num_col=detected_num,
                           lang_col=detected_lang, foil_col=detected_foil, qty_col=detected_qty))
    return rows, total_qty


def pick_price_from_json(json: Dict[str, Any], foil_value: Optional[str]) -> Optional[float]:
    prices = json.get("prices", {}) or {}
    fv = (foil_value or "normal").strip().lower()
    price = None
    if fv == "foil":
        price = prices.get("usd_foil")
    elif fv == "etched":
        price = prices.get("usd_etched")
    else:
        price = prices.get("usd")
    try:
        return float(price) if price not in (None, "", "null") else None
    except Exception:
        return None


def process_append(content_bytes: bytes, db: DBManager, scry: ScryfallClient,
                   set_col: str, num_col: str, lang_col: Optional[str], foil_col: Optional[str],
                   qty_col: Optional[str], default_location: str, logger: logging.Logger,
                   chunk_size: int = 500, prefetch_workers: int = 8, do_prefetch: bool = True):
    rows, total_qty = load_csv_rows(content_bytes, set_col, num_col, lang_col, foil_col, qty_col)
    logger.info("Loaded %d rows (total quantity=%d)", len(rows), total_qty)
    if total_qty == 0:
        print("No items to append (total quantity is 0).")
        return

    # Build unique keys from rows
    unique_keys = []
    seen = set()
    for cr in rows:
        row = cr.row
        sc = row.get(cr.set_col)
        cn = row.get(cr.num_col)
        lang = (row.get(cr.lang_col) if cr.lang_col else "") or ""
        if not sc or not cn:
            continue
        key = (str(sc).strip(), str(cn).strip(), str(lang).strip())
        if key not in seen:
            seen.add(key)
            unique_keys.append(key)

    cache: Dict[Tuple[str, str, str], Optional[Dict[str, Any]]] = {}

    if do_prefetch:
        logger.info("Prefetching %d unique Scryfall lookups with %d workers", len(unique_keys), prefetch_workers)
        cache = prefetch_cards(scry, unique_keys, max_workers=prefetch_workers, logger=logger)
    else:
        logger.info("Prefetch disabled; will fetch on-demand with caching")

    batch: List[Dict[str, Any]] = []
    processed = 0

    pbar = tqdm(total=total_qty, unit="card", desc="Appending", dynamic_ncols=True)
    try:
        for cr in rows:
            row = cr.row
            set_code = (row.get(cr.set_col) if cr.set_col else None)
            collector_number = (row.get(cr.num_col) if cr.num_col else None)
            lang = (row.get(cr.lang_col) if cr.lang_col else None) or ""
            foil_value = (row.get(cr.foil_col) if cr.foil_col else None)
            qty_val = row.get(cr.qty_col) if cr.qty_col else None
            quantity = parse_quantity(qty_val)

            if not set_code or not collector_number:
                logger.warning("Skipping row missing set_code or collector_number: %s", row)
                pbar.update(max(quantity, 0))
                continue
            if quantity <= 0:
                logger.warning("Skipping %s/%s because quantity=%s", set_code, collector_number, quantity)
                continue

            key = (set_code.strip(), collector_number.strip(), (lang or "").strip())
            js = None

            if do_prefetch:
                js = cache.get(key)
                if js is None:
                    logger.warning("No prefetch result or prefetch failed for %s/%s/%s; skipping", key[0], key[1], key[2] or "")
                    pbar.update(quantity)
                    processed += quantity
                    continue
            else:
                # on-demand fetch with caching
                if key in cache:
                    js = cache[key]
                else:
                    try:
                        js = scry.get_card(key[0], key[1], key[2] or None)
                        cache[key] = js
                    except Exception as e:
                        logger.warning("Failed to fetch %s/%s/%s: %s", key[0], key[1], key[2] or "", e)
                        pbar.update(quantity)
                        processed += quantity
                        continue

            name = js.get("name") # type: ignore
            color_identity = js.get("color_identity", []) # type: ignore
            price_usd = pick_price_from_json(js, foil_value) # type: ignore

            for _ in range(quantity):
                entry = {
                    "set_code": key[0],
                    "collector_number": key[1],
                    "lang": key[2],
                    "name": name,
                    "color_identity": color_identity,
                    "price_usd": price_usd,
                    "location": default_location
                }
                batch.append(entry)

                if len(batch) >= chunk_size:
                    n_inserted, last_rowid = db.add_entries(batch)
                    logger.info("Batch inserted %d entries (last_rowid=%s)", n_inserted, last_rowid)
                    batch.clear()

                pbar.update(1)
                processed += 1

        # final flush
        if batch:
            n_inserted, last_rowid = db.add_entries(batch)
            logger.info("Final batch inserted %d entries (last_rowid=%s)", n_inserted, last_rowid)
            batch.clear()
    finally:
        pbar.close()

    logger.info("Appended %d total entries (processed attempts=%d)", processed, total_qty)
    print(f"Appended {processed} total entries.")


def process_remove(content_bytes: bytes, db: DBManager, set_col: str, num_col: str, lang_col: Optional[str],
                   foil_col: Optional[str], qty_col: Optional[str], logger: logging.Logger):
    rows, total_qty = load_csv_rows(content_bytes, set_col, num_col, lang_col, foil_col, qty_col)
    logger.info("Loaded %d rows for removal (total quantity=%d)", len(rows), total_qty)
    if total_qty == 0:
        print("No items to remove (total quantity is 0).")
        return

    removed_total = 0
    pbar = tqdm(total=total_qty, unit="card", desc="Removing", dynamic_ncols=True)
    try:
        for cr in rows:
            row = cr.row
            set_code = (row.get(cr.set_col) if cr.set_col else None)
            collector_number = (row.get(cr.num_col) if cr.num_col else None)
            lang = (row.get(cr.lang_col) if cr.lang_col else None)
            qty_val = row.get(cr.qty_col) if cr.qty_col else None
            quantity = parse_quantity(qty_val)

            if not set_code or not collector_number:
                logger.warning("Skipping row missing set_code or collector_number: %s", row)
                pbar.update(quantity)
                continue
            if quantity <= 0:
                logger.warning("Skipping removal for %s/%s because quantity=%s", set_code, collector_number, quantity)
                pbar.update(0)
                continue

            removed_for_row = 0
            for _ in range(quantity):
                removed_id = db.remove_first_matching(set_code.strip(), collector_number.strip(),
                                                      (lang or "").strip() if lang else None)
                pbar.update(1)
                if removed_id:
                    removed_for_row += 1
                    removed_total += 1
                    logger.info("Removed id=%s for %s/%s/%s", removed_id, set_code, collector_number, lang or "")
                else:
                    if removed_for_row == 0:
                        logger.info("No match found for %s/%s/%s", set_code, collector_number, lang or "")
                    else:
                        logger.info("Stopped after removing %d entries for %s/%s/%s (no more matches).",
                                    removed_for_row, set_code, collector_number, lang or "")
                    break
    finally:
        pbar.close()

    logger.info("Removed %d total entries.", removed_total)
    print(f"Removed {removed_total} total entries.")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["append", "remove"], help="append to DB or remove entries")
    p.add_argument("--folder-id", default='1cc7nHtHuHpkrhTLjKxzStRE8wN2a-1Ia', help="Google Drive folder ID to look for newest CSV")
    p.add_argument("--credentials", help="Path to service account JSON for Drive (required if using folder-id)")
    p.add_argument("--local-csv", help="Path to a local CSV; if provided, Drive is not used")
    p.add_argument("--db", default="cards.db", help="Path to sqlite DB")
    p.add_argument("--location", default="bulk", choices=["binder", "personal", "bulk"], help="Location value to store on append")
    p.add_argument("--set-col", default="Set code", help="CSV column for set code")
    p.add_argument("--num-col", default="Collector number", help="CSV column for collector number")
    p.add_argument("--lang-col", default="Language", help="CSV column for language (optional)")
    p.add_argument("--foil-col", default="Foil", help="CSV column indicating foil/etched/normal")
    p.add_argument("--qty-col", default="Quantity", help="CSV column indicating quantity (integer, default 1)")
    p.add_argument("--log-file", default="mtg_collector.log", help="Path to log file for detailed per-row logs")
    p.add_argument("--verbose", action="store_true", help="Also print INFO messages to console (per-row actions)")
    p.add_argument("--chunk-size", type=int, default=500, help="Batch insert chunk size (default 500)")
    p.add_argument("--prefetch-workers", type=int, default=8, help="Number of parallel workers for Scryfall prefetch")
    p.add_argument("--no-prefetch", action="store_true", help="Disable parallel prefetch; fetch on-demand (cached)")
    args = p.parse_args()

    if not args.local_csv and not args.folder_id:
        print("Either --local-csv or --folder-id must be provided", file=sys.stderr)
        sys.exit(2)

    logger = setup_logging(args.log_file, args.verbose)
    logger.info("Starting mode=%s db=%s", args.mode, args.db)

    db = DBManager(args.db)
    scry = ScryfallClient()

    content_bytes = None
    if args.local_csv:
        try:
            with open(args.local_csv, "rb") as fh:
                content_bytes = fh.read()
        except Exception as e:
            logger.error("Failed to read local CSV %s: %s", args.local_csv, e)
            print("Failed to read local CSV. See log for details.", file=sys.stderr)
            sys.exit(1)
    else:
        # use Drive
        if not args.credentials:
            print("--credentials is required when using --folder-id", file=sys.stderr)
            sys.exit(2)
        drive = DriveClient(service_account_file=args.credentials)
        name, content = drive.get_newest_csv_bytes(args.folder_id)
        if content is None:
            logger.error("No CSV found in folder %s (service account may lack access or folder is empty)", args.folder_id)
            print("No CSV found in folder", file=sys.stderr)
            sys.exit(1)
        content_bytes = content
        print(f"Read newest file from Drive: {name}")
        logger.info("Read newest file from Drive: %s", name)

    do_prefetch = not args.no_prefetch

    if args.mode == "append":
        process_append(content_bytes, db, scry, args.set_col, args.num_col, args.lang_col, args.foil_col, args.qty_col, args.location, logger, chunk_size=args.chunk_size, prefetch_workers=args.prefetch_workers, do_prefetch=do_prefetch)
    else:
        process_remove(content_bytes, db, args.set_col, args.num_col, args.lang_col, args.foil_col, args.qty_col, logger)


if __name__ == "__main__":
    main()
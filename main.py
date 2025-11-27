#!/usr/bin/env python3
"""
Command-line entrypoint.

Modes: append, remove

Examples:
  python cli.py append --folder-id FOLDER_ID --credentials sa.json --db cards.db --location personal
  python cli.py remove --local-csv /tmp/to_remove.csv --db cards.db

CSV column mapping defaults (case-insensitive):
  set_code -> set_code
  collector_number -> collector_number
  language -> language
  foil -> foil   (values: normal | foil | etched)
  quantity -> quantity (integer, default 1)

Logging:
  - Detailed per-row actions (inserted ids, removed ids, fetch failures, etc.) are written to a log file
    specified with --log-file (default: mtg_collector.log).
  - Console output is kept concise: errors/warnings are shown; a final summary is printed.
  - Pass --verbose to also show per-row INFO messages on the console.

If local CSV is provided, Drive is not used.
"""
from __future__ import annotations
import argparse
import csv
import io
import sys
import logging
from typing import Optional, Dict, Any
from datetime import datetime

from scryfall_drive_db.drive_client import DriveClient
from scryfall_drive_db.scryfall_client import ScryfallClient
from scryfall_drive_db.db_manager import DBManager


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
    """
    Given a CSV row keys and candidate names, return the first matching actual key (case-insensitive)
    """
    lower_to_actual = {k.lower(): k for k in row.keys()}
    for cand in candidates:
        if cand.lower() in lower_to_actual:
            return lower_to_actual[cand.lower()]
    return None


def parse_quantity(value: Optional[str]) -> int:
    """
    Safely parse quantity. Default 1 on missing/unparseable; floor for floats; negative or zero -> 0.
    """
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


def iterate_csv_bytes(content: bytes, set_col: str, num_col: str, lang_col: Optional[str],
                      foil_col: Optional[str], qty_col: Optional[str]):
    s = content.decode("utf-8-sig")  # handle BOM
    reader = csv.DictReader(io.StringIO(s))
    # detect actual column names from header if given names aren't present
    first = None
    for row in reader:
        if first is None:
            first = row
            # map columns if the provided ones aren't exactly present
            if set_col not in row:
                found = detect_column(row, [set_col, "set_code", "set", "setcode"])
                if found:
                    set_col = found
            if num_col not in row:
                found = detect_column(row, [num_col, "collector_number", "collector#","number"])
                if found:
                    num_col = found
            if lang_col and lang_col not in row:
                found = detect_column(row, [lang_col, "language", "lang"])
                if found:
                    lang_col = found
            if foil_col and foil_col not in row:
                found = detect_column(row, [foil_col, "foil", "is_foil", "etched"])
                if found:
                    foil_col = found
            if qty_col and qty_col not in row:
                found = detect_column(row, [qty_col, "quantity", "qty", "count"])
                if found:
                    qty_col = found
        yield row, set_col, num_col, lang_col, foil_col, qty_col


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
                   qty_col: Optional[str], default_location: str, logger: logging.Logger):
    total_inserted = 0
    for row, set_col, num_col, lang_col, foil_col, qty_col in iterate_csv_bytes(
            content_bytes, set_col, num_col, lang_col, foil_col, qty_col):
        set_code = (row.get(set_col) if set_col else None)
        collector_number = (row.get(num_col) if num_col else None)
        lang = (row.get(lang_col) if lang_col else None)
        foil_value = (row.get(foil_col) if foil_col else None)
        qty_val = row.get(qty_col) if qty_col else None
        quantity = parse_quantity(qty_val)

        if not set_code or not collector_number:
            logger.warning("Skipping row missing set_code or collector_number: %s", row)
            continue
        if quantity <= 0:
            logger.warning("Skipping %s/%s because quantity=%s", set_code, collector_number, quantity)
            continue

        try:
            js = scry.get_card(set_code.strip(), collector_number.strip(), lang.strip() if lang else None)
        except Exception as e:
            logger.warning("Failed to fetch %s/%s/%s: %s", set_code, collector_number, lang or "", e)
            continue

        name = js.get("name")
        color_identity = js.get("color_identity", [])
        price_usd = pick_price_from_json(js, foil_value)

        # Insert quantity times, but only fetch Scryfall once
        for i in range(quantity):
            entry = {
                "set_code": set_code.strip(),
                "collector_number": collector_number.strip(),
                "lang": (lang or "").strip(),
                "name": name,
                "color_identity": color_identity,
                "price_usd": price_usd,
                "location": default_location
            }
            try:
                row_id = db.add_entry(entry)
                total_inserted += 1
                # Log the detailed per-insert line to the file (INFO). It will only appear on console if --verbose.
                logger.info("Inserted id=%s [%d/%d] %s/%s/%s -> %s", row_id, i+1, quantity,
                            set_code, collector_number, lang or "", name)
            except Exception as e:
                logger.error("Failed to insert entry for %s/%s: %s", set_code, collector_number, e)
    # concise summary to console (and logged)
    logger.info("Appended %d total entries.", total_inserted)
    print(f"Appended {total_inserted} total entries.")


def process_remove(content_bytes: bytes, db: DBManager, set_col: str, num_col: str, lang_col: Optional[str],
                   foil_col: Optional[str], qty_col: Optional[str], logger: logging.Logger):
    total_removed = 0
    for row, set_col, num_col, lang_col, foil_col, qty_col in iterate_csv_bytes(
            content_bytes, set_col, num_col, lang_col, foil_col, qty_col):
        set_code = (row.get(set_col) if set_col else None)
        collector_number = (row.get(num_col) if num_col else None)
        lang = (row.get(lang_col) if lang_col else None)
        qty_val = row.get(qty_col) if qty_col else None
        quantity = parse_quantity(qty_val)

        if not set_code or not collector_number:
            logger.warning("Skipping row missing set_code or collector_number: %s", row)
            continue
        if quantity <= 0:
            logger.warning("Skipping removal for %s/%s because quantity=%s", set_code, collector_number, quantity)
            continue

        removed_for_row = 0
        for i in range(quantity):
            removed_id = db.remove_first_matching(set_code.strip(), collector_number.strip(),
                                                  (lang or "").strip() if lang else None)
            if removed_id:
                removed_for_row += 1
                total_removed += 1
                logger.info("Removed id=%s for %s/%s/%s [%d/%d]", removed_id, set_code, collector_number, lang or "",
                            removed_for_row, quantity)
            else:
                # no more matches for this row
                if removed_for_row == 0:
                    logger.info("No match found for %s/%s/%s", set_code, collector_number, lang or "")
                else:
                    logger.info("Stopped after removing %d/%d entries for %s/%s/%s (no more matches).",
                                removed_for_row, quantity, set_code, collector_number, lang or "")
                break
    logger.info("Removed %d total entries.", total_removed)
    print(f"Removed {total_removed} total entries.")


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
        # brief console feedback
        print(f"Read newest file from Drive: {name}")
        logger.info("Read newest file from Drive: %s", name)

    if args.mode == "append":
        process_append(content_bytes, db, scry, args.set_col, args.num_col, args.lang_col, args.foil_col, args.qty_col, args.location, logger)
    else:
        process_remove(content_bytes, db, args.set_col, args.num_col, args.lang_col, args.foil_col, args.qty_col, logger)


if __name__ == "__main__":
    main()
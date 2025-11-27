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

If local CSV is provided, Drive is not used.
"""
from __future__ import annotations
import argparse
import csv
import io
import sys
from typing import Optional, Dict, Any
from datetime import datetime

from scryfall_drive_db.drive_client import DriveClient
from scryfall_drive_db.scryfall_client import ScryfallClient
from scryfall_drive_db.db_manager import DBManager


def detect_column(row: Dict[str, str], candidates: list) -> Optional[str]:
    """
    Given a CSV row keys and candidate names, return the first matching actual key (case-insensitive)
    """
    lower_to_actual = {k.lower(): k for k in row.keys()}
    for cand in candidates:
        if cand.lower() in lower_to_actual:
            return lower_to_actual[cand.lower()]
    return None


def iterate_csv_bytes(content: bytes, set_col: str, num_col: str, lang_col: Optional[str], foil_col: Optional[str]):
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
        yield row, set_col, num_col, lang_col, foil_col


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
                   set_col: str, num_col: str, lang_col: Optional[str], foil_col: Optional[str], default_location: str):
    count = 0
    for row, set_col, num_col, lang_col, foil_col in iterate_csv_bytes(content_bytes, set_col, num_col, lang_col, foil_col):
        set_code = row.get(set_col) if set_col else None
        collector_number = row.get(num_col) if num_col else None
        lang = row.get(lang_col) if lang_col else None
        foil_value = row.get(foil_col) if foil_col else None

        if not set_code or not collector_number:
            print("Skipping row missing set_code or collector_number:", row, file=sys.stderr)
            continue
        try:
            js = scry.get_card(set_code.strip(), collector_number.strip(), lang.strip() if lang else None)
        except Exception as e:
            print(f"Failed to fetch {set_code}/{collector_number}/{lang}: {e}", file=sys.stderr)
            continue

        name = js.get("name")
        color_identity = js.get("color_identity", [])
        price_usd = pick_price_from_json(js, foil_value)
        entry = {
            "set_code": set_code.strip(),
            "collector_number": collector_number.strip(),
            "lang": (lang or "").strip(),
            "name": name,
            "color_identity": color_identity,
            "price_usd": price_usd,
            "location": default_location,
            "fetched_at": datetime.utcnow().isoformat()
        }
        row_id = db.add_entry(entry)
        count += 1
        print(f"Inserted id={row_id} {set_code}/{collector_number}/{lang or ''} -> {name}")
    print(f"Appended {count} entries.")


def process_remove(content_bytes: bytes, db: DBManager, set_col: str, num_col: str, lang_col: Optional[str], foil_col: Optional[str]):
    count = 0
    for row, set_col, num_col, lang_col, foil_col in iterate_csv_bytes(content_bytes, set_col, num_col, lang_col, foil_col):
        set_code = row.get(set_col) if set_col else None
        collector_number = row.get(num_col) if num_col else None
        lang = row.get(lang_col) if lang_col else None

        if not set_code or not collector_number:
            print("Skipping row missing set_code or collector_number:", row, file=sys.stderr)
            continue
        removed_id = db.remove_first_matching(set_code.strip(), collector_number.strip(), (lang or "").strip() if lang else None)
        if removed_id:
            print(f"Removed id={removed_id} for {set_code}/{collector_number}/{lang or ''}")
            count += 1
        else:
            print(f"No match found for {set_code}/{collector_number}/{lang or ''}")
    print(f"Removed {count} entries.")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["append", "remove"], help="append to DB or remove entries")
    p.add_argument("--folder-id", default='1cc7nHtHuHpkrhTLjKxzStRE8wN2a-1Ia', help="Google Drive folder ID to look for newest CSV")
    p.add_argument("--credentials", help="Path to service account JSON for Drive (required if using folder-id)")
    p.add_argument("--local-csv", help="Path to a local CSV; if provided, Drive is not used")
    p.add_argument("--db", default="database/mtg_collection.db", help="Path to sqlite DB")
    p.add_argument("--location", default="bulk", choices=["binder", "personal", "bulk"], help="Location value to store on append")
    p.add_argument("--set-col", default="Set code", help="CSV column for set code")
    p.add_argument("--num-col", default="Collector number", help="CSV column for collector number")
    p.add_argument("--lang-col", default="Language", help="CSV column for language (optional)")
    p.add_argument("--foil-col", default="Foil", help="CSV column indicating foil/etched/normal")
    args = p.parse_args()

    if not args.local_csv and not args.folder_id:
        print("Either --local-csv or --folder-id must be provided", file=sys.stderr)
        sys.exit(2)

    db = DBManager(args.db)
    scry = ScryfallClient()

    content_bytes = None
    if args.local_csv:
        with open(args.local_csv, "rb") as fh:
            content_bytes = fh.read()
    else:
        # use Drive
        if not args.credentials:
            print("--credentials is required when using --folder-id", file=sys.stderr)
            sys.exit(2)
        drive = DriveClient(service_account_file=args.credentials)
        name, content = drive.get_newest_csv_bytes(args.folder_id)
        if content is None:
            print("No CSV found in folder", file=sys.stderr)
            sys.exit(1)
        content_bytes = content
        print(f"Read newest file from Drive: {name}")

    if args.mode == "append":
        process_append(content_bytes, db, scry, args.set_col, args.num_col, args.lang_col, args.foil_col, args.location)
    else:
        process_remove(content_bytes, db, args.set_col, args.num_col, args.lang_col, args.foil_col)


if __name__ == "__main__":
    main()
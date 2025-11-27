```markdown
# Scryfall CSV → SQLite tool

This tool finds the newest CSV in a Google Drive folder (or reads a local CSV), queries Scryfall for card data, and appends or removes entries in an SQLite DB.

Key behaviors:
- Append: for each CSV row, call Scryfall `/cards/:code/:number(/:lang)` and store:
  - id (auto-increment primary key)
  - set_code
  - collector_number
  - lang
  - name
  - color_identity (stored as a single comma-separated string)
  - price_usd (choice depends on foil value in CSV)
  - location (binder, personal, or bulk)
  - fetched_at (timestamp)
- Remove: find the first DB entry matching set_code, collector_number, and lang and delete it. Quantity-aware deletion described below.

Requirements
- Python 3.9+
- See requirements.txt

Google Drive
- This uses a service account key JSON to authenticate. Share the Drive folder with the service account email or use OAuth (modify DriveClient accordingly).
- Provide the folder ID containing CSV files.

CSV format expectations
- The CSV must have columns that can be mapped to:
  - set code (defaults: `set_code`, case-insensitive)
  - collector number (defaults: `collector_number`, case-insensitive)
  - language (defaults: `language`, case-insensitive; optional)
  - foil indicator (defaults: `foil`, case-insensitive) — values expected: `normal`, `foil`, or `etched` (case-insensitive). If missing or unrecognized, falls back to `normal`.
  - quantity (defaults: `quantity`, case-insensitive) — integer specifying how many copies to insert or remove for that row. If missing or blank the default is 1. If quantity <= 0 the row is skipped.

Quantity behavior (new)
- Append mode:
  - For each CSV row the script fetches the Scryfall JSON once, then inserts N rows into the database where N is the parsed `Quantity` value. This creates duplicate DB rows representing each physical copy.
- Remove mode:
  - For each CSV row the script attempts to remove up to N matching rows (by set_code, collector_number, lang), stopping early if there are not enough matches. Matches are removed starting from the lowest id (first-inserted).

Usage examples
- Append newest CSV from Drive folder:
  python cli.py append --folder-id FOLDER_ID --credentials /path/to/service-account.json --db db/cards.db --location personal

- Remove entries (first matching) for rows in the newest CSV:
  python cli.py remove --folder-id FOLDER_ID --credentials /path/to/service-account.json --db db/cards.db

- Use a local CSV (no Drive):
  python cli.py append --local-csv /tmp/myfile.csv --db db/cards.db

Options
- --set-col / --num-col / --lang-col / --foil-col / --qty-col: override CSV column names.
- --location: binder | personal | bulk (default: personal)

Notes
- The Drive client downloads the newest CSV (by modifiedTime).
- The DB schema includes `set_code` and `collector_number` to allow matching/removal.
- The Scryfall fetch respects the foil/etched preference for price selection.

```
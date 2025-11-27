"""
SQLite DB manager: creates table and provides add/remove operations.

Schema:
- id INTEGER PRIMARY KEY AUTOINCREMENT
- set_code TEXT
- collector_number TEXT
- lang TEXT
- name TEXT
- color_identity TEXT (comma-separated)
- price_usd REAL (nullable)
- location TEXT
- fetched_at TEXT (isoformat)
"""
from __future__ import annotations
import sqlite3
from typing import List, Optional, Dict, Any
from datetime import datetime


DEFAULT_TABLE = "cards"


class DBManager:
    def __init__(self, db_path: str, table: str = DEFAULT_TABLE):
        self.db_path = db_path
        self.table = table
        self._ensure_table()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _ensure_table(self):
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_code TEXT,
            collector_number TEXT,
            lang TEXT,
            name TEXT,
            color_identity TEXT,
            price_usd REAL,
            location TEXT,
            fetched_at TEXT
        );
        """
        with self._connect() as conn:
            conn.execute(sql)

    def add_entry(self, data: Dict[str, Any]) -> int:
        """
        data expected keys:
        set_code, collector_number, lang, name, color_identity (list or string), price_usd (float/None),
        location (string), fetched_at (iso string)
        Returns inserted row id.
        """
        color = data.get("color_identity")
        if isinstance(color, (list, tuple)):
            color_str = ",".join(color)
        else:
            color_str = color or ""

        sql = f"""
        INSERT INTO {self.table} (set_code, collector_number, lang, name, color_identity, price_usd, location, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            data.get("set_code"),
            str(data.get("collector_number")) if data.get("collector_number") is not None else None,
            data.get("lang"),
            data.get("name"),
            color_str,
            data.get("price_usd"),
            data.get("location"),
            data.get("fetched_at") or datetime.utcnow().isoformat(),
        )
        with self._connect() as conn:
            cur = conn.execute(sql, params)
            return cur.lastrowid # type: ignore

    def remove_first_matching(self, set_code: str, collector_number: str, lang: Optional[str]) -> Optional[int]:
        """
        Find the first (lowest id) row that matches set_code, collector_number, and lang (lang may be None)
        and delete it. Returns deleted id or None if not found.
        """
        where_clause = "set_code = ? AND collector_number = ?"
        params = [set_code, str(collector_number)]
        if lang is None:
            where_clause += " AND (lang IS NULL OR lang = '')"
        else:
            where_clause += " AND lang = ?"
            params.append(lang)

        sql_select = f"SELECT id FROM {self.table} WHERE {where_clause} ORDER BY id ASC LIMIT 1"
        with self._connect() as conn:
            cur = conn.execute(sql_select, params)
            row = cur.fetchone()
            if not row:
                return None
            row_id = row[0]
            conn.execute(f"DELETE FROM {self.table} WHERE id = ?", (row_id,))
            return row_id

    def list_all(self) -> List[Dict[str, Any]]:
        sql = f"SELECT id, set_code, collector_number, lang, name, color_identity, price_usd, location, fetched_at FROM {self.table} ORDER BY id"
        with self._connect() as conn:
            cur = conn.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = []
            for r in cur.fetchall():
                rows.append(dict(zip(cols, r)))
            return rows
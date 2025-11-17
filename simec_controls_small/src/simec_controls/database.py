"""Database access and lifecycle management for the UI (Phase 1).

Implements safe close with:
  - pending transaction flush
  - automatic rolling backups (max 5)
  - filename integrity registration/check

This module is intentionally self-contained and portable.
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
import sqlite3
import re
import logging

logger = logging.getLogger(__name__)

@dataclass
class DBState:
    path: Optional[Path] = None
    conn: Optional[sqlite3.Connection] = None

class DatabaseManager:
    """Manage a single working SQLite database connection and metadata."""
    def __init__(self) -> None:
        self._state = DBState()

    # ----------------------- Public API ----------------------------------
    @property
    def is_open(self) -> bool:
        return self._state.conn is not None and self._state.path is not None

    @property
    def path(self) -> Optional[Path]:
        return self._state.path

    def open(self, path: Path, timeout: float = 30.0, wal: bool = True) -> None:
        """Open/connect to the SQLite database at *path*.

        Also registers/validates the filename integrity marker inside the DB.
        Raises sqlite3.Error on failure.
        """
        if self.is_open:
            raise RuntimeError("A database is already open. Close it before opening another.")

        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), timeout=timeout)
        conn.execute("PRAGMA foreign_keys=ON")
        if wal:
            conn.execute("PRAGMA journal_mode=WAL")
        self._state = DBState(path=path, conn=conn)
        self._ensure_metadata_table()
        self._check_or_register_filename(path.name)

    def close_with_backup(self) -> tuple[Optional[Path], Optional[str]]:
        """Flush outstanding ops, create a rolling backup (≤5), then close.

        Returns (backup_path, warning_message). *warning_message* is None if all good.
        Safe to call multiple times.
        """
        if not self.is_open:
            return (None, None)

        warning: Optional[str] = None
        try:
            self._flush_pending()
        except Exception as ex:  # defensive: never lose the chance to backup/close
            logger.error("Error while flushing pending ops: %s", ex)
            warning = f"Pending operations flush error: {ex}"

        backup_path: Optional[Path] = None
        try:
            backup_path = self._create_rolling_backup(max_backups=5)
        except Exception as ex:
            logger.error("Backup failed: %s", ex)
            warning = (f"Backup failed: {ex}" if warning is None 
                       else warning + f"; Backup failed: {ex}")

        try:
            self._really_close()
        finally:
            self._state = DBState()

        return (backup_path, warning)

    def validate_filename_integrity(self) -> bool:
        """Return True if registered filename matches on-disk filename."""
        if not self.is_open:
            return False
        assert self._state.conn is not None and self._state.path is not None
        row = self._state.conn.execute(
            "SELECT value FROM app_metadata WHERE key='registered_filename'"
        ).fetchone()
        return bool(row and row[0] == self._state.path.name)

    # ----------------------- Internal helpers ----------------------------
    def _ensure_metadata_table(self) -> None:
        assert self._state.conn is not None
        self._state.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_metadata (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        self._state.conn.commit()

    def _check_or_register_filename(self, current_name: str) -> None:
        assert self._state.conn is not None
        cur = self._state.conn.cursor()
        row = cur.execute(
            "SELECT value FROM app_metadata WHERE key='registered_filename'"
        ).fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO app_metadata (key, value) VALUES ('registered_filename', ?)",
                (current_name,),
            )
            self._state.conn.commit()
        else:
            registered = row[0]
            if registered != current_name:
                raise RuntimeError(
                    f"Database filename integrity check failed. Expected '{registered}', got '{current_name}'."
                )

    def _flush_pending(self) -> None:
        assert self._state.conn is not None
        conn = self._state.conn
        # Commit any active transaction
        if conn.in_transaction:
            conn.commit()
        # Ensure WAL is checkpointed (best-effort; ignore errors)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            pass

    def _create_rolling_backup(self, max_backups: int = 5) -> Path:
        assert self._state.conn is not None and self._state.path is not None
        src = self._state.path
        stem = src.stem  # e.g., 'SMR' or 'dlims'
        # Backup naming: <stem>_bak###.sqlite (### = 001..999)
        import os
        import glob
        pattern = re.compile(rf"^{re.escape(stem)}_bak(\d{{3}})\.sqlite$", re.IGNORECASE)
        siblings = [Path(p) for p in glob.glob(str(src.parent / f"{stem}_bak*.sqlite"))]
        nums = []
        for f in siblings:
            m = pattern.match(f.name)
            if m:
                nums.append(int(m.group(1)))
        next_num = (max(nums) + 1) if nums else 1
        next_name = f"{stem}_bak{next_num:03d}.sqlite"
        dst = src.parent / next_name
        # Use SQLite online backup API for consistency
        bconn = sqlite3.connect(str(dst))
        with bconn:
            self._state.conn.backup(bconn)  # type: ignore[arg-type]
        bconn.close()

        # Enforce retention (keep only most recent *max_backups*)
        numbered = []
        for f in siblings + [dst]:
            m = pattern.match(f.name)
            if m:
                numbered.append((int(m.group(1)), f))
        numbered.sort()
        while len(numbered) > max_backups:
            oldest_num, oldest_path = numbered.pop(0)
            try:
                oldest_path.unlink(missing_ok=True)
            except Exception as ex:
                logger.warning("Failed to delete old backup %s: %s", oldest_path, ex)
        return dst

    def _really_close(self) -> None:
        if self._state.conn is not None:
            try:
                self._state.conn.close()
            finally:
                self._state.conn = None

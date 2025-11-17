from __future__ import annotations
from simec_controls.ui.plc_import_actions import make_import_handler
"""Main application window for the Process Control Documentation UI scaffold."""

import logging
from pathlib import Path
from datetime import datetime
import re

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QStatusBar, QVBoxLayout, QLabel,
    QMessageBox, QApplication, QFileDialog, QDockWidget,
    QTreeWidget
)

# Local imports (packaged or sibling)
try:
    from simec_controls.ui.menu_bar import build_menu_bar
except Exception:
    from menu_bar import build_menu_bar  # type: ignore

try:
    from simec_controls.database import DatabaseManager
except Exception:
    from database import DatabaseManager  # type: ignore

log = logging.getLogger(__name__)


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self._on_import_plc_module = make_import_handler(self)
        self.setWindowTitle("Process Control Documentation")
        self.resize(1778, 1000)

        self._db = DatabaseManager()
        self._init_ui()

    # ------------------------ UI wiring ---------------------------------
    def _init_ui(self) -> None:
        self._on_import_plc_module = make_import_handler(self)
        build_menu_bar(self)
        status = QStatusBar()
        status.showMessage("Ready")
        self.setStatusBar(status)

        central = QWidget(self)
        lay = QVBoxLayout(central)
        lay.addWidget(QLabel("UI Scaffold Ready — Connect a database to begin.", self))
        central.setLayout(lay)
        self.setCentralWidget(central)

        # Left dock: Asset Tree (placeholder)
        self._asset_tree = QTreeWidget(self)
        self._asset_tree.setHeaderLabels(["Asset Hierarchy"])
        self._asset_tree.setRootIsDecorated(True)
        dock = QDockWidget("Assets", self)
        dock.setWidget(self._asset_tree)
        dock.setObjectName("AssetDock")
        dock.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        self.addDockWidget(Qt.LeftDockWidgetArea, dock)

    # ------------------------ File actions -------------------------------
    def _db_dir(self) -> Path:
        # Default project-relative db folder
        root = Path(__file__).resolve().parents[3]  # .../simec_controls/
        return (root / "db")

    def _on_open_database(self) -> None:
        db_dir = self._db_dir()
        db_dir.mkdir(parents=True, exist_ok=True)
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Open Database",
            str(db_dir),
            "SQLite Databases (*.sqlite *.db)"
        )
        if not file_path:
            return
        try:
            self._db.open(Path(file_path))
        except RuntimeError as e:
            QMessageBox.critical(self, "Integrity Error", str(e))
            return
        except Exception as e:
            QMessageBox.critical(self, "Open Failed", f"{e}")
            return
        ok = self._db.validate_filename_integrity()
        if not ok:
            QMessageBox.warning(
                self,
                "Filename Integrity",
                "Warning: filename integrity could not be confirmed."
            )
        self.statusBar().showMessage(f"Opened: {Path(file_path).name}")
        self.refresh_asset_hierarchy_all()

    def _on_new_database(self) -> None:
        db_dir = self._db_dir()
        db_dir.mkdir(parents=True, exist_ok=True)
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Create New Database",
            str(db_dir / "dlims.sqlite"),
            "SQLite Databases (*.sqlite)"
        )
        if not file_path:
            return
        target = Path(file_path)
        if target.exists():
            resp = QMessageBox.question(
                self,
                "Overwrite?",
                f"'{target.name}' already exists in ./db.\nDo you want to overwrite it?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if resp != QMessageBox.Yes:
                return
            try:
                target.unlink()
            except Exception as ex:
                QMessageBox.critical(self, "Error", f"Could not overwrite file:\n{ex}")
                return
        try:
            self._db.open(target)
            # Ensure filename integrity marker is set by .open()
            self.statusBar().showMessage(f"Created: {target.name}")
        except Exception as e:
            QMessageBox.critical(self, "Creation Failed", f"{e}")

    def _on_close_database(self) -> None:
        """Close the current DB after flushing, rolling backup (≤5), and checks.

        Also used by the window closeEvent to ensure safety on app exit.
        """
        if not self._db.is_open:
            QMessageBox.information(self, "Close Database", "No database is currently open.")
            return

        backup_path, warn = self._db.close_with_backup()
        msg = "Database closed successfully."
        if backup_path:
            msg += f" Backup created: {backup_path.name}"
        if warn:
            msg += f"\nNote: {warn}"
        QMessageBox.information(self, "Close Database", msg)
        self.statusBar().showMessage("Database closed")
        self.clear_asset_hierarchy()

    def _on_restore_backup_database(self) -> None:
        """Restore a selected backup file (<stem>_bakNNN.sqlite) as the working DB (<stem>.sqlite).

        Steps:
          1) Ask user to pick exactly one backup file from the db folder (filter *_bak???.sqlite).
          2) If none/invalid → error dialog and return.
          3) Compute working name <stem>.sqlite. If a DB is open, close it safely (rolling backup).
          4) If <stem>.sqlite exists, rename it to <stem>_dep[_{ISO}].sqlite to keep a deprecation snapshot.
          5) Promote the selected backup to <stem>.sqlite (atomic rename in same folder).
          6) Open the promoted DB and validate filename integrity. On failure → rollback and report.
        """
        db_dir = self._db_dir()
        db_dir.mkdir(parents=True, exist_ok=True)

        # QFileDialog configured for single existing file selection and backup wildcard filter
        dialog = QFileDialog(self, "Restore Backup Database", str(db_dir))
        dialog.setFileMode(QFileDialog.ExistingFile)
        dialog.setNameFilter("SQLite Backups (*_bak???.sqlite)")
        dialog.setOption(QFileDialog.DontUseNativeDialog, True)
        dialog.setLabelText(QFileDialog.Accept, "Restore")
        dialog.setLabelText(QFileDialog.Reject, "Cancel")

        if not dialog.exec():
            return

        selected = dialog.selectedFiles()
        if not selected:
            QMessageBox.critical(self, "Restore Backup Database",
                                 "The selected backup file does not exist or no file was chosen.")
            return

        bak_path = Path(selected[0])
        if not bak_path.exists():
            QMessageBox.critical(self, "Restore Backup Database",
                                 "The selected backup file does not exist or no file was chosen.")
            return

        # Match "<stem>_bakNNN.sqlite"
        m = re.match(r"^(?P<stem>.+)_bak\d{3}\.sqlite$", bak_path.name)
        if not m:
            QMessageBox.critical(self, "Restore Backup Database",
                                 "Invalid backup filename. Expected pattern '<name>_bakNNN.sqlite'.")
            return
        stem = m.group("stem")
        working = bak_path.with_name(f"{stem}.sqlite")
        dep_name = f"{stem}_dep.sqlite"
        dep_path = bak_path.with_name(dep_name)

        # If a database is currently open, close it safely first (creates rolling backup)
        try:
            if self._db.is_open:
                self._db.close_with_backup()
        except Exception as e:
            QMessageBox.critical(self, "Restore Backup Database",
                                 f"Could not close the current database: {e}")
            return

        # Prepare deprecation snapshot for prior working DB if present
        created_dep = None
        if working.exists():
            # If <stem>_dep.sqlite already exists, append a timestamp suffix
            if dep_path.exists():
                ts = datetime.now().strftime("%Y%m%dT%H%M%S")
                dep_path = bak_path.with_name(f"{stem}_dep_{ts}.sqlite")
            try:
                working.rename(dep_path)
                created_dep = dep_path
            except Exception as e:
                QMessageBox.critical(self, "Restore Backup Database",
                                     f"Could not preserve the prior working database as a deprecation snapshot:\n{e}")
                return

        # Promote backup to working
        try:
            bak_path.rename(working)
        except Exception as e:
            # Attempt to revert any deprecation rename
            if created_dep and not working.exists():
                try:
                    created_dep.rename(working)
                except Exception:
                    pass
            QMessageBox.critical(self, "Restore Backup Database",
                                 f"Could not promote backup to working database:\n{e}")
            return

        # Open and validate filename integrity
        try:
            self._db.open(working)
            ok = self._db.validate_filename_integrity()
            if not ok:
                # Rollback: close; move working back to backup name; restore dep if it existed
                try:
                    if self._db.is_open:
                        self._db.close_with_backup()
                except Exception:
                    pass
                # Move working back to a backup-style name (best-effort; use _bak999 if needed)
                revert_name = bak_path.name if not bak_path.exists() else f"{stem}_bak999.sqlite"
                revert_path = working.with_name(revert_name)
                try:
                    working.rename(revert_path)
                except Exception:
                    pass
                # Restore dep snapshot (best-effort)
                if created_dep and not working.exists():
                    try:
                        created_dep.rename(working)
                    except Exception:
                        pass
                QMessageBox.critical(self, "Restore Backup Database",
                                     "Filename integrity mismatch detected on restored database. Restore aborted and rolled back.")
                return
        except Exception as e:
            # Attempt to roll back promotion
            try:
                if self._db.is_open:
                    self._db.close_with_backup()
            except Exception:
                pass
            try:
                # Move working back to original backup name if free
                revert_target = working.with_name(bak_path.name if not bak_path.exists() else f"{stem}_bak999.sqlite")
                working.rename(revert_target)
            except Exception:
                pass
            # Put deprecation snapshot back if it existed
            if created_dep and not working.exists():
                try:
                    created_dep.rename(working)
                except Exception:
                    pass
            QMessageBox.critical(self, "Restore Backup Database", f"Failed to open restored database:\n{e}")
            return

        # Success
        self.statusBar().showMessage(
            f"Restored backup '{bak_path.name}' → Working '{working.name}'. "
            + (f"Prior working saved as '{created_dep.name}'" if created_dep else "No prior working existed.")
        )
        QMessageBox.information(
            self, "Restore Backup Database",
            f"Successfully restored '{bak_path.name}' as '{working.name}'.\n"
            + (f"Previous working saved as '{created_dep.name}'." if created_dep else "")
        )
        self.refresh_asset_hierarchy_all()

    # ------------------------ Qt events ----------------------------------
    def closeEvent(self, event) -> None:  # noqa: N802 (Qt API)
        """Ensure DB is safely closed with backup if the app is exiting."""
        try:
            if self._db.is_open:
                self._db.close_with_backup()
        finally:
            super().closeEvent(event)
            
   

    def refresh_asset_hierarchy(self, controller_name: str) -> None:

        """

        Reloads/updates the Assets tree to display the newly imported PLC
        identified by `controller_name`, including its Programs → Routines,
        AOIs, and Tags, sourced from the normalized tables.
        Non-destructive to existing UI; safe if DB is closed/missing.
        This method performs read-only DB access and only updates/creates
        the subtree for `controller_name`.

        """

        import logging, sqlite3
        from pathlib import Path

        log = logging.getLogger("simec.ui")

        # DB path
        db_path = None

        try:
            for attr in ("_db", "db", "database", "db_manager"):
                mgr = getattr(self, attr, None)
                if mgr is None:
                    continue
                db_path = getattr(mgr, "path", None) or getattr(mgr, "database_path", None)
                if callable(db_path):
                    db_path = db_path()
                if db_path:
                    break

            db_path = db_path or getattr(self, "db_path", None)

        except Exception:
            db_path = None

        if not db_path or not Path(db_path).exists():
            log.debug("refresh_asset_hierarchy skipped: no open database.")
            return

        # Tree

        tree = None

        for name in ("_asset_tree", "assets_tree", "asset_tree", "tree_assets", "treeViewAssets", "tree_widget_assets"):
            tree = getattr(self, name, None)
            if tree is not None:
                break
        if tree is None:
            log.warning("refresh_asset_hierarchy: assets tree widget not found on MainWindow.")
            return

        try:
            tree.setUpdatesEnabled(False)
        except Exception:
            pass

        plc_node, plc_label = None, f"PLC {controller_name}"

        try:
            top_count = tree.topLevelItemCount()

            for i in range(top_count):
                item = tree.topLevelItem(i)
                if item and item.text(0) == plc_label:
                    plc_node = item
                    break

            if plc_node is None:
                from PySide6.QtWidgets import QTreeWidgetItem

                plc_node = QTreeWidgetItem([plc_label])
                tree.addTopLevelItem(plc_node)
            else:
                plc_node.takeChildren()

        except Exception as exc:

            log.warning("refresh_asset_hierarchy: could not prepare PLC node: %s", exc)

            try: tree.setUpdatesEnabled(True)
            except Exception: pass
            return

        uri = f"file:{Path(db_path).as_posix()}?mode=ro"

        try:
            conn = sqlite3.connect(uri, uri=True); conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM plc_controllers WHERE name = ? LIMIT 1",(controller_name,),)
            row_ctrl = cur.fetchone()

            if not row_ctrl:
                log.debug("refresh_asset_hierarchy: controller '%s' not found in DB.", controller_name)
                return

            controller_id = row_ctrl["id"]

            cur.execute("SELECT id, name FROM plc_programs WHERE controller_id = ? ORDER BY name",(controller_id,),)

            programs = cur.fetchall()

            cur.execute("SELECT id, name FROM plc_aois WHERE controller_id = ? ORDER BY name",(controller_id,),)

            aois = cur.fetchall()

            cur.execute("SELECT name FROM plc_tags WHERE controller_id = ? ORDER BY name LIMIT 5000",(controller_id,),)

            tags = cur.fetchall()

        except Exception as exc:

            log.warning("refresh_asset_hierarchy: query failed: %s", exc); return

        finally:

            try: conn.close()

            except Exception: pass

        try:

            from PySide6.QtWidgets import QTreeWidgetItem

            for p in programs:

                prog_item = QTreeWidgetItem([f"Program: {p['name']}"]); plc_node.addChild(prog_item)

                conn = sqlite3.connect(uri, uri=True); conn.row_factory = sqlite3.Row

                try:

                    cur = conn.cursor()

                    cur.execute("SELECT name FROM plc_routines WHERE program_id = ? ORDER BY name",(p["id"],),)

                    routines = cur.fetchall()

                finally:

                    try: conn.close()

                    except Exception: pass

                for r in routines:

                    prog_item.addChild(QTreeWidgetItem([f"Routine: {r['name']}"]))

            if aois:

                for a in aois:

                    plc_node.addChild(QTreeWidgetItem([f"AOI: {a['name']}"]))

            tags_root = QTreeWidgetItem(["Tags"]); plc_node.addChild(tags_root)

            for t in tags:

                tags_root.addChild(QTreeWidgetItem([f"Tag: {t['name']}"]))

            try: tree.expandItem(plc_node)

            except Exception: pass

            log.info("Asset hierarchy refreshed for controller '%s'", controller_name)

        except Exception as exc:

            log.warning("refresh_asset_hierarchy: UI populate failed: %s", exc)

        finally:

            try: tree.setUpdatesEnabled(True)

            except Exception: pass


    def refresh_asset_hierarchy_all(self) -> None:

        """

        Build (or rebuild) the full Data Warehouse Asset Hierarchy from the
        currently open database. Safe no-op if no DB is open. Read-only DB access.

        """

        import logging, sqlite3

        from pathlib import Path

        log = logging.getLogger("simec.ui")


        db_path = None

        try:

            for attr in ("_db", "db", "database", "db_manager"):

                mgr = getattr(self, attr, None)

                if mgr is None:

                    continue

                db_path = getattr(mgr, "path", None) or getattr(mgr, "database_path", None)
                if callable(db_path):
                    db_path = db_path()

                if db_path:

                    break

            db_path = db_path or getattr(self, "db_path", None)

        except Exception:

            db_path = None

        if not db_path or not Path(db_path).exists():

            log.debug("refresh_asset_hierarchy_all skipped: no open database.")

            return


        tree = None

        for name in ("_asset_tree", "assets_tree", "asset_tree", "tree_assets", "treeViewAssets", "tree_widget_assets"):

            tree = getattr(self, name, None)

            if tree is not None:

                break

        if tree is None:

            log.warning("refresh_asset_hierarchy_all: assets tree widget not found on MainWindow.")

            return


        try: tree.setUpdatesEnabled(False)

        except Exception: pass


        try: tree.clear()

        except Exception: pass


        uri = f"file:{Path(db_path).as_posix()}?mode=ro"

        controllers = []

        try:

            conn = sqlite3.connect(uri, uri=True); conn.row_factory = sqlite3.Row

            cur = conn.cursor()

            cur.execute("SELECT name FROM plc_controllers ORDER BY name")

            controllers = [r["name"] for r in cur.fetchall()]

        except Exception as exc:

            log.warning("refresh_asset_hierarchy_all: query failed: %s", exc)

        finally:

            try: conn.close()

            except Exception: pass


        for name in controllers:

            try:

                self.refresh_asset_hierarchy(name)

            except Exception as exc:

                log.warning("refresh_asset_hierarchy_all: refresh single failed for '%s': %s", name, exc)


        try: tree.setUpdatesEnabled(True)

        except Exception: pass


        log.info("Asset hierarchy fully refreshed (%d controllers).", len(controllers))


# ---------------------------- Helpers ------------------------------------

    def clear_asset_hierarchy(self) -> None:
        """Clear the Asset Hierarchy tree when no database is open."""
        try:
            tree = getattr(self, "_asset_tree", None) or getattr(self, "assets_tree", None) or getattr(self, "asset_tree", None)
            if tree is None:
                return
            tree.clear()
        except Exception as ex:
            log.warning("clear_asset_hierarchy failed: %s", ex)

def launch_ui() -> int:
    """Convenience entry for scripts to start the UI."""
    import sys
    app = QApplication.instance() or QApplication(sys.argv)
    w = MainWindow()
    w.show()
    return app.exec()
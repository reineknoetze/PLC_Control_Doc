from __future__ import annotations
from pathlib import Path
from typing import Callable, Optional, Dict, Any
import logging

from PySide6.QtWidgets import QFileDialog, QMessageBox, QApplication, QProgressDialog
from PySide6.QtCore import Qt

from simec_controls.processors.plc_importer import ImportConfig, import_plc_module_xml
from simec_controls.errors import Ok, Err  # Result model

LOG_UI = logging.getLogger("simec.ui")


def make_import_handler(main_window, db_path: Path | str | None = None) -> Callable[[], None]:
    """
    Factory that returns a slot callable bound to the given main_window.
    Non-destructive; assumes main_window implements refresh_asset_hierarchy(name: str).
    The database-open check is performed **when the handler is invoked**, not at factory time.
    """
    # Normalize incoming db_path if provided
    if isinstance(db_path, str):
        try:
            db_path = Path(db_path)
        except Exception:
            db_path = None

    def _handler() -> None:
        LOG_UI.info("PLC Import dialog opened")

        # Resolve active DB path at call-time
        _db_path: Optional[Path] = db_path if isinstance(db_path, Path) else None
        if _db_path is None:
            try:
                mgr = getattr(main_window, "_db", None)
                if mgr and getattr(mgr, "is_open", False):
                    p = getattr(mgr, "path", None)
                    if callable(p):
                        _db_path = Path(p())
                    else:
                        _db_path = Path(p) if p else None
            except Exception:
                _db_path = None
        if _db_path is None:
            QMessageBox.warning(
                main_window,
                "No Database Open",
                "Please open a database before importing a PLC module.",
            )
            return

        start_dir = Path("./imports") if Path("./imports").exists() else Path(".")

        dlg = QFileDialog(main_window, "Import PLC Module (XML)", str(start_dir))
        dlg.setFileMode(QFileDialog.ExistingFile)
        dlg.setNameFilter("PLC Module XML (*.L5X)")
        dlg.setViewMode(QFileDialog.Detail)
        dlg.setOption(QFileDialog.ReadOnly, True)
        if not dlg.exec():
            return
        files = dlg.selectedFiles()
        if not files:
            QMessageBox.critical(
                main_window,
                "Error",
                "No valid PLC module file selected. Please select a valid *.L5X file.",
            )
            return
        l5x = Path(files[0]).resolve()
        if not l5x.exists() or l5x.suffix.lower() != ".l5x":
            QMessageBox.critical(
                main_window,
                "Error",
                "No valid PLC module file selected. Please select a valid *.L5X file.",
            )
            return
        # UNC guard
        if str(l5x).startswith("\\\\"):
            QMessageBox.critical(
                main_window, "Error", "Non-local/UNC paths are not allowed."
            )
            return

        progress = QProgressDialog("Reading…", "Cancel", 0, 100, main_window)
        progress.setWindowModality(Qt.WindowModal)
        progress.setAutoClose(False)
        progress.setMinimumDuration(0)
        progress.show()

        cancelled = {"flag": False}
        progress.canceled.connect(lambda: cancelled.__setitem__("flag", True))

        def update_progress(phase: str, value: int):
            progress.setLabelText(phase)
            progress.setValue(value)
            QApplication.processEvents()

        try:
            cfg = ImportConfig(
                db_path=_db_path,
                large_file_mb_threshold=25,
                chunk_size_elements=5000,
                enable_progress_dialog=True,
                abort_on_schema_mismatch=True,
                indexes_after_import=True,
                validate_only=False,
                facility_id=None,
                unit_id=None,
            )

            res = import_plc_module_xml(
                l5x,
                cfg,
                progress_callback=update_progress,
                is_cancelled=lambda: cancelled["flag"],
            )

            if isinstance(res, Ok):
                payload: Dict[str, Any] = res.value
                ctrl = payload.get("controller_name", "Unknown")
                # Call host's refresh (no-throw)
                try:
                    main_window.refresh_asset_hierarchy(ctrl)
                except Exception as ex:
                    LOG_UI.warning("refresh_asset_hierarchy failed: %s", ex)

                main_window.statusBar().showMessage(
                    (
                        "Import completed: {ctrl} (programs={p}, routines={r}, "
                        "tags={t}, aois={a})"
                    ).format(
                        ctrl=ctrl,
                        p=payload.get("programs", 0),
                        r=payload.get("routines", 0),
                        t=payload.get("tags", 0),
                        a=payload.get("aois", 0),
                    ),
                    8000,
                )
                QMessageBox.information(
                    main_window,
                    "Import Completed",
                    (
                        "Import completed: {ctrl}\n"
                        "Programs={p}, Routines={r}, Tags={t}, AOIs={a}"
                    ).format(
                        ctrl=ctrl,
                        p=payload.get("programs", 0),
                        r=payload.get("routines", 0),
                        t=payload.get("tags", 0),
                        a=payload.get("aois", 0),
                    ),
                )
            else:
                err = res.error
                LOG_UI.error("Import failed: %s", err)
                QMessageBox.critical(
                    main_window,
                    "Import Failed",
                    f"{err.kind.name}: {err.message}",
                )
        finally:
            progress.close()

    return _handler

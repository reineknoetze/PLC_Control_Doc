"""Menu bar construction for the main window (Phase 1)."""
from __future__ import annotations
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QMenuBar, QMainWindow, QMessageBox

def build_menu_bar(window: QMainWindow) -> QMenuBar:
    """Create and return the application menu bar.
    Always shows menu actions; connects to window slots when available.
    """
    menu_bar: QMenuBar = window.menuBar()

    # ------------------------------------------------------------------
    # File menu
    # ------------------------------------------------------------------
    file_menu = menu_bar.addMenu("&File")

    # New Database
    new_action = QAction("&New Database…", window)
    new_action.setStatusTip("Create a new SQLite database")
    slot = getattr(window, "_on_new_database", None)
    if callable(slot):
        new_action.triggered.connect(slot)  # type: ignore[call-arg]
    else:
        new_action.setEnabled(False)
    file_menu.addAction(new_action)

    # Open Database
    open_action = QAction("&Open Database…", window)
    open_action.setStatusTip("Open an existing SQLite database")
    slot = getattr(window, "_on_open_database", None)
    if callable(slot):
        open_action.triggered.connect(slot)  # type: ignore[call-arg]
    else:
        open_action.setEnabled(False)
    file_menu.addAction(open_action)

    # Close Database
    close_action = QAction("&Close Database", window)
    close_action.setStatusTip("Close the current database")
    slot = getattr(window, "_on_close_database", None)
    if callable(slot):
        close_action.triggered.connect(slot)  # type: ignore[call-arg]
    else:
        close_action.setEnabled(False)
    file_menu.addAction(close_action)

    # Restore Backup Database — always enabled; dispatch at runtime
    file_menu.addSeparator()
    restore_action = QAction("Restore &Backup Database…", window)
    restore_action.setStatusTip("Restore a previously created database backup as the active working database")

    def _dispatch_restore():
        slot = getattr(window, "_on_restore_backup_database", None)
        if callable(slot):
            slot()
        else:
            QMessageBox.information(window, "Restore Backup Database",
                                    "The restore handler is not available in this build.")
    restore_action.triggered.connect(_dispatch_restore)
    file_menu.addAction(restore_action)

    file_menu.addSeparator()

    # Exit
    exit_action = QAction("E&xit", window)
    exit_action.setStatusTip("Exit the application")
    exit_action.triggered.connect(window.close)
    file_menu.addAction(exit_action)

    # ------------------------------------------------------------------
    # Help menu (placeholder)
    # ------------------------------------------------------------------
    menu_bar.addMenu("&Help")
    
    # ------------------------------------------------------------------
    # PLC menu (additive; positioned to the right of File)
    # ------------------------------------------------------------------
    try:
        # Create PLC menu if not present
        plc_menu = None
        for act in menu_bar.actions():
            if act.text().replace("&","").lower() == "plc":
                plc_menu = act.menu()
                break
        if plc_menu is None:
            # Insert immediately after File if possible
            plc_menu = menu_bar.addMenu("&PLC")
            acts = list(menu_bar.actions())
            # Find File index
            file_idx = None
            for i,a in enumerate(acts):
                if a.text().replace("&","").lower() == "file":
                    file_idx = i
                    break
            if file_idx is not None and file_idx < len(acts)-1:
                # Move PLC just after File by reinserting
                plc_action = plc_menu.menuAction()
                menu_bar.removeAction(plc_action)
                menu_bar.insertMenu(acts[file_idx+1], plc_menu)

        # Add Import PLC action idempotently
        exists = any(a.text() == "Import PLC Module (XML)" for a in plc_menu.actions())
        if not exists:
                        act_import = QAction("Import PLC Module (XML)", window)
            
                        # Prefer MainWindow-provided slot if present
                        slot = getattr(window, "_on_import_plc_module", None)
                        if callable(slot):
                            act_import.triggered.connect(slot)
                        else:
                            # Fallback to handler factory with error logging
                            try:
                                from simec_controls.ui.plc_import_actions import make_import_handler
                                handler = make_import_handler(window)
                                act_import.triggered.connect(handler)
                            except Exception as ex:
                                import logging, traceback
                                logging.getLogger("simec.ui").error("PLC import handler wiring failed: %s", ex)
                                logging.getLogger("simec.ui").debug("Traceback:\n%s", traceback.format_exc())
                                act_import.triggered.connect(
                                    lambda: QMessageBox.warning(
                                        window, "Not Implemented",
                                        "Import handler is not available in this build."
                                    )
                                )
            
                        plc_menu.addAction(act_import)
    except Exception:
        pass

    return menu_bar
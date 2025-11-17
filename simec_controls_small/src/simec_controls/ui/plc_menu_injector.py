
from __future__ import annotations
from typing import Callable, Optional
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QMenuBar, QMenu, QWidget

def _find_menu(menu_bar: QMenuBar, title_plain: str) -> Optional[QMenu]:
    ttl = title_plain.lower()
    for act in menu_bar.actions():
        if act.text().replace("&", "").lower() == ttl:
            return act.menu()
    return None

def inject_plc_menu(main_window: QWidget, on_import_plc: Callable[[], None]) -> None:
    """
    Non-destructively add a 'PLC' menu with 'Import PLC Module (XML)' action.
    Safe to call multiple times. Does not alter central widgets or docks.
    """
    menu_bar: QMenuBar = main_window.menuBar()

    # leave existing menus untouched (File, Edit, View, Help, etc.)
    plc_menu = _find_menu(menu_bar, "PLC")
    if plc_menu is None:
        plc_menu = QMenu("&PLC", menu_bar)
        # place immediately to the right of File if present, else append
        acts = list(menu_bar.actions())
        insert_after = None
        for i, a in enumerate(acts):
            if a.text().replace("&", "").lower() == "file":
                insert_after = i
                break
        if insert_after is None or insert_after >= len(acts) - 1:
            menu_bar.addMenu(plc_menu)
        else:
            anchor = acts[insert_after + 1]
            menu_bar.insertMenu(anchor, plc_menu)

    # ensure action exists once
    action_text = "Import PLC Module (XML)"
    if not any(a.text() == action_text for a in plc_menu.actions()):
        act = QAction(action_text, main_window)
        act.triggered.connect(on_import_plc)
        plc_menu.addAction(act)

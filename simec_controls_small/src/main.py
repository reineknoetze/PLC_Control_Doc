# --- Portable bootstrap: ensure 'src' is on sys.path and discover project root ---
from __future__ import annotations
import sys
from pathlib import Path

_CUR = Path(__file__).resolve()
_SRC_DIR = _CUR.parent                      # .../PROJECT_ROOT/src
_PROJ_ROOT = _SRC_DIR.parent                # .../PROJECT_ROOT

# Optional: verify root marker (robust in case of relocations)
if not (_PROJ_ROOT / ".smc_ctrl-project").exists():
    for p in _CUR.parents:
        if (p / ".smc_ctrl-project").exists():
            _PROJ_ROOT = p
            _SRC_DIR = _PROJ_ROOT / "src"
            break

if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))
# --- end bootstrap ---

from simec_controls.ui.main_window import MainWindow  # noqa: E402
from PySide6.QtWidgets import QApplication            # noqa: E402
from PySide6.QtCore import Qt                         # noqa: E402
import logging                                        # noqa: E402


def _get_or_create_app() -> QApplication:
    """
    Return the existing QApplication if present; otherwise create one.
    Never create a second QApplication (avoids RuntimeError on rerun).
    """
    app = QApplication.instance()
    if app is None:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        app = QApplication(sys.argv)
    return app


def _find_existing_window(app: QApplication) -> MainWindow | None:
    """Return an existing MainWindow instance if one is already alive."""
    for w in app.topLevelWidgets():
        if isinstance(w, MainWindow):
            return w
    return None


def _show_window(win: MainWindow) -> None:
    """Show and bring the window to the front."""
    win.show()
    win.raise_()
    win.activateWindow()


def main() -> int:
    app = _get_or_create_app()

    # If a previous MainWindow still exists (same kernel/process), reuse it.
    win = _find_existing_window(app)
    if win is None:
        win = MainWindow()

    _show_window(win)

    # If the event loop is not running (e.g., after you closed the UI),
    # start it again. Calling exec() on an existing QApplication is valid.
    # In Spyder, this will block the console while the UI is open—normal for Qt apps.
    return app.exec()


if __name__ == "__main__":
    sys.exit(main())
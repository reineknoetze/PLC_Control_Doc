
# Import PLC Modules (Additive Patch)

This patch adds **PLC → Import PLC Module (XML)** without changing your existing MainWindow or panes.

## Wire-up (2 lines)
In your existing `MainWindow.__init__` _after_ you build the central widget and docks:

```python
from simec_controls.ui.plc_menu_injector import inject_plc_menu
from simec_controls.ui.plc_import_actions import make_import_handler

inject_plc_menu(self, make_import_handler(self))
```

Ensure your `MainWindow` defines:
```python
def refresh_asset_hierarchy(self, controller_name: str) -> None:
    # reload your tree from DB
    ...
```

The importer writes raw XML + normalized records (controllers, programs, routines, tags, AOIs),
handles idempotence and version deltas, and logs to `simec.import`/`simec.schema`/`simec.ui`.

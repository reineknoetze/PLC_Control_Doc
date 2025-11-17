def test_ui_launch_import() -> None:
    """Import smoke test for MainWindow class."""
    from simec_controls.ui.main_window import MainWindow
    assert MainWindow is not None

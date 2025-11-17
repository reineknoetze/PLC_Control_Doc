
from pathlib import Path
from simec_controls.processors.plc_importer import import_plc_module_xml, ImportConfig
from simec_controls.errors import Ok

def test_validate_and_idempotent(tmp_path: Path):
    db = tmp_path / "dlims.sqlite"
    l5x = tmp_path / "s1.l5x"
    l5x.write_text('<RSLogix5000Content SchemaRevision="1.0"><Controller Name="C1"><Programs/></Controller></RSLogix5000Content>')
    cfg = ImportConfig(db_path=db, validate_only=False)
    r1 = import_plc_module_xml(l5x, cfg); assert isinstance(r1, Ok)
    r2 = import_plc_module_xml(l5x, cfg); assert isinstance(r2, Ok)

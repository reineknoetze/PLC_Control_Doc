
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Any
import logging, sqlite3, hashlib, re, json, time
from xml.etree import ElementTree as ET

from jsonschema import validate as js_validate, Draft202012Validator  # type: ignore
from simec_controls.errors import Ok, Err, Result, AppError, ErrorKind

LOG_IMPORT = logging.getLogger("simec.import")
LOG_SCHEMA = logging.getLogger("simec.schema")

def _find_project_root(start: Path) -> Path:
    """
    Walk upwards from `start` to locate the project root. Prefer a directory
    that contains the marker file `.smc_ctrl-project`; fall back to a directory
    that contains a `schemas` folder. Ultimately, return the filesystem root of
    the repository containing this module.
    """
    for p in [start] + list(start.parents):
        if (p / ".smc_ctrl-project").exists():
            return p
        if (p / "schemas").is_dir():
            return p
    # Default to start if nothing else is found
    return start

def _load_optional_config_yaml(project_root: Path) -> dict:
    """
    Attempt to load optional YAML configuration at `<project_root>/config/config.yaml`.
    If PyYAML is not available or the file does not exist, return an empty dict.
    """
    cfg_path = project_root / "config" / "config.yaml"
    if not cfg_path.exists():
        return {}
    try:
        import yaml  # type: ignore
        with cfg_path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
            if not isinstance(data, dict):
                return {}
            return data
    except Exception:
        # Silently ignore config issues for portability
        return {}

def _resolve_schema_root(project_root: Path, config: dict) -> Path:
    """
    Determine absolute schema root, honoring optional config:
      schemas.schema_root: str (absolute or relative to project_root)
    Default: `<project_root>/schemas/json_schemas`
    """
    cfg_root = None
    try:
        cfg_root = (((config or {}).get("schemas") or {}).get("schema_root"))
    except Exception:
        cfg_root = None

    if isinstance(cfg_root, str) and cfg_root.strip():
        candidate = Path(cfg_root)
        if not candidate.is_absolute():
            candidate = project_root / candidate
        return candidate.resolve()
    return (project_root / "schemas" / "json_schemas").resolve()



@dataclass(frozen=True)
class ImportConfig:
    db_path: Path
    chunk_size_elements: int = 5000
    large_file_mb_threshold: int = 25
    enable_progress_dialog: bool = True
    abort_on_schema_mismatch: bool = True
    indexes_after_import: bool = True
    validate_only: bool = False
    facility_id: Optional[int] = None
    unit_id: Optional[int] = None

def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def _canonicalize_xml(xml_text: str) -> str:
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return xml_text

    def ser(e: ET.Element) -> str:
        attrs = " ".join(f'{k}="{v}"' for k, v in sorted(e.attrib.items()))
        start = f"<{e.tag}{(' ' + attrs) if attrs else ''}>"
        children = "".join(ser(c) for c in list(e))
        text = (e.text or "").strip()
        end = f"</{e.tag}>"
        return start + text + children + end

    return ser(root)

def _detect_vendor(root_tag: str) -> Optional[str]:
    tag = re.sub(r"^\{.*\}", "", root_tag)
    if tag == "RSLogix5000Content": return "rockwell"
    if tag == "SiemensProject": return "siemens"
    if tag == "UnityProject": return "schneider"
    return None

def _read_file_text(path: Path) -> Result[str, AppError]:
    try:
        return Ok(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception as ex:
        return Err(AppError(ErrorKind.GENERIC, f"Failed to read file: {path}", str(ex)))


def _load_schema(vendor: str) -> Result[dict, AppError]:
    """
    Load and merge the base schema with the vendor-specific schema using a robust,
    CWD-independent resolver. Returns {"allOf": [base, vendor]} on success.
    """
    # Derive project root from this module's location
    this_file = Path(__file__).resolve()
    project_root = _find_project_root(this_file.parent)
    config = _load_optional_config_yaml(project_root)
    schema_root = _resolve_schema_root(project_root, config)

    base = (schema_root / "base" / "plc_module_import_schema_base.json")
    vendor_map = {
        "rockwell": schema_root / "rockwell" / "plc_module_import_schema_l5x.json",
        "siemens":  schema_root / "siemens"  / "plc_module_import_schema_tia.json",
        "schneider":schema_root / "schneider"/ "plc_module_import_schema_unity.json",
    }

    LOG_SCHEMA.info("Using schema_root: %s", str(schema_root))

    vend_schema = vendor_map.get(vendor)
    if vend_schema is None:
        return Err(AppError(ErrorKind.GENERIC, f"Unrecognized vendor '{vendor}'"))

    # Preflight existence check with user-actionable errors
    missing = []
    if not base.exists():
        missing.append(str(base))
    if not vend_schema.exists():
        missing.append(str(vend_schema))
    if missing:
        msg = "Schema bundle not found. Missing files:\n" + "\n".join(missing)
        return Err(AppError(ErrorKind.GENERIC, f"Schema not found for vendor='{vendor}'", msg))

    try:
        base_obj = json.loads(base.read_text(encoding="utf-8"))
        vend_obj = json.loads(vend_schema.read_text(encoding="utf-8"))
        return Ok({"allOf": [base_obj, vend_obj]})
    except Exception as ex:
        return Err(AppError(ErrorKind.GENERIC, f"Failed to load schema for vendor='{vendor}'", str(ex)))


def _ensure_core_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(r"""
    PRAGMA foreign_keys = ON;
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    CREATE TABLE IF NOT EXISTS plc_imports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        vendor TEXT NOT NULL,
        version TEXT,
        xml_blob TEXT NOT NULL,
        hash TEXT NOT NULL,
        vendor_ns TEXT,
        import_timestamp TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS plc_modules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_name TEXT NOT NULL,
        vendor TEXT NOT NULL,
        current_version INTEGER NOT NULL,
        last_import_id INTEGER NOT NULL,
        UNIQUE(controller_name, vendor),
        FOREIGN KEY (last_import_id) REFERENCES plc_imports(id)
    );
    CREATE TABLE IF NOT EXISTS plc_module_deltas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module_id INTEGER NOT NULL,
        old_hash TEXT NOT NULL,
        new_hash TEXT NOT NULL,
        change_summary TEXT,
        timestamp TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (module_id) REFERENCES plc_modules(id)
    );
    CREATE TABLE IF NOT EXISTS plc_controllers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        vendor TEXT NOT NULL,
        version TEXT,
        description TEXT,
        hash TEXT NOT NULL,
        import_id INTEGER NOT NULL,
        facility_id INTEGER,
        unit_id INTEGER,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (import_id) REFERENCES plc_imports(id)
    );
    CREATE TABLE IF NOT EXISTS plc_programs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        UNIQUE(name, controller_id),
        FOREIGN KEY (controller_id) REFERENCES plc_controllers(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS plc_routines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        program_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        type TEXT,
        logic_xml TEXT,
        UNIQUE(name, program_id),
        FOREIGN KEY (program_id) REFERENCES plc_programs(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS plc_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        data_type TEXT,
        scope TEXT,
        initial_value TEXT,
        UNIQUE(name, controller_id, scope),
        FOREIGN KEY (controller_id) REFERENCES plc_controllers(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS plc_aois (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        definition_xml TEXT,
        UNIQUE(name, controller_id),
        FOREIGN KEY (controller_id) REFERENCES plc_controllers(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS schema_registry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vendor TEXT NOT NULL,
        version TEXT NOT NULL,
        schema_path TEXT NOT NULL,
        hash TEXT NOT NULL,
        registered_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_plc_imports_hash ON plc_imports(hash);
    CREATE INDEX IF NOT EXISTS idx_plc_controllers_name ON plc_controllers(name);
    CREATE INDEX IF NOT EXISTS idx_plc_programs_controller ON plc_programs(controller_id);
    CREATE INDEX IF NOT EXISTS idx_plc_routines_program ON plc_routines(program_id);
    CREATE INDEX IF NOT EXISTS idx_plc_tags_controller ON plc_tags(controller_id);
    CREATE INDEX IF NOT EXISTS idx_plc_modules_name ON plc_modules(controller_name);
    CREATE INDEX IF NOT EXISTS idx_plc_routines_name ON plc_routines(name);
    CREATE INDEX IF NOT EXISTS idx_plc_tags_name ON plc_tags(name);
    CREATE INDEX IF NOT EXISTS idx_plc_aois_name ON plc_aois(name);
    """)

def _schema_registry_hash(conn: sqlite3.Connection, vendor: str) -> Optional[str]:
    cur = conn.execute("SELECT hash FROM schema_registry WHERE vendor=? ORDER BY id DESC LIMIT 1", (vendor,))
    r = cur.fetchone()
    return r[0] if r else None

def _register_schema_if_missing(conn: sqlite3.Connection, vendor: str, schema_obj: dict, schema_path: str) -> None:
    h = hashlib.sha256(json.dumps(schema_obj, sort_keys=True).encode("utf-8")).hexdigest()
    cur = conn.execute("SELECT id FROM schema_registry WHERE vendor=? AND hash=?", (vendor, h))
    if cur.fetchone() is None:
        conn.execute(
            "INSERT INTO schema_registry(vendor, version, schema_path, hash) VALUES(?,?,?,?)",
            (vendor, "1.0", schema_path, h),
        )
        LOG_SCHEMA.info("Registered schema for vendor=%s", vendor)

def import_plc_module_xml(
    l5x_path: Path,
    cfg: ImportConfig,
    progress_callback: Optional[Callable[[str, int], None]] = None,
    is_cancelled: Optional[Callable[[], bool]] = None,
) -> Result[dict[str, Any], AppError]:
    t0 = time.time()
    progress = progress_callback or (lambda phase, v: None)
    cancelled = is_cancelled or (lambda: False)

    if l5x_path.suffix.lower() != ".l5x":
        return Err(AppError(ErrorKind.GENERIC, "File must be a .L5X XML export"))

    progress("Reading file…", 5)
    rf = _read_file_text(l5x_path)
    if isinstance(rf, Err):
        return rf
    xml_text = rf.value

    if cancelled():
        return Err(AppError(ErrorKind.GENERIC, "Import cancelled by user"))

    try:
        root = ET.fromstring(xml_text)
    except Exception as ex:
        return Err(AppError(ErrorKind.GENERIC, "Invalid XML document", str(ex)))

    vendor = _detect_vendor(root.tag)
    if vendor is None:
        return Err(AppError(ErrorKind.GENERIC, "Unrecognized PLC vendor format. Please register a schema for this vendor."))

    progress("Validating schema…", 15)
    ls = _load_schema(vendor)
    if isinstance(ls, Err):
        return ls
    schema_obj = ls.value
    try:
        projection = {"root_tag": re.sub(r'^\{.*\}', '', root.tag)}
        Draft202012Validator.check_schema(schema_obj)
        js_validate(projection, schema_obj)
    except Exception as ex:
        return Err(AppError(ErrorKind.GENERIC, "Schema validation failed", str(ex)))

    try:
        conn = sqlite3.connect(str(cfg.db_path))
    except Exception as ex:
        return Err(AppError(ErrorKind.GENERIC, f"Failed to connect to {cfg.db_path}", str(ex)))

    try:
        _ensure_core_tables(conn)
        schema_path = f"schemas/json_schemas/{vendor}/plc_module_import_schema_" + ("l5x.json" if vendor=="rockwell" else ("tia.json" if vendor=="siemens" else "unity.json"))
        _register_schema_if_missing(conn, vendor, schema_obj, schema_path)
        current_hash = _schema_registry_hash(conn, vendor)
        schema_hash_now = hashlib.sha256(json.dumps(schema_obj, sort_keys=True).encode("utf-8")).hexdigest()
        if current_hash and current_hash != schema_hash_now and cfg.abort_on_schema_mismatch:
            return Err(AppError(ErrorKind.GENERIC, "Schema registry and baseline definition differ. Please synchronize before continuing."))

        progress("Canonicalizing…", 25)
        canon = _canonicalize_xml(xml_text)
        file_hash = _sha256(canon)

        cur = conn.execute("SELECT id FROM plc_imports WHERE filename=? AND vendor=? AND hash=?", (l5x_path.name, vendor, file_hash))
        row = cur.fetchone()
        if row:
            ctrl_name = "Unknown"
            ctrl_node = root.find(".//Controller")
            if ctrl_node is not None:
                ctrl_name = ctrl_node.attrib.get("Name", ctrl_name)
            return Ok({"controller_name": ctrl_name, "programs": 0, "routines": 0, "tags": 0, "aois": 0, "import_id": row[0]})

        if cfg.validate_only:
            return Ok({"controller_name": "ValidationOnly", "programs": 0, "routines": 0, "tags": 0, "aois": 0, "import_id": None})

        progress("Beginning transaction…", 30)
        conn.execute("SAVEPOINT RAW_STORE")
        conn.execute(
            "INSERT INTO plc_imports(filename, vendor, version, xml_blob, hash, vendor_ns) VALUES(?,?,?,?,?,?)",
            (l5x_path.name, vendor, root.attrib.get("SchemaRevision", ""), canon, file_hash, None),
        )
        import_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        progress("Parsing controller…", 40)
        ctrl_node = root.find(".//Controller")
        controller_name = "Unknown"
        if ctrl_node is not None:
            controller_name = ctrl_node.attrib.get("Name", controller_name)
        controller_hash = _sha256(canon)
        conn.execute(
            """INSERT INTO plc_controllers(name, vendor, version, description, hash, import_id, facility_id, unit_id)
               VALUES(?,?,?,?,?,?,?,?)""",
            (controller_name, vendor, root.attrib.get("SchemaRevision",""), None, controller_hash, import_id, cfg.facility_id, cfg.unit_id),
        )
        controller_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        progress("Upserting module…", 50)
        cur = conn.execute("SELECT id, current_version, last_import_id FROM plc_modules WHERE controller_name=? AND vendor=?",
                           (controller_name, vendor))
        row = cur.fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO plc_modules(controller_name, vendor, current_version, last_import_id) VALUES(?,?,?,?)",
                (controller_name, vendor, 1, import_id),
            )
            module_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        else:
            module_id, current_version, _ = row
            last_hash_row = conn.execute("""
                SELECT p.hash FROM plc_imports p
                JOIN plc_modules m ON p.id = m.last_import_id
                WHERE m.id=?
            """, (module_id,)).fetchone()
            old_hash = last_hash_row[0] if last_hash_row else ""
            if old_hash != file_hash:
                conn.execute("INSERT INTO plc_module_deltas(module_id, old_hash, new_hash, change_summary) VALUES(?,?,?,?)",
                             (module_id, old_hash, file_hash, "Controller hash changed"))
                conn.execute("UPDATE plc_modules SET current_version=?, last_import_id=? WHERE id=?",
                             (current_version + 1, import_id, module_id))
            else:
                conn.execute("UPDATE plc_modules SET last_import_id=? WHERE id=?", (import_id, module_id))

        progress("Parsing programs & routines…", 65)
        programs_count = routines_count = tags_count = aois_count = 0

        for prog in root.findall(".//Programs/Program"):
            pname = prog.attrib.get("Name", "Program")
            conn.execute("INSERT INTO plc_programs(controller_id, name, description) VALUES(?,?,?)",
                         (controller_id, pname, prog.attrib.get("Description")))
            program_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            programs_count += 1

            for r in prog.findall("./Routines/Routine"):
                rname = r.attrib.get("Name", "Routine")
                rtype = r.attrib.get("Type")
                r_xml = ET.tostring(r, encoding="unicode")
                conn.execute("""INSERT INTO plc_routines(program_id, name, type, logic_xml) VALUES(?,?,?,?)""",
                             (program_id, rname, rtype, r_xml))
                routines_count += 1

        progress("Parsing controller tags…", 78)
        for tag in root.findall(".//Controller/Tags/Tag"):
            tname = tag.attrib.get("Name", "Tag")
            dtype = tag.attrib.get("DataType")
            init = None
            val = tag.find("./Data/Value")
            if val is not None and val.text is not None:
                init = val.text
            conn.execute("""INSERT INTO plc_tags(controller_id, name, data_type, scope, initial_value)
                            VALUES(?,?,?,?,?)""", (controller_id, tname, dtype, "Controller", init))
            tags_count += 1

        progress("Parsing AOIs…", 86)
        for aoi in root.findall(".//AddOnInstructionDefinitions/AddOnInstructionDefinition"):
            aname = aoi.attrib.get("Name", "AOI")
            aoi_xml = ET.tostring(aoi, encoding="unicode")
            conn.execute("""INSERT INTO plc_aois(controller_id, name, definition_xml) VALUES(?,?,?)""",
                         (controller_id, aname, aoi_xml))
            aois_count += 1

        progress("Committing…", 97)
        conn.execute("RELEASE SAVEPOINT RAW_STORE")
        conn.commit()

        dt_ms = int((time.time() - t0) * 1000)
        LOG_IMPORT.info("Import completed in %sms (programs=%s, routines=%s, tags=%s, aois=%s)",
                        dt_ms, programs_count, routines_count, tags_count, aois_count)

        return Ok({
            "controller_name": controller_name,
            "programs": programs_count,
            "routines": routines_count,
            "tags": tags_count,
            "aois": aois_count,
            "import_id": import_id,
            "duration_ms": dt_ms
        })
    except Exception as ex:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        return Err(AppError(ErrorKind.GENERIC, "Import failed", str(ex)))
    finally:
        try:
            conn.close()
        except Exception:
            pass

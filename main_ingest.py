import duckdb
import uuid
import datetime
import os
import hashlib
import argparse
import logging
import logging.handlers
import shutil

try:
    from config import DUCKDB_PATH
    from database_setup import get_db_connection
    from xml_handler import parse_xml_file, _sanitize_name as sanitize_xml_name
except ImportError as e:
    print(f"Error: Could not import necessary project modules: {e}")
    exit(1)

ARCHIVE_DIR = "processed_xml_archive"
FAILED_DIR = "failed_xml"
INGESTION_LOGIC_VERSION_NUMBER = "1.0.0-dynamic-ingestor-v4"

# --- Logging ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "ingest.log")

log = logging.getLogger("nemsis_ingest")
if not log.handlers:
    log.setLevel(logging.INFO)
    _fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    _console = logging.StreamHandler()
    _console.setFormatter(_fmt)
    log.addHandler(_console)

    _file = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
    _file.setFormatter(_fmt)
    log.addHandler(_file)


def generate_unique_file_id():
    return str(uuid.uuid4())


def get_file_md5(file_path):
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        return None
    except Exception as e:
        log.error("Error calculating MD5 for %s: %s", file_path, e)
        return None


def get_ingestion_logic_schema_id(conn, version_number):
    try:
        result = conn.execute(
            "SELECT SchemaVersionID FROM SchemaVersions WHERE VersionNumber = ?",
            (version_number,),
        ).fetchone()
        return result[0] if result else None
    except Exception as e:
        log.error("DB Error getting schema id: %s", e)
        return None


def log_processed_file(conn, processed_file_id, original_file_name, md5_hash, status, schema_version_id):
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    try:
        conn.execute(
            "INSERT INTO XMLFilesProcessed (ProcessedFileID, OriginalFileName, MD5Hash, ProcessingTimestamp, Status, SchemaVersionID, DemographicGroup) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (processed_file_id, original_file_name, md5_hash, timestamp, status, schema_version_id, None),
        )
        log.info("Logged file %s (ID: %s) with status %s", original_file_name, processed_file_id, status)
        return True
    except Exception as e:
        log.error("DB error logging processed file %s: %s", original_file_name, e)
        return False


def archive_file(file_path, archive_directory):
    if not os.path.exists(file_path):
        return False
    try:
        if not os.path.exists(archive_directory):
            os.makedirs(archive_directory)
        base_filename = os.path.basename(file_path)
        archive_path = os.path.join(archive_directory, base_filename)
        if os.path.exists(archive_path):
            log.warning("File %s already in archive. Overwriting.", base_filename)
        shutil.move(file_path, archive_path)
        log.info("File %s archived to %s", file_path, archive_path)
        return True
    except Exception as e:
        log.error("Error archiving file %s: %s", file_path, e)
        return False


_table_column_cache = {}


def get_table_columns(conn, table_name):
    safe_table_name = sanitize_xml_name(table_name)
    if safe_table_name in _table_column_cache:
        return _table_column_cache[safe_table_name]

    cols = set()
    try:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            (safe_table_name.lower(),),
        ).fetchall()
        cols = {row[0] for row in rows}
        _table_column_cache[safe_table_name] = cols
    except Exception as e:
        if "does not exist" not in str(e).lower():
            log.error("Error getting columns for %s: %s", safe_table_name, e)
        _table_column_cache[safe_table_name] = set()
    return cols


def ensure_table_and_columns(conn, table_name_suggestion, element_attributes, common_db_columns):
    table_name_raw = sanitize_xml_name(table_name_suggestion)
    if not table_name_raw:
        log.error("Table name suggestion is empty after sanitization.")
        return None, set()

    table_name = f'"{table_name_raw.lower()}"'
    existing_columns = get_table_columns(conn, table_name_raw)

    common_cols_sql = [
        '"element_id" TEXT PRIMARY KEY',
        '"parent_element_id" TEXT',
        '"pcr_uuid_context" TEXT',
        '"original_tag_name" TEXT',
        '"text_content" TEXT',
    ]

    if not existing_columns:
        attr_cols_for_create = []
        current_common_names = {c.split()[0].strip('"') for c in common_cols_sql}
        for attr in element_attributes.keys():
            sanitized_attr = sanitize_xml_name(attr).lower()
            if sanitized_attr not in current_common_names:
                attr_cols_for_create.append(f'"{sanitized_attr}" TEXT')
                current_common_names.add(sanitized_attr)

        final_cols_for_create = common_cols_sql + list(set(attr_cols_for_create))
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(final_cols_for_create)});"
        try:
            conn.execute(create_sql)
            created_cols = {col_def.split()[0].strip('"').lower() for col_def in final_cols_for_create}
            _table_column_cache[table_name_raw] = created_cols
            log.info("Table %s created.", table_name)
        except Exception as e:
            log.error("Error creating table %s: %s", table_name, e)
            return None, set()

    current_table_cols = get_table_columns(conn, table_name_raw)
    missing_attr_cols = set()
    for attr in element_attributes.keys():
        sanitized_attr = sanitize_xml_name(attr).lower()
        if sanitized_attr not in current_table_cols and sanitized_attr not in {
            c.split()[0].strip('"') for c in common_cols_sql
        }:
            missing_attr_cols.add(sanitized_attr)

    for col_name in missing_attr_cols:
        col_name_quoted = f'"{col_name}"'
        try:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name_quoted} TEXT;")
            log.info("Added column %s to %s", col_name_quoted, table_name)
            _table_column_cache[table_name_raw].add(col_name)
        except Exception as e:
            log.error("Error adding %s to %s: %s", col_name_quoted, table_name, e)

    return table_name_raw, get_table_columns(conn, table_name_raw)


def delete_existing_pcr_data(conn, pcr_uuid):
    if not pcr_uuid:
        return
    log.debug("Checking PCR UUID %s for pre-deletion.", pcr_uuid)
    try:
        tables_to_check = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_name NOT IN ('SchemaVersions', 'XMLFilesProcessed',
                'XSD_Elements', 'XSD_SimpleTypes', 'XSD_Enumerations',
                'XSD_ElementAttributes', 'XSD_ElementValueSet',
                'ElementDefinitions', 'FieldDefinitions',
                'view_registry', 'view_columns', 'view_excludes', 'gnis_places')
        """).fetchall()

        for (table_name_raw,) in tables_to_check:
            columns = get_table_columns(conn, table_name_raw)
            if "pcr_uuid_context" in columns:
                table_name_quoted = f'"{table_name_raw}"'
                try:
                    conn.execute(f'DELETE FROM {table_name_quoted} WHERE "pcr_uuid_context" = ?', (pcr_uuid,))
                    count = conn.execute(f'SELECT changes()').fetchone()
                    # DuckDB doesn't have changes() - use a count before/after approach
                except Exception as e:
                    log.error("Error deleting from %s: %s", table_name_quoted, e)

        log.debug("Deletion complete for PCR %s", pcr_uuid)
    except Exception as e:
        log.error("DB error during PCR deletion: %s", e)


def move_to_failed(file_path):
    """Move a file to the failed_xml/ directory so it can be reimported."""
    failed_dir = os.path.join(BASE_DIR, FAILED_DIR)
    if not os.path.exists(file_path):
        return None
    try:
        if not os.path.exists(failed_dir):
            os.makedirs(failed_dir)
        dest = os.path.join(failed_dir, os.path.basename(file_path))
        if os.path.exists(dest):
            # Append timestamp to avoid overwriting previous failures
            name, ext = os.path.splitext(os.path.basename(file_path))
            dest = os.path.join(failed_dir, f"{name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}")
        shutil.move(file_path, dest)
        log.info("Failed file moved to %s", dest)
        return dest
    except Exception as e:
        log.error("Could not move failed file %s: %s", file_path, e)
        return None


def process_xml_file(db_conn, xml_file_path, ingestion_schema_id):
    """Ingest a NEMSIS XML file. Returns (success: bool, failure_reason: str|None)."""
    log.info("Processing XML: %s", xml_file_path)
    processed_file_id = generate_unique_file_id()
    original_file_name = os.path.basename(xml_file_path)
    md5_hash = get_file_md5(xml_file_path)

    if md5_hash is None and os.path.exists(xml_file_path):
        reason = "Could not compute MD5 hash"
        log.error("%s — %s", original_file_name, reason)
        log_processed_file(db_conn, processed_file_id, original_file_name, None, "Error_MD5", ingestion_schema_id)
        move_to_failed(xml_file_path)
        return False, reason

    if not os.path.exists(xml_file_path):
        reason = "File not found"
        log.error("%s — %s at %s", original_file_name, reason, xml_file_path)
        log_processed_file(db_conn, processed_file_id, original_file_name, md5_hash if md5_hash else "N/A", "Error_FileNotFound", ingestion_schema_id)
        return False, reason

    elements_data = parse_xml_file(xml_file_path)

    if not elements_data:
        reason = "XML parsing returned no elements (empty or malformed file)"
        log.error("%s — %s", original_file_name, reason)
        log_processed_file(db_conn, processed_file_id, original_file_name, md5_hash, "Error_Parsing_Empty", ingestion_schema_id)
        move_to_failed(xml_file_path)
        return False, reason

    unique_pcr_uuids_in_file = {el["pcr_uuid_context"] for el in elements_data if el.get("pcr_uuid_context")}

    common_db_columns = {"element_id", "parent_element_id", "pcr_uuid_context", "original_tag_name", "text_content"}

    try:
        db_conn.execute("BEGIN")

        if unique_pcr_uuids_in_file:
            log.info("Found %d unique PCR UUID(s) — clearing existing data before insert.", len(unique_pcr_uuids_in_file))
            for pcr_uuid in unique_pcr_uuids_in_file:
                delete_existing_pcr_data(db_conn, pcr_uuid)
        else:
            log.info("No PCR UUIDs found in file; skipping pre-deletion.")

        for element in elements_data:
            table_name_raw, actual_table_columns = ensure_table_and_columns(
                db_conn, element["table_suggestion"], element["attributes"], common_db_columns
            )

            if not table_name_raw or not actual_table_columns:
                raise Exception(f"Failed to ensure table/columns for {element['table_suggestion']}")

            insert_data = {
                "element_id": element["element_id"],
                "parent_element_id": element.get("parent_element_id"),
                "pcr_uuid_context": element.get("pcr_uuid_context"),
                "original_tag_name": element["element_tag"],
                "text_content": element.get("text_content"),
            }
            for attr_key, attr_value in element["attributes"].items():
                insert_data[sanitize_xml_name(attr_key).lower()] = attr_value

            filtered_insert_data = {k: v for k, v in insert_data.items() if k.lower() in actual_table_columns}

            cols_for_sql = ", ".join([f'"{k}"' for k in filtered_insert_data.keys()])
            placeholders = ", ".join(["?"] * len(filtered_insert_data))
            values = list(filtered_insert_data.values())

            table_name_quoted = f'"{table_name_raw.lower()}"'
            sql = f"INSERT INTO {table_name_quoted} ({cols_for_sql}) VALUES ({placeholders})"
            try:
                db_conn.execute(sql, values)
            except Exception as e:
                log.error("DB Insert Error: %s  SQL: %s", e, sql)
                raise

        db_conn.execute("COMMIT")
        log.info("Ingestion succeeded: %s (%d PCRs)", original_file_name, len(unique_pcr_uuids_in_file))
        log_processed_file(db_conn, processed_file_id, original_file_name, md5_hash, "Staged_Dynamic_DuckDB_V4", ingestion_schema_id)

        if not archive_file(xml_file_path, ARCHIVE_DIR):
            log.warning("Data staged for %s, but failed to archive the file.", original_file_name)
        return True, None

    except Exception as e:
        db_conn.execute("ROLLBACK")
        reason = f"Database transaction error: {e}"
        log.error("%s — %s. Rolled back.", original_file_name, reason)
        log_processed_file(db_conn, processed_file_id, original_file_name, md5_hash, "Error_Staging_Tx_DuckDB_V4", ingestion_schema_id)
        move_to_failed(xml_file_path)
        return False, reason
    finally:
        _table_column_cache.clear()


def main():
    global ARCHIVE_DIR
    parser = argparse.ArgumentParser(description="NEMSIS XML Dynamic Data Ingestion Tool V4 (DuckDB)")
    parser.add_argument("xml_file", help="Path to the NEMSIS XML file to process.")
    parser.add_argument("--archive-dir", default=ARCHIVE_DIR, help=f"Archive directory. Default: {ARCHIVE_DIR}")

    args = parser.parse_args()
    ARCHIVE_DIR = args.archive_dir

    log.info("--- NEMSIS Dynamic Data Ingestion V4 (DuckDB) ---")
    log.info("DB: %s  Archive: %s  Version: %s", DUCKDB_PATH, ARCHIVE_DIR, INGESTION_LOGIC_VERSION_NUMBER)

    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            log.error("Could not connect to database.")
            return

        if not os.path.exists(ARCHIVE_DIR):
            os.makedirs(ARCHIVE_DIR)

        ingestion_schema_id = get_ingestion_logic_schema_id(conn, INGESTION_LOGIC_VERSION_NUMBER)
        if ingestion_schema_id is None:
            log.error("Ingestion version '%s' not in SchemaVersions. Run database_setup.py first.", INGESTION_LOGIC_VERSION_NUMBER)
            return
        log.info("Using IngestionSchemaID: %s for Version: %s", ingestion_schema_id, INGESTION_LOGIC_VERSION_NUMBER)

        success, reason = process_xml_file(conn, args.xml_file, ingestion_schema_id)

        if success:
            log.info("--- Ingestion for %s completed successfully. ---", args.xml_file)
        else:
            log.error("--- Ingestion for %s FAILED: %s ---", args.xml_file, reason)
            log.error("File moved to %s/ — fix the issue and reimport.", FAILED_DIR)

    except Exception as e:
        log.exception("Critical error in main: %s", e)
    finally:
        if conn:
            conn.close()
        log.info("Database connection closed.")


if __name__ == "__main__":
    main()

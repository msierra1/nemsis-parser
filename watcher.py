"""
Hot-folder watcher for NEMSIS XML ingestion.

Monitors the `nemsis_xml/` directory and automatically ingests any new .xml
file that appears there, then archives it — same as running main_ingest.py
manually.

Usage:
    python3 watcher.py                  # watches nemsis_xml/ by default
    python3 watcher.py --watch-dir /path/to/folder
"""

import argparse
import logging
import logging.handlers
import os
import subprocess
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from config import DUCKDB_PATH
from database_setup import get_db_connection
from main_ingest import (
    ARCHIVE_DIR,
    FAILED_DIR,
    INGESTION_LOGIC_VERSION_NUMBER,
    get_file_md5,
    get_ingestion_logic_schema_id,
    process_xml_file,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "watcher.log")

# Path to the ems-quality-measures project for incremental refresh
QUALITY_PROJECT = os.path.join(os.path.dirname(BASE_DIR), "ems-quality-measures")
DEFAULT_WATCH_DIR = os.path.join(BASE_DIR, "nemsis_xml")

# --- Logging: console + rotating file (5 MB, keep 3 backups) ---
log = logging.getLogger("nemsis_watcher")
log.setLevel(logging.INFO)

_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_console = logging.StreamHandler()
_console.setFormatter(_fmt)
log.addHandler(_console)

_file = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
_file.setFormatter(_fmt)
log.addHandler(_file)


def refresh_quality_db():
    """Trigger incremental refresh of the ems-quality-measures DB.

    Runs as a subprocess so it doesn't block the watcher or interfere
    with the nemsis-parser's DuckDB connection.
    """
    refresh_script = os.path.join(QUALITY_PROJECT, "src", "load_nemsis.py")
    if not os.path.exists(refresh_script):
        log.warning("Quality measures project not found at %s — skipping refresh.", QUALITY_PROJECT)
        return

    try:
        log.info("Triggering incremental refresh of quality DB…")
        result = subprocess.run(
            ["python3", "-c",
             "import sys; sys.path.insert(0, 'src'); "
             "from load_nemsis import incremental_refresh; "
             "r = incremental_refresh(); "
             f"print(f'Quality DB refresh: {{r}}')"],
            cwd=QUALITY_PROJECT,
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode == 0:
            log.info("Quality DB refresh: %s", result.stdout.strip())
        else:
            log.error("Quality DB refresh failed: %s", result.stderr.strip())
    except subprocess.TimeoutExpired:
        log.error("Quality DB refresh timed out (120s).")
    except Exception as e:
        log.error("Quality DB refresh error: %s", e)


def notify(title: str, message: str):
    """Send a macOS notification via osascript (silent no-op on failure)."""
    try:
        subprocess.run(
            ["osascript", "-e", f'display notification "{message}" with title "{title}"'],
            check=False,
            capture_output=True,
        )
    except Exception:
        pass


class XMLIngestHandler(FileSystemEventHandler):
    def __init__(self, ingestion_schema_id):
        super().__init__()
        self.ingestion_schema_id = ingestion_schema_id
        self._seen: set[str] = set()

    def on_created(self, event):
        if event.is_directory:
            return
        self._handle(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        self._handle(event.dest_path)

    def _handle(self, path: str):
        if not path.lower().endswith(".xml"):
            return
        if path in self._seen:
            return

        time.sleep(0.5)
        if not os.path.exists(path):
            return

        filename = os.path.basename(path)
        self._seen.add(path)

        conn = None
        try:
            conn = get_db_connection()
            if conn is None:
                log.error("Could not connect to database. Skipping %s", filename)
                notify("NEMSIS Watcher ❌", f"DB connection failed for {filename}")
                return

            # Duplicate check via MD5 — only skip if a SUCCESSFUL import exists
            # Files with Error_* status are NOT considered duplicates
            md5 = get_file_md5(path)
            existing = conn.execute(
                "SELECT processingtimestamp FROM XMLFilesProcessed WHERE md5hash = ? AND status LIKE 'Staged%' LIMIT 1",
                (md5,),
            ).fetchone()
            if existing:
                log.warning("Skipping %s — duplicate of file successfully ingested on %s", filename, existing[0].strftime("%Y-%m-%d %H:%M"))
                notify("NEMSIS Watcher ⚠️", f"{filename} skipped (duplicate of successful import)")
                from main_ingest import archive_file
                archive_file(path, ARCHIVE_DIR)
                return

            log.info("New file detected: %s — starting ingestion", filename)
            notify("NEMSIS Watcher", f"Ingesting {filename}…")

            success, reason = process_xml_file(conn, path, self.ingestion_schema_id)

            if success:
                log.info("Ingestion succeeded: %s", filename)
                notify("NEMSIS Watcher ✅", f"{filename} ingested successfully")
                refresh_quality_db()
            else:
                log.error("Ingestion failed: %s — %s", filename, reason)
                notify("NEMSIS Watcher ❌", f"{filename} FAILED: {reason}. Moved to {FAILED_DIR}/ — reimport after fixing.")
                # Remove from _seen so the file can be retried if re-dropped
                self._seen.discard(path)

        except Exception as e:
            log.exception("Unexpected error ingesting %s: %s", filename, e)
            notify("NEMSIS Watcher ❌", f"Error ingesting {filename}: {e}")
            # Allow retry on unexpected errors too
            self._seen.discard(path)
        finally:
            if conn:
                conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="NEMSIS hot-folder watcher — auto-ingests XML files on arrival"
    )
    parser.add_argument(
        "--watch-dir",
        default=DEFAULT_WATCH_DIR,
        help=f"Directory to watch. Default: {DEFAULT_WATCH_DIR}",
    )
    args = parser.parse_args()

    watch_dir = os.path.abspath(args.watch_dir)
    if not os.path.exists(watch_dir):
        os.makedirs(watch_dir)
        log.info("Created watch directory: %s", watch_dir)

    log.info("Log file: %s", LOG_FILE)
    log.info("Connecting to %s ...", DUCKDB_PATH)

    conn = get_db_connection()
    if conn is None:
        log.error("Cannot connect to database. Exiting.")
        return

    ingestion_schema_id = get_ingestion_logic_schema_id(conn, INGESTION_LOGIC_VERSION_NUMBER)
    conn.close()

    if ingestion_schema_id is None:
        log.error(
            "Ingestion version '%s' not found in SchemaVersions. Run database_setup.py first.",
            INGESTION_LOGIC_VERSION_NUMBER,
        )
        return

    log.info("Watching: %s  (archive → %s)", watch_dir, ARCHIVE_DIR)
    log.info("Press Ctrl+C to stop.\n")

    handler = XMLIngestHandler(ingestion_schema_id)
    observer = Observer()
    observer.schedule(handler, watch_dir, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopping watcher...")
    finally:
        observer.stop()
        observer.join()
        log.info("Watcher stopped.")


if __name__ == "__main__":
    main()

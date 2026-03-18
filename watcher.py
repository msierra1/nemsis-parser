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

from config import PG_HOST, PG_PORT, PG_DATABASE
from database_setup import get_db_connection
from main_ingest import (
    ARCHIVE_DIR,
    INGESTION_LOGIC_VERSION_NUMBER,
    get_ingestion_logic_schema_id,
    process_xml_file,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "watcher.log")
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
        log.info("New file detected: %s — starting ingestion", filename)
        notify("NEMSIS Watcher", f"Ingesting {filename}…")

        conn = None
        try:
            conn = get_db_connection()
            if conn is None:
                log.error("Could not connect to database. Skipping %s", filename)
                notify("NEMSIS Watcher ❌", f"DB connection failed for {filename}")
                return

            # Redirect stdout so ingest detail appears in watcher.log
            import io, sys
            captured = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured

            try:
                success = process_xml_file(conn, path, self.ingestion_schema_id)
            finally:
                sys.stdout = old_stdout
                detail = captured.getvalue().strip()
                if detail:
                    for line in detail.splitlines():
                        log.info("  [ingest] %s", line)

            if success:
                log.info("Ingestion succeeded: %s", filename)
                notify("NEMSIS Watcher ✅", f"{filename} ingested successfully")
            else:
                log.error("Ingestion failed: %s — check watcher.log", filename)
                notify("NEMSIS Watcher ❌", f"{filename} failed — check watcher.log")

        except Exception as e:
            log.exception("Unexpected error ingesting %s: %s", filename, e)
            notify("NEMSIS Watcher ❌", f"Error ingesting {filename}")
        finally:
            if conn:
                conn.close()
            self._seen.discard(path)


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
    log.info("Connecting to %s@%s:%s ...", PG_DATABASE, PG_HOST, PG_PORT)

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

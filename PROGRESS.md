# NEMSIS Parser Project Progress

## Setup Complete
- DuckDB database: `ems.duckdb`
- Python 3.13
- Dependencies: pandas, duckdb, python-dotenv, requests, watchdog
- `.env` configured with DB credentials
- `database_setup.py` run — core tables created
- `create_definitions.py` run — NEMSIS XSD schema loaded

## Data Ingested
- Multiple XML files loaded via `main_ingest.py` and hot-folder watcher
- Agencies: 0502 (1,292+ PCRs), 0406 (474+ PCRs)
- All NEMSIS tables created dynamically (eVitals, eTimes, etc.)

## Views
- `ems_views.py init` run
- Views (all verified):
  - `headers` — per-PCR identifiers: PCR#, agency, incident#, unit, response mode
  - `times` — 14 timestamp columns
  - `vitals` — 40 clinical columns
  - `procedures` — 14 columns
  - `patient` — demographics: name, DOB, age, gender, race, address
  - `patient_full` — extends patient with resolved city/county/state via GNIS

## GNIS Locality Resolution
- NEMSIS encodes cities as USGS GNIS Feature IDs
- `gnis_places` table loaded from USGS national populated places file + NY/NJ full domestic names files (190k+ records)
- `gnis_name(feature_id)` PostgreSQL function for inline lookups

## Logging & Error Handling
- `main_ingest.py` uses rotating file logger (`ingest.log`, 5MB, 3 backups)
- `process_xml_file()` returns `(success, failure_reason)` tuple
- Failed files moved to `failed_xml/` directory (not archived, not treated as duplicates)
- macOS notifications include failure reason and reimport instructions
- Failed files can be retried by re-dropping into `nemsis_xml/`

## Duplicate Detection
- **File-level**: MD5 hash check — only files that succeeded (`Staged%` status) are treated as duplicates
- **PCR-level**: Existing PCR data is deleted before re-insert, so overlapping files are safe

## Hot-Folder Watcher
- `watcher.py` watches `nemsis_xml/` and auto-ingests any `.xml` file dropped there
- Files archived to `processed_xml_archive/` on success
- Failed files moved to `failed_xml/`
- `watchdog` dependency for filesystem events
- Rotating log: `watcher.log` (5MB, 3 backups)
- **Auto-refreshes** the ems-quality-measures quality DB after each successful ingest (incremental — only new PCRs)

## Quality DB Integration
- After each successful ingest, the watcher triggers `incremental_refresh()` in the ems-quality-measures project
- Only new PCR UUIDs are flattened and upserted — scales to 100k+ records
- Both ops and quality dashboards read from the quality DB

## Useful Commands
```bash
# Start the hot-folder watcher (drop XMLs into nemsis_xml/ to auto-ingest)
cd ~/Projects/nemsis-parser && python3 watcher.py

# Watch a custom folder instead
python3 watcher.py --watch-dir /path/to/your/drop/folder

# Ingest a new XML file manually
python3 main_ingest.py nemsis_xml/yourfile.xml

# Batch ingest all XML files
for f in ~/Projects/nemsis-parser/nemsis_xml/*.xml; do python3 main_ingest.py "$f"; done

# Rebuild views after changes
python3 ems_views.py rebuild

# Connect to database
python3 -c "import duckdb; con = duckdb.connect('ems.duckdb'); print(con.execute('SHOW TABLES').fetchdf())"
```

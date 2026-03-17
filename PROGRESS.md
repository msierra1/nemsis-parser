# NEMSIS Parser Project Progress

## Setup Complete
- PostgreSQL 17 installed and running on localhost:5432
- Database: `nemsis_db`
- Python 3.13.4
- Dependencies installed: pandas, psycopg2-binary, python-dotenv, requests
- `.env` configured with DB credentials
- `database_setup.py` run — core tables created
- `create_definitions.py` run — NEMSIS XSD schema loaded

## Data Ingested
- First XML file loaded successfully via `main_ingest.py`
- All NEMSIS tables created dynamically (eVitals, eTimes, etc.)

## Views
- `ems_views.py init` run
- Views (all verified):
  - `headers` — cardinality one, sections: record + response (per-PCR identifiers: PCR#, agency, incident#, unit, response mode)
  - `times` — cardinality one, section: times (14 timestamp columns)
  - `vitals` — cardinality many, section: vitals (40 clinical columns, 1492 rows)
  - `procedures` — cardinality many, section: procedures (14 columns, 174 rows)
  - `patient` — cardinality one, section: patient (demographics: name, DOB, age, gender, race, address)

## Known Issues / Fixes
- Initial vitals view used `--section vitals` which only returned pain score
  - Fixed by using `--section evitals` to match actual table names
- `headers` view was returning only a raw container element
  - Rebuilt using `record` + `response` sections with explicit per-PCR fields
- Duplicate columns (`last_name_2`, etc.) caused by `fielddefinitions` and `XSD_Elements` JOINs producing multiple rows per element
  - Fixed with `DISTINCT ON` in both CTEs in `ems_views.py`
- `vitals` section registration was `evitals`; corrected to `vitals` to match `classify_section()` output

## GNIS Locality Resolution
- NEMSIS encodes cities as USGS GNIS Feature IDs
- `gnis_places` table loaded from USGS national populated places file + NY/NJ full domestic names files (190k+ records)
- `load_gnis.py` — downloads and upserts GNIS data; run with `--file` to skip download
- `gnis_name(feature_id)` PostgreSQL function for inline lookups
- `patient_full` view — extends `patient` with resolved `city`, `county`, and `state` (via `gnis_places` JOIN)
- Source files: `gnis_populated_places.zip`, `gnis_ny.zip`, `gnis_nj.zip`

## Next Steps
- [ ] Load more XML files for broader dataset
- [ ] Explore data with SQL queries
- [ ] Decide on analytics goals (dashboard, QI reporting, etc.)

## Hot-Folder Watcher
- `watcher.py` watches `nemsis_xml/` and auto-ingests any `.xml` file dropped there
- Files are archived to `processed_xml_archive/` on success (same as manual ingest)
- `watchdog` added as a dependency

## Useful Commands
```bash
# Start the hot-folder watcher (drop XMLs into nemsis_xml/ to auto-ingest)
cd ~/Projects/nemsis-parser && python3 watcher.py

# Watch a custom folder instead
python3 watcher.py --watch-dir /path/to/your/drop/folder

# Ingest a new XML file
cd ~/Projects/nemsis-parser && python3 main_ingest.py nemsis_xml/yourfile.xml

# Batch ingest all XML files
for f in ~/Projects/nemsis-parser/nemsis_xml/*.xml; do python3 main_ingest.py "$f"; done

# Rebuild views after changes
python3 ems_views.py rebuild

# Connect to database
/Library/PostgreSQL/17/bin/psql -U postgres -d nemsis_db

# Query vitals (vertical format)
/Library/PostgreSQL/17/bin/psql -U postgres -d nemsis_db -x -c "SELECT * FROM vitals LIMIT 5;"
```

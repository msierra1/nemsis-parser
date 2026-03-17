"""
Download and load USGS GNIS Populated Places into PostgreSQL.

Creates a gnis_places table with feature_id, feature_name, state_name,
state_numeric, and county_name — the columns needed to resolve NEMSIS
city codes.

Usage:
    python3 load_gnis.py                  # download fresh copy and load
    python3 load_gnis.py --file path.zip  # use an already-downloaded zip
"""

import argparse
import io
import os
import zipfile

import psycopg2
import psycopg2.extras
import requests

from database_setup import get_db_connection

GNIS_URL = "https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/Topical/PopulatedPlaces_National_Text.zip"
DEFAULT_ZIP = os.path.join(os.path.dirname(__file__), "gnis_populated_places.zip")

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS gnis_places (
    feature_id      TEXT PRIMARY KEY,
    feature_name    TEXT NOT NULL,
    state_name      TEXT,
    state_numeric   TEXT,
    county_name     TEXT
);
"""

UPSERT = """
INSERT INTO gnis_places (feature_id, feature_name, state_name, state_numeric, county_name)
VALUES %s
ON CONFLICT (feature_id) DO UPDATE
    SET feature_name  = EXCLUDED.feature_name,
        state_name    = EXCLUDED.state_name,
        state_numeric = EXCLUDED.state_numeric,
        county_name   = EXCLUDED.county_name;
"""


def download(url: str, dest: str):
    print(f"Downloading {url} ...")
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)
    print(f"Saved to {dest}")


def load(conn, zip_path: str):
    print(f"Reading {zip_path} ...")
    with zipfile.ZipFile(zip_path) as zf:
        txt_name = next(n for n in zf.namelist() if n.endswith(".txt"))
        data = zf.read(txt_name).decode("utf-8-sig")

    seen = {}
    lines = data.splitlines()
    header = [h.lower() for h in lines[0].split("|")]
    idx = {col: header.index(col) for col in ("feature_id", "feature_name", "state_name", "state_numeric", "county_name")}

    for line in lines[1:]:
        parts = line.split("|")
        if len(parts) < len(header):
            continue
        fid = parts[idx["feature_id"]].strip()
        seen[fid] = (
            fid,
            parts[idx["feature_name"]].strip(),
            parts[idx["state_name"]].strip(),
            parts[idx["state_numeric"]].strip(),
            parts[idx["county_name"]].strip(),
        )

    rows = list(seen.values())

    print(f"Parsed {len(rows):,} records. Loading into PostgreSQL ...")
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE)
        psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=1000)
    conn.commit()
    print(f"Done — {len(rows):,} rows upserted into gnis_places.")


def main():
    parser = argparse.ArgumentParser(description="Load USGS GNIS populated places into nemsis_db")
    parser.add_argument("--file", default=None, help="Path to existing zip file (skips download)")
    args = parser.parse_args()

    zip_path = args.file or DEFAULT_ZIP

    if not args.file:
        download(GNIS_URL, zip_path)

    conn = get_db_connection()
    if conn is None:
        print("Could not connect to database.")
        return
    try:
        load(conn, zip_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

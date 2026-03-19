import duckdb
import datetime

from config import DUCKDB_PATH


def get_db_connection() -> duckdb.DuckDBPyConnection:
    """Opens a connection to the DuckDB database file."""
    conn = duckdb.connect(DUCKDB_PATH)
    print(f"Successfully connected to DuckDB: {DUCKDB_PATH}")
    return conn


def create_xsd_schema_tables(conn):
    """Creates XSD schema metadata tables."""
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS xsd_elements_id_seq;

        CREATE TABLE IF NOT EXISTS XSD_Elements (
          id          INTEGER PRIMARY KEY DEFAULT nextval('xsd_elements_id_seq'),
          DatasetName TEXT NOT NULL,
          ElementNumber TEXT,
          ElementName TEXT NOT NULL,
          XMLName     TEXT NOT NULL,
          TypeName    TEXT,
          GroupName   TEXT,
          Definition  TEXT,
          Usage       TEXT,
          v2Number    TEXT,
          National    BOOLEAN,
          State       BOOLEAN,
          MinOccurs   INTEGER,
          MaxOccurs   TEXT,
          Nillable    BOOLEAN DEFAULT FALSE,
          HasSimpleContent BOOLEAN DEFAULT FALSE,
          CreatedAt   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS XSD_SimpleTypes (
          TypeName      TEXT PRIMARY KEY,
          BaseType      TEXT,
          Documentation TEXT
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS XSD_Enumerations (
          TypeName        TEXT REFERENCES XSD_SimpleTypes(TypeName),
          Code            TEXT,
          CodeDescription TEXT,
          PRIMARY KEY (TypeName, Code)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS XSD_ElementAttributes (
          ElementId     INTEGER REFERENCES XSD_Elements(id),
          AttributeName TEXT,
          AllowedValues TEXT,
          UNIQUE (ElementId, AttributeName)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS XSD_ElementValueSet (
          ElementId INTEGER REFERENCES XSD_Elements(id),
          TypeName  TEXT REFERENCES XSD_SimpleTypes(TypeName),
          PRIMARY KEY (ElementId, TypeName)
        );
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_xe_dataset_num ON XSD_Elements(DatasetName, ElementNumber);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_xe_xmlname ON XSD_Elements(XMLName);")
    print("Checked/Created XSD_* schema metadata tables and indexes.")


def create_tables(conn):
    """Creates the core database tables if they don't exist."""
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS schema_versions_id_seq;

        CREATE TABLE IF NOT EXISTS SchemaVersions (
            SchemaVersionID INTEGER PRIMARY KEY DEFAULT nextval('schema_versions_id_seq'),
            VersionNumber   TEXT NOT NULL UNIQUE,
            CreationDate    TIMESTAMPTZ NOT NULL,
            UpdateDate      TIMESTAMPTZ,
            Description     TEXT,
            DemographicGroup TEXT
        );
    """)
    print("Checked/Created SchemaVersions table.")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS XMLFilesProcessed (
            ProcessedFileID  TEXT PRIMARY KEY,
            OriginalFileName TEXT NOT NULL,
            MD5Hash          TEXT,
            ProcessingTimestamp TIMESTAMPTZ NOT NULL,
            Status           TEXT NOT NULL,
            SchemaVersionID  INTEGER REFERENCES SchemaVersions(SchemaVersionID),
            DemographicGroup TEXT
        );
    """)
    print("Checked/Created XMLFilesProcessed table.")
    print("Core database tables checked/created successfully.")


def add_initial_schema_version(
    conn,
    version_number="1.0.0-dynamic-ingestor-v4",
    description="Dynamic table logic v4 (PCR UUID based overwrite).",
    demographic_group=None,
):
    """Adds an initial record to SchemaVersions if none exists."""
    result = conn.execute("SELECT COUNT(*) FROM SchemaVersions").fetchone()
    if result[0] == 0:
        creation_date = datetime.datetime.now(datetime.timezone.utc)
        try:
            conn.execute(
                "INSERT INTO SchemaVersions (VersionNumber, CreationDate, Description, DemographicGroup) VALUES (?, ?, ?, ?)",
                (version_number, creation_date, description, demographic_group),
            )
            print(f"Initial schema version {version_number} added to SchemaVersions.")
        except Exception as e:
            print(f"Error adding initial schema version: {e}")
    else:
        print("SchemaVersions table already contains entries. Skipping.")


if __name__ == "__main__":
    print("Initializing DuckDB database.")
    db_conn = None
    try:
        db_conn = get_db_connection()
        create_tables(db_conn)
        create_xsd_schema_tables(db_conn)
        add_initial_schema_version(db_conn, demographic_group="SystemInternal_DuckDB_v4")
    except Exception as e:
        print(f"Error during setup: {e}")
    finally:
        if db_conn:
            db_conn.close()
            print("DuckDB connection closed.")
    print("Database setup finished.")

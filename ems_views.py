#!/usr/bin/env python3
"""
EMS/NEMSIS dynamic views: init metadata + manage/rebuild section views.

Usage examples:
  python ems_views.py init
  python ems_views.py rebuild
  python ems_views.py list-views

  python ems_views.py add-view v_patient --cardinality one --section patient --use-resolved 1
  python ems_views.py add-col  v_patient ePatient.01 --alias patient_last_name --agg MAX
  python ems_views.py add-col  v_patient ePatient.02 --alias patient_first_name
  python ems_views.py exclude  v_patient ePatient.99
  python ems_views.py delete-view v_patient

Env:
  Uses config.get_db_connection() if importable; else PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.
"""

import argparse
import os
import re
import sys
from typing import Optional, List, Tuple

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConn


# ---------------------------
# DB connection
# ---------------------------
def get_conn() -> PGConn:
    """
    Try your project's get_db_connection(); otherwise use PG* env.
    """
    try:
        from database_setup import get_db_connection as project_conn  # type: ignore

        conn = project_conn()
        if conn:
            return conn
    except ImportError as e:
        print(f"Error: Could not import necessary project modules: {e}")
        print(
            "Please ensure config.py, database_setup.py (updated), and xml_handler.py are in the PYTHONPATH."
        )
        exit(1)

    dsn = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5432"),
        "dbname": os.getenv("PGDATABASE", "postgres"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", ""),
    }
    conn = psycopg2.connect(**dsn)  # type: ignore
    conn.autocommit = False
    return conn


# ---------------------------
# Helpers
# ---------------------------
IDENT_RE = re.compile(r"[^a-zA-Z0-9]+")


def ident_sanitize_py(src: str) -> str:
    """
    Pure-Python mirror of ident_sanitize(): produce a safe SQL identifier.
    """
    s = IDENT_RE.sub("_", src or "").strip("_").lower()
    if not s:
        import hashlib

        s = "col_" + hashlib.md5((src or "").encode("utf-8")).hexdigest()[:8]
    if s[0].isdigit():
        s = "x_" + s
    return s


def exec_sql(
    conn: PGConn, sql: str, params: Optional[tuple] = None, silent: bool = False
):
    with conn.cursor() as cur:
        cur.execute(sql, params)
    if not silent:
        print("OK:", sql.splitlines()[0][:120])


def fetchall(conn: PGConn, sql: str, params: Optional[tuple] = None):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def mog(cur, template: str, value) -> str:
    """
    Use psycopg2 mogrify to safely embed SQL literals.
    """
    return cur.mogrify(template, (value,)).decode("utf-8")


def get_view_columns(conn, view_name):
    sql = """
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name=%s
      ORDER BY ordinal_position
    """
    rows = fetchall(conn, sql, (view_name,))
    return [r["column_name"] for r in rows]


def needs_drop_recreate(
    existing_cols: list[str], desired_cols: list[str], cardinality: str
) -> bool:
    # keys present depending on cardinality
    keys = ["pcr_uuid_context"] + (["instance_id"] if cardinality == "many" else [])
    e = [c for c in existing_cols if c not in keys]
    d = desired_cols[:]  # already excludes keys
    return e != d  # any name/order difference requires drop & recreate


# ---------------------------
# Initialization
# ---------------------------
INIT_SQL = r"""
-- 1) Classifier for sections (adjust patterns as needed)
CREATE OR REPLACE FUNCTION classify_section(elemnum text, tag text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN COALESCE(elemnum, tag) ~* '^dAgency'            THEN 'agency'          -- or 'dagency' if you want distinct
    WHEN COALESCE(elemnum, tag) ~* '^eCrew'              THEN 'crew'
    WHEN COALESCE(elemnum, tag) ~* '^eDevice'            THEN 'device'
    WHEN COALESCE(elemnum, tag) ~* '^eLabs'              THEN 'labs'
    WHEN COALESCE(elemnum, tag) ~* '^eNarrative'         THEN 'narrative'
    WHEN COALESCE(elemnum, tag) ~* '^eProtocols'         THEN 'protocols'
    WHEN COALESCE(elemnum, tag) ~* '^Header'             THEN 'header'
    WHEN COALESCE(elemnum, tag) ~* '^PatientCareReport'  THEN 'pcr'             -- or 'record' if you prefer
    WHEN COALESCE(elemnum, tag) ~* '^EMSDataSet'         THEN 'emsdataset'
    WHEN COALESCE(elemnum, tag) ~* '^eCustomConfiguration' THEN 'custom'
    WHEN COALESCE(elemnum, tag) ~* '^eCustomResults'     THEN 'custom'
    WHEN COALESCE(elemnum, tag) ~* '^eAgency'  THEN 'agency'
    WHEN COALESCE(elemnum, tag) ~* '^eAirway'  THEN 'airway'    
    WHEN COALESCE(elemnum, tag) ~* '^eArrest'      THEN 'arrest'
    WHEN COALESCE(elemnum, tag) ~* '^eCustom'      THEN 'custom'
    WHEN COALESCE(elemnum, tag) ~* '^eDispatch'    THEN 'dispatch'
    WHEN COALESCE(elemnum, tag) ~* '^eDisposition' THEN 'disposition'
    WHEN COALESCE(elemnum, tag) ~* '^eExam'        THEN 'exam'
    WHEN COALESCE(elemnum, tag) ~* '^eHistory'     THEN 'history'
    WHEN COALESCE(elemnum, tag) ~* '^eInjury'      THEN 'injury'
    WHEN COALESCE(elemnum, tag) ~* '^eMedications' THEN 'medications'
    WHEN COALESCE(elemnum, tag) ~* '^eOther'       THEN 'other'
    WHEN COALESCE(elemnum, tag) ~* '^ePatient'     THEN 'patient'
    WHEN COALESCE(elemnum, tag) ~* '^eOutcome'     THEN 'outcome'
    WHEN COALESCE(elemnum, tag) ~* '^ePayment'     THEN 'payment'
    WHEN COALESCE(elemnum, tag) ~* '^eProcedures'  THEN 'procedures'
    WHEN COALESCE(elemnum, tag) ~* '^eRecord'      THEN 'record'
    WHEN COALESCE(elemnum, tag) ~* '^eResponse'    THEN 'response'
    WHEN COALESCE(elemnum, tag) ~* '^eScene'       THEN 'scene'
    WHEN COALESCE(elemnum, tag) ~* '^eSituation'   THEN 'situation'
    WHEN COALESCE(elemnum, tag) ~* '^eTimes'       THEN 'times'
    WHEN COALESCE(elemnum, tag) ~* '^eVitals'      THEN 'vitals'
    ELSE 'other'
  END;
$$;

-- 2) Metadata tables
CREATE TABLE IF NOT EXISTS view_registry (
  view_name          text PRIMARY KEY,
  cardinality        text NOT NULL CHECK (cardinality IN ('one','many')),
  section            text,
  where_sql          text,
  use_resolved       boolean NOT NULL DEFAULT true,
  group_key_expr     text DEFAULT 'COALESCE(parent_element_id, element_id)',
  description        text
);

CREATE TABLE IF NOT EXISTS view_columns (
  view_name          text REFERENCES view_registry(view_name) ON DELETE CASCADE,
  elementnumber      text NOT NULL,
  alias              text,
  value_kind         text  DEFAULT 'inherit',   -- inherit|resolved|raw
  agg_fn             text  DEFAULT 'MAX',       -- MAX|MIN|STRING_AGG_DISTINCT
  position           int   DEFAULT 1000,
  PRIMARY KEY(view_name, elementnumber)
);

CREATE TABLE IF NOT EXISTS view_excludes (
  view_name          text REFERENCES view_registry(view_name) ON DELETE CASCADE,
  elementnumber      text NOT NULL,
  PRIMARY KEY(view_name, elementnumber)
);
"""


def build_v_elements_long(conn: PGConn):
    """
    Create or replace v_elements_long by UNION-ing all dynamic tables
    that contain the required columns.
    """
    rows = fetchall(
        conn,
        """
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE t.table_schema = 'public'
          AND t.table_type   = 'BASE TABLE'
          AND t.table_name NOT IN ('schemaversions','xmlfilesprocessed','fielddefinitions','elementdefinitions')
          AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema='public' AND c.table_name=t.table_name AND c.column_name='pcr_uuid_context')
          AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema='public' AND c.table_name=t.table_name AND c.column_name='element_id')
          AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema='public' AND c.table_name=t.table_name AND c.column_name='original_tag_name')
          AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_schema='public' AND c.table_name=t.table_name AND c.column_name='text_content')
        ORDER BY t.table_name
        """,
    )
    if not rows:
        raise RuntimeError("No dynamic tables found to build v_elements_long.")

    parts = []
    for r in rows:
        tn = r["table_name"]
        parts.append(
            f"SELECT {psql_literal(tn)}::text AS source_table, element_id, parent_element_id, pcr_uuid_context, original_tag_name, text_content FROM {psql_ident(tn)}"
        )
    sql = (
        "CREATE OR REPLACE VIEW v_elements_long AS\n"
        + "\nUNION ALL\n".join(parts)
        + ";"
    )
    exec_sql(conn, sql)


def build_v_elements_with_section(conn: PGConn):
    sql = r"""
    CREATE OR REPLACE VIEW v_elements_with_section AS
    WITH labels AS (
      SELECT DISTINCT
        COALESCE(e.elementnumber, e.xmlname) AS elementnumber,
        NULLIF(e.elementname, '')           AS xsd_elementname
      FROM XSD_Elements e
    )
    SELECT
      e.pcr_uuid_context,
      e.element_id,
      e.parent_element_id,
      e.original_tag_name,
      -- Keep the element “number” when available, else the raw tag
      COALESCE(fd.elementnumber, e.original_tag_name)                          AS elementnumber,
      -- Prefer rich XSD label, else FieldDefinitions, else the raw tag
      COALESCE(l.xsd_elementname, NULLIF(fd.elementname,''), e.original_tag_name) AS elementname,
      classify_section(fd.elementnumber, e.original_tag_name)                  AS section,
      e.text_content
    FROM v_elements_long e
    LEFT JOIN fielddefinitions fd
      ON fd.elementnumber = e.original_tag_name
    LEFT JOIN labels l
      ON l.elementnumber = e.original_tag_name;
    """
    exec_sql(conn, sql)


def build_v_elements_resolved(conn: PGConn):
    sql = r"""
    CREATE OR REPLACE VIEW v_elements_resolved AS
    WITH src AS (
      SELECT
        v.pcr_uuid_context,
        v.element_id,
        v.parent_element_id,
        v.original_tag_name,
        v.elementnumber,
        v.elementname,
        v.section,
        v.text_content
      FROM v_elements_with_section v
    ),
    -- Map each elementnumber to the value-set (simpleType) actually used for enumeration
    map AS (
      SELECT DISTINCT
        COALESCE(e.elementnumber, e.xmlname) AS elementnumber,
        ev.typename                           AS typename
      FROM XSD_Elements e
      JOIN XSD_ElementValueSet ev ON ev.elementid = e.id
    )
    SELECT
      s.*,
      CASE
        WHEN m.typename IS NOT NULL THEN
          (
            SELECT string_agg(
                     DISTINCT COALESCE(en.codedescription, tok),
                     ' | ' ORDER BY COALESCE(en.codedescription, tok)
                   )
            FROM regexp_split_to_table(s.text_content, E'[\\s,;|]+') AS t(tok)
            LEFT JOIN XSD_Enumerations en
              ON en.typename = m.typename
             AND en.code     = btrim(t.tok)
          )
        ELSE s.text_content
      END AS resolved_value
    FROM src s
    LEFT JOIN map m
      ON m.elementnumber = s.elementnumber;
    """
    exec_sql(conn, sql)


def psql_ident(name: str) -> str:
    """
    Quote identifier safely for simple cases. Assumes no schema dotting.
    """
    return '"' + name.replace('"', '""') + '"'


def psql_literal(val: str) -> str:
    """
    Quote text literal safely (simple).
    """
    return "'" + val.replace("'", "''") + "'"


def init_all(conn: PGConn):
    exec_sql(conn, INIT_SQL, silent=True)
    build_v_elements_long(conn)
    build_v_elements_with_section(conn)
    build_v_elements_resolved(conn)
    conn.commit()
    print("Initialization completed.")


# ---------------------------
# Rebuild views from metadata
# ---------------------------
ALLOWED_AGG = {"MAX", "MIN", "STRING_AGG_DISTINCT"}


def build_view_sql(conn: PGConn, view_name: str) -> tuple[str, list[str]]:
    """
    Read view_registry + view_columns (+excludes) and generate the CREATE VIEW statement.
    Returns (sql, desired_column_aliases).
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT * FROM view_registry WHERE view_name=%s", (view_name,))
        r = cur.fetchone()
        if not r:
            raise RuntimeError(f"view_registry has no entry for {view_name}")

        cardinality = r["cardinality"]
        section = r["section"]
        where_sql = r["where_sql"]
        use_resolved = bool(r["use_resolved"])
        group_key_expr = (
            r["group_key_expr"] or "COALESCE(parent_element_id, element_id)"
        )
        value_default = "resolved_value" if use_resolved else "text_content"

        # ---------- explicit columns (curated) ----------
        cur.execute(
            """
            WITH fd_dedup AS (
            SELECT DISTINCT ON (elementnumber) elementnumber, elementname
            FROM fielddefinitions
            ORDER BY elementnumber
            ),
            labels AS (
            SELECT DISTINCT ON (COALESCE(e.elementnumber, e.xmlname))
                COALESCE(e.elementnumber, e.xmlname) AS elementnumber,
                NULLIF(e.elementname,'')             AS xsd_elementname
            FROM XSD_Elements e
            ORDER BY COALESCE(e.elementnumber, e.xmlname)
            )
            SELECT
            vc.elementnumber,
            COALESCE(
                NULLIF(vc.alias,''),
                l.xsd_elementname,
                NULLIF(fd.elementname,''),
                vc.elementnumber
            ) AS alias_src,
            COALESCE(NULLIF(vc.value_kind,''),'inherit') AS value_kind,
            UPPER(COALESCE(NULLIF(vc.agg_fn,''),'MAX'))  AS agg_fn,
            COALESCE(vc.position, 1000)                  AS position
            FROM view_columns vc
            LEFT JOIN fd_dedup fd ON fd.elementnumber = vc.elementnumber
            LEFT JOIN labels l    ON l.elementnumber  = vc.elementnumber
            WHERE vc.view_name = %s
            ORDER BY position, alias_src
            """,
            (view_name,),
        )
        cols = cur.fetchall()

        # ---------- fallback (no curated columns) ----------
        if not cols:
            if where_sql:
                filter_sql = f"({where_sql})"
            elif section:
                filter_sql = cur.mogrify("section = %s", (section,)).decode()
            else:
                filter_sql = "TRUE"

            cur.execute(
                f"""
                WITH fd_dedup AS (
                SELECT DISTINCT ON (elementnumber) elementnumber, elementname
                FROM fielddefinitions
                ORDER BY elementnumber
                ),
                labels AS (
                SELECT DISTINCT ON (COALESCE(e.elementnumber, e.xmlname))
                    COALESCE(e.elementnumber, e.xmlname) AS elementnumber,
                    NULLIF(e.elementname,'')             AS xsd_elementname
                FROM XSD_Elements e
                ORDER BY COALESCE(e.elementnumber, e.xmlname)
                )
                SELECT DISTINCT
                v.elementnumber,
                COALESCE(l.xsd_elementname, NULLIF(v.elementname,''), v.elementnumber) AS alias_src,
                'inherit' AS value_kind,
                'MAX'     AS agg_fn,
                1000      AS position
                FROM v_elements_resolved v
                LEFT JOIN labels l
                ON l.elementnumber = v.elementnumber
                LEFT JOIN fd_dedup fd
                ON fd.elementnumber = v.elementnumber
                WHERE {filter_sql}
                AND NOT EXISTS (
                        SELECT 1 FROM view_excludes x
                        WHERE x.view_name = %s
                        AND x.elementnumber = v.elementnumber
                )
                ORDER BY alias_src
                """,
                (view_name,),
            )

            cols = cur.fetchall()

        if not cols:
            return None, []  # type:ignore

        # ---------- build select list with safe, unique aliases ----------
        used_aliases: set[str] = set()
        desired_aliases: list[str] = []
        select_exprs: list[str] = []

        for c in cols:
            elemnum = c["elementnumber"]
            alias_src = c["alias_src"] or elemnum
            # sanitize and deduplicate alias
            base_alias = ident_sanitize_py(alias_src)
            alias = base_alias
            i = 2
            while alias in used_aliases:
                alias = f"{base_alias}_{i}"
                i += 1
            used_aliases.add(alias)
            desired_aliases.append(alias)

            # choose value column
            kind = (c["value_kind"] or "inherit").lower()
            value_col = (
                "resolved_value"
                if kind == "resolved"
                else "text_content" if kind == "raw" else value_default
            )

            # aggregator
            agg_fn = (c["agg_fn"] or "MAX").upper()
            if agg_fn not in ALLOWED_AGG:
                raise RuntimeError(
                    f"Unsupported agg_fn {agg_fn} for {view_name}.{elemnum}"
                )

            elem_lit = mog(cur, "%s", elemnum)

            if agg_fn == "STRING_AGG_DISTINCT":
                expr = (
                    f"string_agg("
                    f"  DISTINCT CASE WHEN elementnumber={elem_lit} THEN {value_col} END,"
                    f"  ' | ' "
                    f"  ORDER BY CASE WHEN elementnumber={elem_lit} THEN {value_col} END"
                    f") AS {psql_ident(alias)}"
                )
            else:
                expr = (
                    f"{agg_fn}(CASE WHEN elementnumber={elem_lit} THEN {value_col} END) "
                    f"AS {psql_ident(alias)}"
                )

            select_exprs.append(expr)

        # ---------- row filter ----------
        if where_sql:
            filter_sql = f"({where_sql})"  # admin-supplied; keep as-is
        elif section:
            filter_sql = cur.mogrify("section = %s", (section,)).decode()
        else:
            filter_sql = "TRUE"

        # ---------- final SQL ----------
        if cardinality == "one":
            sql = (
                f"CREATE OR REPLACE VIEW {psql_ident(view_name)} AS\n"
                f"SELECT pcr_uuid_context,\n  " + ",\n  ".join(select_exprs) + "\n"
                f"FROM v_elements_resolved\n"
                f"WHERE {filter_sql}\n"
                f"GROUP BY pcr_uuid_context\n"
                f"ORDER BY pcr_uuid_context;\n"
            )
        elif cardinality == "many":
            sql = (
                f"CREATE OR REPLACE VIEW {psql_ident(view_name)} AS\n"
                f"WITH s AS (\n"
                f"  SELECT pcr_uuid_context, ({group_key_expr}) AS instance_id, elementnumber, resolved_value, text_content, section\n"
                f"  FROM v_elements_resolved\n"
                f"  WHERE {filter_sql}\n"
                f")\n"
                f"SELECT pcr_uuid_context, instance_id,\n  "
                + ",\n  ".join(select_exprs)
                + "\n"
                f"FROM s\n"
                f"GROUP BY pcr_uuid_context, instance_id\n"
                f"ORDER BY pcr_uuid_context, instance_id;\n"
            )
        else:
            raise RuntimeError(
                f"Invalid cardinality '{cardinality}' for view {view_name} (expected 'one' or 'many')."
            )

        return sql, desired_aliases


def rebuild(conn, only=None):
    views = fetchall(conn, "SELECT view_name FROM view_registry ORDER BY view_name")
    if only:
        want = set(only)
        views = [v for v in views if v["view_name"] in want]

    for v in views:
        view_name = v["view_name"]
        # Gather registry row (for cardinality) and existing cols
        reg = fetchall(
            conn,
            "SELECT cardinality FROM view_registry WHERE view_name=%s",
            (view_name,),
        )
        if not reg:
            print(f"Skipping {view_name}: not in registry.")
            continue
        cardinality = reg[0]["cardinality"]

        existing_cols = get_view_columns(conn, view_name)
        sql_and_cols = build_view_sql(conn, view_name)
        if not sql_and_cols or not sql_and_cols[0]:
            print(f"Skipping {view_name}: no columns resolved.")
            continue
        sql, desired_cols = sql_and_cols

        if not existing_cols:
            exec_sql(conn, sql)
            continue

        if needs_drop_recreate(existing_cols, desired_cols, cardinality):
            try:
                exec_sql(conn, f"DROP VIEW {psql_ident(view_name)};", silent=True)
            except Exception as e:
                print(
                    f"ERROR: Could not drop {view_name}; dependent objects exist.\n{e}"
                )
                print(
                    "Tip: rebuild dependents first, or drop with CASCADE if acceptable."
                )
                raise
            exec_sql(conn, sql)
            continue

        to_remove = [
            c
            for c in existing_cols
            if c not in (["pcr_uuid_context", "instance_id"] + desired_cols)
        ]
        if to_remove:
            placeholders = ",\n  ".join(
                f"NULL::text AS {psql_ident(c)}" for c in to_remove
            )
            if "SELECT pcr_uuid_context, instance_id," in sql:
                sql = sql.replace(
                    "SELECT pcr_uuid_context, instance_id,",
                    "SELECT pcr_uuid_context, instance_id,\n  "
                    + placeholders
                    + ",\n  ",
                )
            else:
                sql = sql.replace(
                    "SELECT pcr_uuid_context,",
                    "SELECT pcr_uuid_context,\n  " + placeholders + ",\n  ",
                )

        exec_sql(conn, sql)

        # Try dropping placeholders (ignored if referenced)
        for col in to_remove:
            try:
                exec_sql(
                    conn,
                    f"ALTER VIEW {psql_ident(view_name)} DROP COLUMN {psql_ident(col)};",
                    silent=True,
                )
            except Exception as e:
                print(f"Note: could not drop column {view_name}.{col}: {e}")

    conn.commit()
    print("Rebuild complete.")


# ---------------------------
# Admin ops
# ---------------------------
def add_view(
    conn: PGConn,
    view_name: str,
    cardinality: str,
    section: Optional[str],
    where_sql: Optional[str],
    use_resolved: bool,
    group_key_expr: Optional[str],
    description: Optional[str],
):
    exec_sql(
        conn,
        """
        INSERT INTO view_registry(view_name, cardinality, section, where_sql, use_resolved, group_key_expr, description)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (view_name) DO UPDATE
        SET cardinality=EXCLUDED.cardinality,
            section=EXCLUDED.section,
            where_sql=EXCLUDED.where_sql,
            use_resolved=EXCLUDED.use_resolved,
            group_key_expr=EXCLUDED.group_key_expr,
            description=EXCLUDED.description
    """,
        (
            view_name,
            cardinality,
            section,
            where_sql,
            use_resolved,
            group_key_expr,
            description,
        ),
    )
    conn.commit()
    print(f"View '{view_name}' registered.")


def add_col(
    conn: PGConn,
    view_name: str,
    elementnumber: str,
    alias: Optional[str],
    value_kind: Optional[str],
    agg_fn: Optional[str],
    position: Optional[int],
):
    agg_fn = (agg_fn or "MAX").upper()
    if agg_fn not in ALLOWED_AGG:
        raise SystemExit(f"agg_fn must be one of {sorted(ALLOWED_AGG)}")
    exec_sql(
        conn,
        """
        INSERT INTO view_columns(view_name, elementnumber, alias, value_kind, agg_fn, position)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (view_name, elementnumber) DO UPDATE
        SET alias=EXCLUDED.alias,
            value_kind=EXCLUDED.value_kind,
            agg_fn=EXCLUDED.agg_fn,
            position=EXCLUDED.position
    """,
        (view_name, elementnumber, alias, value_kind, agg_fn, position),
    )
    conn.commit()
    print(f"Column {elementnumber} -> {view_name} added/updated.")


def add_exclude(conn: PGConn, view_name: str, elementnumber: str):
    exec_sql(
        conn,
        """
        INSERT INTO view_excludes(view_name, elementnumber)
        VALUES (%s,%s)
        ON CONFLICT DO NOTHING
    """,
        (view_name, elementnumber),
    )
    conn.commit()
    print(f"Excluded {elementnumber} from {view_name}.")


def delete_view(conn: PGConn, view_name: str, drop_object: bool):
    # remove metadata
    exec_sql(
        conn, "DELETE FROM view_excludes WHERE view_name=%s", (view_name,), silent=True
    )
    exec_sql(
        conn, "DELETE FROM view_columns  WHERE view_name=%s", (view_name,), silent=True
    )
    exec_sql(
        conn, "DELETE FROM view_registry WHERE view_name=%s", (view_name,), silent=True
    )
    if drop_object:
        exec_sql(
            conn, f"DROP VIEW IF EXISTS {psql_ident(view_name)} CASCADE;", silent=True
        )
    conn.commit()
    print(f"Deleted metadata for {view_name}.")


def list_views(conn: PGConn):
    rows = fetchall(
        conn,
        """
        SELECT vr.view_name, vr.cardinality, COALESCE(vr.section,'-') AS section,
               CASE WHEN vr.use_resolved THEN 'resolved' ELSE 'raw' END AS mode,
               COALESCE(vr.group_key_expr,'-') AS group_key_expr,
               COALESCE(vr.description,'') AS description
        FROM view_registry vr ORDER BY vr.view_name
    """,
    )
    if not rows:
        print("(no views registered)")
        return
    w = max(len(r["view_name"]) for r in rows) + 2
    for r in rows:
        print(
            f"{r['view_name']:<{w}}  {r['cardinality']:<4}  sec={r['section']:<10}  mode={r['mode']:<8}  group={r['group_key_expr']}  {r['description']}"
        )


# ---------------------------
# CLI
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="EMS/NEMSIS dynamic view builder")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser(
        "init",
        help="Create helper funcs, long/section/resolved views, and metadata tables",
    )

    rp = sub.add_parser(
        "rebuild", help="Rebuild all registered views (or only the named ones)"
    )
    rp.add_argument("--only", nargs="*", help="Subset of view names to rebuild")

    lv = sub.add_parser("list-views", help="List registered views")

    av = sub.add_parser("add-view", help="Add or update a view_registry row")
    av.add_argument("view_name")
    av.add_argument("--cardinality", required=True, choices=["one", "many"])
    av.add_argument("--section", help="Section name (patient, vitals, etc.)")
    av.add_argument("--where-sql", help="Custom SQL filter instead of section")
    av.add_argument(
        "--use-resolved", type=int, default=1, help="1=decode enums; 0=raw codes"
    )
    av.add_argument(
        "--group-key-expr",
        help="Only for many; default COALESCE(parent_element_id, element_id)",
    )
    av.add_argument("--description", help="Free text")

    ac = sub.add_parser("add-col", help="Add or update a column mapping in a view")
    ac.add_argument("view_name")
    ac.add_argument("elementnumber", help="e.g. ePatient.01")
    ac.add_argument("--alias")
    ac.add_argument(
        "--value-kind", choices=["inherit", "resolved", "raw"], default="inherit"
    )
    ac.add_argument("--agg", default="MAX", help="MAX | MIN | STRING_AGG_DISTINCT")
    ac.add_argument("--position", type=int, default=1000)

    ex = sub.add_parser(
        "exclude", help="Exclude an elementnumber from a view's fallback set"
    )
    ex.add_argument("view_name")
    ex.add_argument("elementnumber")

    dv = sub.add_parser(
        "delete-view",
        help="Delete a view from registry (and optionally drop the DB view)",
    )
    dv.add_argument("view_name")
    dv.add_argument("--drop-object", action="store_true")

    args = ap.parse_args()

    conn = get_conn()
    try:
        if args.cmd == "init":
            init_all(conn)
        elif args.cmd == "rebuild":
            rebuild(conn, args.only)
        elif args.cmd == "list-views":
            list_views(conn)
        elif args.cmd == "add-view":
            add_view(
                conn,
                args.view_name,
                args.cardinality,
                args.section,
                args.where_sql,
                bool(args.use_resolved),
                args.group_key_expr,
                args.description,
            )
        elif args.cmd == "add-col":
            add_col(
                conn,
                args.view_name,
                args.elementnumber,
                args.alias,
                args.value_kind,
                args.agg,
                args.position,
            )
        elif args.cmd == "exclude":
            add_exclude(conn, args.view_name, args.elementnumber)
        elif args.cmd == "delete-view":
            delete_view(conn, args.view_name, args.drop_object)
        else:
            ap.print_help()
    except Exception as e:
        conn.rollback()
        print("ERROR:", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

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
"""

import argparse
import hashlib
import os
import re
import sys
from typing import Optional, List

import duckdb


# ---------------------------
# DB connection
# ---------------------------
def get_conn() -> duckdb.DuckDBPyConnection:
    from database_setup import get_db_connection
    return get_db_connection()


# ---------------------------
# Helpers
# ---------------------------
IDENT_RE = re.compile(r"[^a-zA-Z0-9]+")


def ident_sanitize_py(src: str) -> str:
    s = IDENT_RE.sub("_", src or "").strip("_").lower()
    if not s:
        s = "col_" + hashlib.md5((src or "").encode("utf-8")).hexdigest()[:8]
    if s[0].isdigit():
        s = "x_" + s
    return s


def exec_sql(conn, sql: str, params=None, silent: bool = False):
    conn.execute(sql, params or [])
    if not silent:
        print("OK:", sql.splitlines()[0][:120])


def fetchall(conn, sql: str, params=None):
    result = conn.execute(sql, params or [])
    cols = [d[0] for d in result.description]
    return [dict(zip(cols, row)) for row in result.fetchall()]


def sql_literal(value) -> str:
    """Safely embed a string as a SQL literal."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def get_view_columns(conn, view_name):
    rows = fetchall(
        conn,
        "SELECT column_name FROM information_schema.columns WHERE table_name=? ORDER BY ordinal_position",
        (view_name,),
    )
    return [r["column_name"] for r in rows]


def needs_drop_recreate(existing_cols: list, desired_cols: list, cardinality: str) -> bool:
    keys = ["pcr_uuid_context"] + (["instance_id"] if cardinality == "many" else [])
    e = [c for c in existing_cols if c not in keys]
    d = desired_cols[:]
    return e != d


# ---------------------------
# Initialization
# ---------------------------
INIT_SQL = """
-- 1) Section classifier macro
CREATE OR REPLACE MACRO classify_section(elemnum, tag) AS (
  CASE
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^dagency')              THEN 'agency'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^ecrew')               THEN 'crew'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^edevice')             THEN 'device'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^elabs')               THEN 'labs'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^enarrative')          THEN 'narrative'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^eprotocols')          THEN 'protocols'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^header')              THEN 'header'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^patientcarereport')   THEN 'pcr'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^emsdataset')          THEN 'emsdataset'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^ecustomconfiguration') THEN 'custom'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^ecustomresults')      THEN 'custom'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^eagency')             THEN 'agency'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^eairway')             THEN 'airway'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^earrest')             THEN 'arrest'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^ecustom')             THEN 'custom'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^edispatch')           THEN 'dispatch'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^edisposition')        THEN 'disposition'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^eexam')               THEN 'exam'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^ehistory')            THEN 'history'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^einjury')             THEN 'injury'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^emedications')        THEN 'medications'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^eother')              THEN 'other'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^epatient')            THEN 'patient'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^eoutcome')            THEN 'outcome'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^epayment')            THEN 'payment'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^eprocedures')         THEN 'procedures'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^erecord')             THEN 'record'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^eresponse')           THEN 'response'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^escene')              THEN 'scene'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^esituation')          THEN 'situation'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^etimes')              THEN 'times'
    WHEN regexp_matches(LOWER(COALESCE(elemnum, tag)), '^evitals')             THEN 'vitals'
    ELSE 'other'
  END
);

-- 2) Metadata tables
CREATE TABLE IF NOT EXISTS view_registry (
  view_name          TEXT PRIMARY KEY,
  cardinality        TEXT NOT NULL CHECK (cardinality IN ('one','many')),
  section            TEXT,
  where_sql          TEXT,
  use_resolved       BOOLEAN NOT NULL DEFAULT TRUE,
  group_key_expr     TEXT DEFAULT 'COALESCE(parent_element_id, element_id)',
  description        TEXT
);

CREATE TABLE IF NOT EXISTS view_columns (
  view_name          TEXT REFERENCES view_registry(view_name),
  elementnumber      TEXT NOT NULL,
  alias              TEXT,
  value_kind         TEXT DEFAULT 'inherit',
  agg_fn             TEXT DEFAULT 'MAX',
  position           INTEGER DEFAULT 1000,
  PRIMARY KEY(view_name, elementnumber)
);

CREATE TABLE IF NOT EXISTS view_excludes (
  view_name          TEXT REFERENCES view_registry(view_name),
  elementnumber      TEXT NOT NULL,
  PRIMARY KEY(view_name, elementnumber)
);
"""


def build_v_elements_long(conn):
    rows = fetchall(
        conn,
        """
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE t.table_type = 'BASE TABLE'
          AND t.table_name NOT IN (
              'schemaversions','xmlfilesprocessed','fielddefinitions','elementdefinitions',
              'xsd_elements','xsd_simpletypes','xsd_enumerations','xsd_elementattributes',
              'xsd_elementvalueset','view_registry','view_columns','view_excludes','gnis_places'
          )
          AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_name=t.table_name AND c.column_name='pcr_uuid_context')
          AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_name=t.table_name AND c.column_name='element_id')
          AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_name=t.table_name AND c.column_name='original_tag_name')
          AND EXISTS (SELECT 1 FROM information_schema.columns c WHERE c.table_name=t.table_name AND c.column_name='text_content')
        ORDER BY t.table_name
        """,
    )
    if not rows:
        raise RuntimeError("No dynamic tables found to build v_elements_long.")

    parts = []
    for r in rows:
        tn = r["table_name"]
        parts.append(
            f"SELECT {sql_literal(tn)}::TEXT AS source_table, element_id, parent_element_id, pcr_uuid_context, original_tag_name, text_content FROM {psql_ident(tn)}"
        )
    sql = (
        "CREATE OR REPLACE VIEW v_elements_long AS\n"
        + "\nUNION ALL\n".join(parts)
        + ";"
    )
    exec_sql(conn, sql)


def build_v_elements_with_section(conn):
    sql = """
    CREATE OR REPLACE VIEW v_elements_with_section AS
    WITH labels AS (
      SELECT
        COALESCE(e.elementnumber, e.xmlname) AS elementnumber,
        ANY_VALUE(NULLIF(e.elementname, ''))  AS xsd_elementname
      FROM XSD_Elements e
      GROUP BY COALESCE(e.elementnumber, e.xmlname)
    )
    SELECT
      e.pcr_uuid_context,
      e.element_id,
      e.parent_element_id,
      e.original_tag_name,
      COALESCE(fd.elementnumber, e.original_tag_name)                               AS elementnumber,
      COALESCE(l.xsd_elementname, NULLIF(fd.elementname,''), e.original_tag_name)   AS elementname,
      classify_section(fd.elementnumber, e.original_tag_name)                        AS section,
      e.text_content
    FROM v_elements_long e
    LEFT JOIN fielddefinitions fd ON fd.elementnumber = e.original_tag_name
    LEFT JOIN labels l            ON l.elementnumber  = e.original_tag_name;
    """
    exec_sql(conn, sql)


def build_v_elements_resolved(conn):
    sql = """
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
    map AS (
      SELECT
        COALESCE(e.elementnumber, e.xmlname) AS elementnumber,
        ANY_VALUE(ev.typename)               AS typename
      FROM XSD_Elements e
      JOIN XSD_ElementValueSet ev ON ev.elementid = e.id
      GROUP BY COALESCE(e.elementnumber, e.xmlname)
    )
    SELECT
      s.*,
      CASE
        WHEN m.typename IS NOT NULL THEN
          COALESCE(
            (SELECT en.codedescription
             FROM XSD_Enumerations en
             WHERE en.typename = m.typename
               AND en.code = trim(s.text_content)
             LIMIT 1),
            s.text_content
          )
        ELSE s.text_content
      END AS resolved_value
    FROM src s
    LEFT JOIN map m ON m.elementnumber = s.elementnumber;
    """
    exec_sql(conn, sql)


def psql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def psql_literal(val: str) -> str:
    return "'" + val.replace("'", "''") + "'"


def init_all(conn):
    exec_sql(conn, INIT_SQL, silent=True)
    build_v_elements_long(conn)
    build_v_elements_with_section(conn)
    build_v_elements_resolved(conn)
    print("Initialization completed.")


# ---------------------------
# Rebuild views from metadata
# ---------------------------
ALLOWED_AGG = {"MAX", "MIN", "STRING_AGG_DISTINCT"}


def build_view_sql(conn, view_name: str) -> tuple:
    r = fetchall(conn, "SELECT * FROM view_registry WHERE view_name=?", (view_name,))
    if not r:
        raise RuntimeError(f"view_registry has no entry for {view_name}")
    r = r[0]

    cardinality   = r["cardinality"]
    section       = r["section"]
    where_sql     = r["where_sql"]
    use_resolved  = bool(r["use_resolved"])
    group_key_expr = r["group_key_expr"] or "COALESCE(parent_element_id, element_id)"
    value_default  = "resolved_value" if use_resolved else "text_content"

    # ---------- explicit columns (curated) ----------
    cols = fetchall(
        conn,
        """
        WITH fd_dedup AS (
          SELECT elementnumber, ANY_VALUE(elementname) AS elementname
          FROM fielddefinitions GROUP BY elementnumber
        ),
        labels AS (
          SELECT
            COALESCE(e.elementnumber, e.xmlname)   AS elementnumber,
            ANY_VALUE(NULLIF(e.elementname,''))     AS xsd_elementname
          FROM XSD_Elements e
          GROUP BY COALESCE(e.elementnumber, e.xmlname)
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
        WHERE vc.view_name = ?
        ORDER BY position, alias_src
        """,
        (view_name,),
    )

    # ---------- fallback (no curated columns) ----------
    if not cols:
        if where_sql:
            filter_sql = f"({where_sql})"
        elif section:
            filter_sql = f"section = {sql_literal(section)}"
        else:
            filter_sql = "TRUE"

        cols = fetchall(
            conn,
            f"""
            WITH fd_dedup AS (
              SELECT elementnumber, ANY_VALUE(elementname) AS elementname
              FROM fielddefinitions GROUP BY elementnumber
            ),
            labels AS (
              SELECT
                COALESCE(e.elementnumber, e.xmlname)   AS elementnumber,
                ANY_VALUE(NULLIF(e.elementname,''))     AS xsd_elementname
              FROM XSD_Elements e
              GROUP BY COALESCE(e.elementnumber, e.xmlname)
            )
            SELECT DISTINCT
              v.elementnumber,
              COALESCE(l.xsd_elementname, NULLIF(v.elementname,''), v.elementnumber) AS alias_src,
              'inherit' AS value_kind,
              'MAX'     AS agg_fn,
              1000      AS position
            FROM v_elements_resolved v
            LEFT JOIN labels   l  ON l.elementnumber  = v.elementnumber
            LEFT JOIN fd_dedup fd ON fd.elementnumber = v.elementnumber
            WHERE {filter_sql}
              AND NOT EXISTS (
                SELECT 1 FROM view_excludes x
                WHERE x.view_name = ? AND x.elementnumber = v.elementnumber
              )
            ORDER BY alias_src
            """,
            (view_name,),
        )

    if not cols:
        return None, []

    # ---------- build select list with safe, unique aliases ----------
    used_aliases: set = set()
    desired_aliases: list = []
    select_exprs: list = []

    for c in cols:
        elemnum   = c["elementnumber"]
        alias_src = c["alias_src"] or elemnum
        base_alias = ident_sanitize_py(alias_src)
        alias = base_alias
        i = 2
        while alias in used_aliases:
            alias = f"{base_alias}_{i}"
            i += 1
        used_aliases.add(alias)
        desired_aliases.append(alias)

        kind = (c["value_kind"] or "inherit").lower()
        value_col = (
            "resolved_value" if kind == "resolved"
            else "text_content" if kind == "raw"
            else value_default
        )

        agg_fn = (c["agg_fn"] or "MAX").upper()
        if agg_fn not in ALLOWED_AGG:
            raise RuntimeError(f"Unsupported agg_fn {agg_fn} for {view_name}.{elemnum}")

        elem_lit = sql_literal(elemnum)

        if agg_fn == "STRING_AGG_DISTINCT":
            expr = (
                f"string_agg("
                f"  DISTINCT CASE WHEN elementnumber={elem_lit} THEN {value_col} END,"
                f"  ' | '"
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
        filter_sql = f"({where_sql})"
    elif section:
        filter_sql = f"section = {sql_literal(section)}"
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
        raise RuntimeError(f"Invalid cardinality '{cardinality}' for view {view_name}.")

    return sql, desired_aliases


def rebuild(conn, only=None):
    views = fetchall(conn, "SELECT view_name FROM view_registry ORDER BY view_name")
    if only:
        want = set(only)
        views = [v for v in views if v["view_name"] in want]

    for v in views:
        view_name = v["view_name"]
        reg = fetchall(conn, "SELECT cardinality FROM view_registry WHERE view_name=?", (view_name,))
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

        if not existing_cols or needs_drop_recreate(existing_cols, desired_cols, cardinality):
            try:
                exec_sql(conn, f"DROP VIEW IF EXISTS {psql_ident(view_name)};", silent=True)
            except Exception as e:
                print(f"ERROR: Could not drop {view_name}: {e}")
                raise
            exec_sql(conn, sql)
        else:
            exec_sql(conn, sql)

    print("Rebuild complete.")


# ---------------------------
# Admin ops
# ---------------------------
def add_view(conn, view_name, cardinality, section, where_sql, use_resolved, group_key_expr, description):
    exec_sql(
        conn,
        """
        INSERT INTO view_registry(view_name, cardinality, section, where_sql, use_resolved, group_key_expr, description)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT (view_name) DO UPDATE
        SET cardinality=EXCLUDED.cardinality,
            section=EXCLUDED.section,
            where_sql=EXCLUDED.where_sql,
            use_resolved=EXCLUDED.use_resolved,
            group_key_expr=EXCLUDED.group_key_expr,
            description=EXCLUDED.description
        """,
        (view_name, cardinality, section, where_sql, use_resolved, group_key_expr, description),
    )
    print(f"View '{view_name}' registered.")


def add_col(conn, view_name, elementnumber, alias, value_kind, agg_fn, position):
    agg_fn = (agg_fn or "MAX").upper()
    if agg_fn not in ALLOWED_AGG:
        raise SystemExit(f"agg_fn must be one of {sorted(ALLOWED_AGG)}")
    exec_sql(
        conn,
        """
        INSERT INTO view_columns(view_name, elementnumber, alias, value_kind, agg_fn, position)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT (view_name, elementnumber) DO UPDATE
        SET alias=EXCLUDED.alias,
            value_kind=EXCLUDED.value_kind,
            agg_fn=EXCLUDED.agg_fn,
            position=EXCLUDED.position
        """,
        (view_name, elementnumber, alias, value_kind, agg_fn, position),
    )
    print(f"Column {elementnumber} -> {view_name} added/updated.")


def add_exclude(conn, view_name, elementnumber):
    exec_sql(
        conn,
        "INSERT INTO view_excludes(view_name, elementnumber) VALUES (?,?) ON CONFLICT DO NOTHING",
        (view_name, elementnumber),
    )
    print(f"Excluded {elementnumber} from {view_name}.")


def delete_view(conn, view_name, drop_object):
    exec_sql(conn, "DELETE FROM view_excludes WHERE view_name=?", (view_name,), silent=True)
    exec_sql(conn, "DELETE FROM view_columns  WHERE view_name=?", (view_name,), silent=True)
    exec_sql(conn, "DELETE FROM view_registry WHERE view_name=?", (view_name,), silent=True)
    if drop_object:
        exec_sql(conn, f"DROP VIEW IF EXISTS {psql_ident(view_name)};", silent=True)
    print(f"Deleted metadata for {view_name}.")


def list_views(conn):
    rows = fetchall(
        conn,
        """
        SELECT vr.view_name, vr.cardinality,
               COALESCE(vr.section,'-') AS section,
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
        print(f"{r['view_name']:<{w}}  {r['cardinality']:<4}  sec={r['section']:<10}  mode={r['mode']:<8}  {r['description']}")


# ---------------------------
# CLI
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="EMS/NEMSIS dynamic view builder")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="Create helper macros, foundation views, and metadata tables")

    rp = sub.add_parser("rebuild", help="Rebuild all registered views (or only the named ones)")
    rp.add_argument("--only", nargs="*", help="Subset of view names to rebuild")

    sub.add_parser("list-views", help="List registered views")

    av = sub.add_parser("add-view", help="Add or update a view_registry row")
    av.add_argument("view_name")
    av.add_argument("--cardinality", required=True, choices=["one", "many"])
    av.add_argument("--section", help="Section name (patient, vitals, etc.)")
    av.add_argument("--where-sql", help="Custom SQL filter instead of section")
    av.add_argument("--use-resolved", type=int, default=1, help="1=decode enums; 0=raw codes")
    av.add_argument("--group-key-expr", help="Only for many; default COALESCE(parent_element_id, element_id)")
    av.add_argument("--description", help="Free text")

    ac = sub.add_parser("add-col", help="Add or update a column mapping in a view")
    ac.add_argument("view_name")
    ac.add_argument("elementnumber", help="e.g. ePatient.01")
    ac.add_argument("--alias")
    ac.add_argument("--value-kind", choices=["inherit", "resolved", "raw"], default="inherit")
    ac.add_argument("--agg", default="MAX", help="MAX | MIN | STRING_AGG_DISTINCT")
    ac.add_argument("--position", type=int, default=1000)

    ex = sub.add_parser("exclude", help="Exclude an elementnumber from a view's fallback set")
    ex.add_argument("view_name")
    ex.add_argument("elementnumber")

    dv = sub.add_parser("delete-view", help="Delete a view from registry (and optionally drop the DB view)")
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
            add_view(conn, args.view_name, args.cardinality, args.section,
                     args.where_sql, bool(args.use_resolved), args.group_key_expr, args.description)
        elif args.cmd == "add-col":
            add_col(conn, args.view_name, args.elementnumber, args.alias,
                    args.value_kind, args.agg, args.position)
        elif args.cmd == "exclude":
            add_exclude(conn, args.view_name, args.elementnumber)
        elif args.cmd == "delete-view":
            delete_view(conn, args.view_name, args.drop_object)
        else:
            ap.print_help()
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

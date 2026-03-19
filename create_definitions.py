import re
import csv
import requests
from typing import Optional, Tuple, Dict, List
from xml.etree import ElementTree as ET
from database_setup import get_db_connection

# =========================
# CONFIG (NEMSIS v3.5.1)
# =========================
BASE_XSD_URL = "https://nemsis.org/media/nemsis_v3/release-3.5.1/XSDs/NEMSIS_XSDs"
EMS_DATASET_XSD_URL = f"{BASE_XSD_URL}/EMSDataSet_v3.xsd"
COMMON_TYPES_XSD_URL = f"{BASE_XSD_URL}/commonTypes_v3.xsd"

XS = {"xs": "http://www.w3.org/2001/XMLSchema"}


# =========================
# DB helpers
# =========================
def exec_sql(conn, sql, params=None, many=False):
    if many:
        conn.executemany(sql, params)
    else:
        conn.execute(sql, params or [])


def fetchone(conn, sql, params=None):
    return conn.execute(sql, params or []).fetchone()


def ensure_tables(conn):
    conn.execute("CREATE SEQUENCE IF NOT EXISTS xsd_elements_id_seq;")
    exec_sql(conn, """
    CREATE TABLE IF NOT EXISTS XSD_Elements (
      id            INTEGER PRIMARY KEY DEFAULT nextval('xsd_elements_id_seq'),
      DatasetName   TEXT NOT NULL,
      ElementNumber TEXT,
      ElementName   TEXT NOT NULL,
      XMLName       TEXT NOT NULL,
      TypeName      TEXT,
      GroupName     TEXT,
      Definition    TEXT,
      Usage         TEXT,
      v2Number      TEXT,
      National      BOOLEAN,
      State         BOOLEAN,
      MinOccurs     INTEGER,
      MaxOccurs     TEXT,
      Nillable      BOOLEAN DEFAULT FALSE,
      HasSimpleContent BOOLEAN DEFAULT FALSE,
      CreatedAt     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    exec_sql(conn, """
    CREATE TABLE IF NOT EXISTS XSD_ElementAttributes (
      ElementId     INTEGER REFERENCES XSD_Elements(id),
      AttributeName TEXT,
      AllowedValues TEXT,
      UNIQUE (ElementId, AttributeName)
    );
    """)
    exec_sql(conn, """
    CREATE TABLE IF NOT EXISTS XSD_SimpleTypes (
      TypeName      TEXT PRIMARY KEY,
      BaseType      TEXT,
      Documentation TEXT
    );
    """)
    exec_sql(conn, """
    CREATE TABLE IF NOT EXISTS XSD_Enumerations (
      TypeName        TEXT REFERENCES XSD_SimpleTypes(TypeName),
      Code            TEXT,
      CodeDescription TEXT,
      PRIMARY KEY (TypeName, Code)
    );
    """)
    exec_sql(conn, """
    CREATE TABLE IF NOT EXISTS XSD_ElementValueSet (
      ElementId INTEGER REFERENCES XSD_Elements(id),
      TypeName  TEXT REFERENCES XSD_SimpleTypes(TypeName),
      PRIMARY KEY (ElementId, TypeName)
    );
    """)


def clear_all_datasets(conn):
    conn.execute("DELETE FROM XSD_ElementAttributes;")
    conn.execute("DELETE FROM XSD_ElementValueSet;")
    conn.execute("DELETE FROM XSD_Elements;")
    # DuckDB sequences reset automatically when the table is empty
    # Types/enums are shared — truncate them too for a full reload:
    # conn.execute("DELETE FROM XSD_Enumerations;")
    # conn.execute("DELETE FROM XSD_SimpleTypes;")


# =========================
# XML utils
# =========================
def get_xml(url: str) -> ET.Element:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return ET.fromstring(r.content)


def text_or_none(node: Optional[ET.Element]) -> Optional[str]:
    if node is None:
        return None
    txt = "".join(node.itertext()).strip()
    return txt or None


def bool_from_text(t: Optional[str]) -> Optional[bool]:
    if t is None:
        return None
    return t.lower() in ("yes", "true")


def parse_minmax(elem: ET.Element) -> Tuple[Optional[int], Optional[str]]:
    mi = elem.get("minOccurs")
    ma = elem.get("maxOccurs")
    mino = (
        int(mi)
        if mi and mi.isdigit()
        else (0 if mi == "0" else (1 if mi == "1" else None))
    )
    return mino, (ma if ma is not None else None)


def element_number_from_xmlname(xmlname: str) -> Optional[str]:
    # Works across all datasets, e.g., eDispatch.03, eResponse.23, ePatient.01
    return xmlname if re.match(r"^e[A-Za-z]+?\.\d{2,}$", xmlname) else None


def extract_tacdoc_fields(doc_elem: Optional[ET.Element]) -> Dict[str, Optional[str]]:
    """
    xs:documentation usually embeds <nemsisTacDoc>...; best-effort parse.
    Returns Name, Definition, Usage, v2Number, National, State if present.
    """
    out = {
        "Name": None,
        "Definition": None,
        "Usage": None,
        "v2Number": None,
        "National": None,
        "State": None,
    }
    if doc_elem is None:
        return out  # type: ignore I think the type checker is wrong!
    raw = "".join(doc_elem.itertext()).strip()
    try:
        wrapper = ET.fromstring(
            f"<root>{ET.tostring(doc_elem, encoding='unicode')}</root>"
        )
        for e in wrapper.iter():
            tag = re.sub(r"^\{.*\}", "", e.tag).strip()
            val = text_or_none(e)
            tl = tag.lower()
            if tl in ("name", "elementname") and val:
                out["Name"] = val  # type: ignore
            elif tl in ("definition",):
                out["Definition"] = val  # type: ignore
            elif tl in ("usage",):
                out["Usage"] = val  # type: ignore
            elif tl in ("v2number", "v2 number"):
                out["v2Number"] = val  # type: ignore
            elif tl == "national":
                out["National"] = val  # type: ignore
            elif tl == "state":
                out["State"] = val  # type: ignore
    except Exception:
        if raw and not out["Definition"]:
            out["Definition"] = raw  # type: ignore
    return out  # type: ignore


def parse_attributes(attr_parent: ET.Element) -> Dict[str, Optional[str]]:
    """
    Return {attrName: allowedUnionMemberTypes or None}
    """
    out = {}
    for a in attr_parent.findall(".//xs:attribute", XS):
        name = a.get("name")
        if not name:
            continue
        union = a.find("xs:simpleType/xs:union", XS)
        if union is not None:
            out[name] = "|".join((union.get("memberTypes") or "").split())
        else:
            out[name] = None
    return out


# =========================
# Upserts
# =========================
def upsert_simple_types(conn, trees: List[ET.Element]):
    for tree in trees:
        for st in tree.findall("xs:simpleType", XS):
            tname = st.get("name")
            if not tname:
                continue
            restr = st.find("xs:restriction", XS)
            base = restr.get("base") if restr is not None else None
            doc = text_or_none(st.find("xs:annotation/xs:documentation", XS))
            exec_sql(
                conn,
                "INSERT INTO XSD_SimpleTypes (TypeName, BaseType, Documentation) VALUES (?,?,?) "
                "ON CONFLICT (TypeName) DO UPDATE SET BaseType=EXCLUDED.BaseType, Documentation=EXCLUDED.Documentation",
                (tname, base, doc),
            )
            if restr is not None:
                for enum in restr.findall("xs:enumeration", XS):
                    code = enum.get("value")
                    label = text_or_none(
                        enum.find("xs:annotation/xs:documentation", XS)
                    )
                    if code:
                        exec_sql(
                            conn,
                            "INSERT INTO XSD_Enumerations (TypeName, Code, CodeDescription) VALUES (?,?,?) "
                            "ON CONFLICT (TypeName, Code) DO UPDATE SET CodeDescription=EXCLUDED.CodeDescription",
                            (tname, code, label),
                        )


def insert_element(conn, payload: Dict) -> int:
    row = fetchone(
        conn,
        """
        INSERT INTO XSD_Elements
          (DatasetName, ElementNumber, ElementName, XMLName, TypeName, GroupName,
           Definition, Usage, v2Number, National, State,
           MinOccurs, MaxOccurs, Nillable, HasSimpleContent)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        RETURNING id;
        """,
        (
            payload["DatasetName"], payload["ElementNumber"], payload["ElementName"],
            payload["XMLName"], payload["TypeName"], payload["GroupName"],
            payload["Definition"], payload["Usage"], payload["v2Number"],
            payload["National"], payload["State"], payload["MinOccurs"],
            payload["MaxOccurs"], payload["Nillable"], payload["HasSimpleContent"],
        ),
    )
    return int(row[0])


def insert_attribute(conn, element_id: int, name: str, allowed: Optional[str]):
    exec_sql(
        conn,
        """
        INSERT INTO XSD_ElementAttributes (ElementId, AttributeName, AllowedValues)
        VALUES (?,?,?)
        ON CONFLICT (ElementId, AttributeName) DO UPDATE SET AllowedValues=EXCLUDED.AllowedValues
    """,
        (element_id, name, allowed),
    )


def map_element_valueset(conn, element_id: int, type_name: Optional[str]):
    if not type_name:
        return
    exec_sql(
        conn,
        """
        INSERT INTO XSD_ElementValueSet (ElementId, TypeName)
        VALUES (?,?)
        ON CONFLICT (ElementId, TypeName) DO NOTHING
    """,
        (element_id, type_name),
    )


# =========================
# Traversal
# =========================
def walk_elements_for_dataset(
    conn, dataset_name: str, root_seq: ET.Element, group_name: Optional[str] = None
):
    for el in root_seq.findall("xs:element", XS):
        xmlname = el.get("name")
        if not xmlname:
            continue
        has_simple = el.find("xs:complexType/xs:simpleContent", XS) is not None
        type_name = el.get("type")
        nillable = el.get("nillable") == "true"
        mino, maxo = parse_minmax(el)
        doc = el.find("xs:annotation/xs:documentation", XS)
        tac = extract_tacdoc_fields(doc)

        payload = {
            "DatasetName": dataset_name,
            "ElementNumber": element_number_from_xmlname(xmlname),
            "ElementName": (tac.get("Name") or xmlname),
            "XMLName": xmlname,
            "TypeName": type_name,
            "GroupName": group_name,
            "Definition": tac.get("Definition"),
            "Usage": tac.get("Usage"),
            "v2Number": tac.get("v2Number"),
            "National": bool_from_text(tac.get("National")),
            "State": bool_from_text(tac.get("State")),
            "MinOccurs": mino,
            "MaxOccurs": maxo,
            "Nillable": nillable,
            "HasSimpleContent": has_simple,
        }
        elem_id = insert_element(conn, payload)

        # Attributes attached to this element / its inline complexType
        attr_block = el.find("xs:complexType", XS) or el
        for aname, allowed in parse_attributes(attr_block).items():
            insert_attribute(conn, elem_id, aname, allowed)

        # Value set mapping
        if has_simple:
            base = attr_block.find("xs:simpleContent/xs:extension", XS)
            if base is not None and base.get("base"):
                map_element_valueset(conn, elem_id, base.get("base"))
            # attributes within extension
            for a in attr_block.findall(
                "xs:simpleContent/xs:extension/xs:attribute", XS
            ):
                aname = a.get("name")
                if aname:
                    union = a.find("xs:simpleType/xs:union", XS)
                    allowed = (
                        "|".join((union.get("memberTypes") or "").split())
                        if union is not None
                        else None
                    )
                    insert_attribute(conn, elem_id, aname, allowed)
        else:
            if type_name:
                map_element_valueset(conn, elem_id, type_name)

        # Recurse into inline complex groups
        inline_seq = el.find("xs:complexType/xs:sequence", XS)
        if inline_seq is not None:
            walk_elements_for_dataset(
                conn, dataset_name, inline_seq, group_name=xmlname
            )


def discover_module_schema_urls() -> List[Tuple[str, str]]:
    """
    Returns list of (dataset_name, absolute_schema_url) for all xs:include’d modules.
    """
    ems = get_xml(EMS_DATASET_XSD_URL)
    modules: List[Tuple[str, str]] = []
    for inc in ems.findall("xs:include", XS):
        href = inc.get("schemaLocation")
        if not href:
            continue
        # Normalize URL
        if not href.startswith("http"):
            url = f"{BASE_XSD_URL}/{href.strip()}"
        else:
            url = href.strip()
        # Derive dataset name from file name: eDispatch_v3.xsd -> eDispatch
        m = re.search(r"/([^/]+)_v3\.xsd$", url)
        ds = m.group(1) if m else url.rsplit("/", 1)[-1].replace("_v3.xsd", "")
        if ds != "commonTypes":  # modules; commonTypes handled separately
            modules.append((ds, url))
    return modules


def ingest_all_schemas(conn):
    ensure_tables(conn)
    clear_all_datasets(conn)

    # 1) Load common types (enums etc.)
    common = get_xml(COMMON_TYPES_XSD_URL)

    # 2) Discover all module schemas from EMSDataSet_v3.xsd
    modules = discover_module_schema_urls()
    trees = [common]

    # 3) Download module trees
    module_trees: Dict[str, ET.Element] = {}
    for ds, url in modules:
        t = get_xml(url)
        module_trees[ds] = t
        trees.append(t)

    # 4) Upsert all simpleTypes/enumerations from common + all modules
    upsert_simple_types(conn, trees)

    # 5) For each module, find its complexType (name == dataset) and walk
    for ds, tree in module_trees.items():
        ct = None
        for c in tree.findall("xs:complexType", XS):
            if c.get("name") == ds:
                ct = c
                break
        if ct is None:
            # Some modules may export only types; skip with note
            print(
                f"[WARN] Could not find complexType name='{ds}' in {ds}_v3.xsd; skipping elements."
            )
            continue
        seq = ct.find("xs:sequence", XS)
        if seq is None:
            print(f"[WARN] No xs:sequence under complexType '{ds}'; skipping.")
            continue
        walk_elements_for_dataset(conn, ds, seq)

    print("[XSD] Ingestion complete for all modules (3.5.1).")


# =========================
# (Optional) Legacy 3.5.1 loaders
# =========================
NEMSIS_ENUM_URL = "https://nemsis.org/media/nemsis_v3/release-3.5.1/DataDictionary/Ancillary/DEMEMS/Combined_ElementEnumerations.txt"
FIELD_DEF_URL = "https://nemsis.org/media/nemsis_v3/release-3.5.1/DataDictionary/Ancillary/DEMEMS/Combined_ElementAttributes.txt"


def create_legacy_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ElementDefinitions (
            DatasetName   TEXT,
            ElementNumber TEXT,
            ElementName   TEXT,
            Code          TEXT,
            CodeDescription TEXT
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS FieldDefinitions (
            Dataset       TEXT,
            DatasetType   TEXT,
            ElementNumber TEXT,
            ElementName   TEXT,
            Attribute     TEXT
        );
    """)


def populate_legacy_tables(conn):
    for url, tbl in [
        (NEMSIS_ENUM_URL, "ElementDefinitions"),
        (FIELD_DEF_URL, "FieldDefinitions"),
    ]:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        reader = csv.DictReader(r.text.splitlines(), delimiter="|")
        rows = [
            {k.strip().strip("'"): (v or "").strip().strip("'") for k, v in row.items()}
            for row in reader
        ]
        conn.execute(f"DELETE FROM {tbl};")
        if tbl == "ElementDefinitions":
            conn.executemany(
                "INSERT INTO ElementDefinitions (DatasetName, ElementNumber, ElementName, Code, CodeDescription) VALUES (?,?,?,?,?)",
                [(x.get("DatasetName",""), x.get("ElementNumber",""), x.get("ElementName",""), x.get("Code",""), x.get("CodeDescription","")) for x in rows],
            )
        else:
            conn.executemany(
                "INSERT INTO FieldDefinitions (Dataset, DatasetType, ElementNumber, ElementName, Attribute) VALUES (?,?,?,?,?)",
                [(x.get("Dataset",""), x.get("DatasetType",""), x.get("ElementNumber",""), x.get("ElementName",""), x.get("Attribute","")) for x in rows],
            )
        print(f"[Legacy] Inserted {len(rows)} into {tbl}")


# =========================
# Entrypoint
# =========================
def setup_definitions(conn):
    ingest_all_schemas(conn)  # 3.3.4 modules + commonTypes
    create_legacy_tables(conn)  # optional
    populate_legacy_tables(conn)  # optional
    conn.close()


if __name__ == "__main__":
    conn = get_db_connection()
    setup_definitions(conn)

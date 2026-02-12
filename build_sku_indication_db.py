"""
Build a standalone SKU-Indication database from BAG Preparations XML files.

Creates sku_indication.db with:
- sku: one row per GTIN with normalized pack attributes and validity period
- limitation_text: unique limitation texts with cashback detection
- sku_indication: links SKU → indication_code → text with temporal validity
- extract_info: reference table for XML extracts

Re-parses all XML files directly (independent from swiss_pharma_limitations.db).
"""

import hashlib
import html
import logging
import re
import sqlite3
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

import pandas as pd

from build_sku_normalized import (
    parse_pack_description, parse_substance_qty, COMPUTABLE_SUBSTANCE_UNITS,
)
from cashback_extractor import (
    detect_cashback, extract_cashback_sentence,
    extract_calculation, extract_unit, clean_html,
)

# ============================================================
# Configuration
# ============================================================

BASE_DIR = Path(r"c:\Users\micha\OneDrive\Matching_indication_code")
EXTRACTED_DIR = BASE_DIR / "extracted"
DB_PATH = BASE_DIR / "sku_indication.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ============================================================
# Regex patterns (from extract_limitations.py)
# ============================================================

RE_NUMERIC = re.compile(r"(\d{5}\.\d{2})\.?\b")

RE_BOLD = re.compile(r"<b>(.+?)</b>")

RE_HEADER_BOLD = re.compile(
    r"(?:^|<br\s*/?>[\s\n]*(?:<br\s*/?>[\s\n]*)*)"
    r"(<b>.+?</b>)",
    re.MULTILINE,
)

STRUCTURAL_BOLD_NAMES = {
    "UND", "ODER", "AND", "OR", "ET", "OU",
    "und", "oder", "and", "or", "et", "ou",
}

STRUCTURAL_PREFIXES = (
    "Vor Therapiebeginn",
    "Therapiefortführung", "Therapiefortsetzung",
    "Therapieabbruch",
    "nach AJCC",
    "Fr. ", "CHF ",
    "Maximal ",
    "Dosierungsschema",
    "Für alle vergütungspflichtigen",
    "Rückerstattungen",
    "Erwachsene",
    "Kriterien für die Vergütung",
)

TEXT_PATTERNS = [
    # German
    re.compile(r"Indikationscode[^:]{0,60}:\s*(\d{5}\.\d{2})", re.IGNORECASE),
    re.compile(r"Code[^:]{0,40}Krankenversicherer[^:]{0,40}:\s*(\d{5}\.\d{2})", re.IGNORECASE),
    re.compile(r"Code[^:]{0,60}bermitteln[^:]{0,20}:\s*(\d{5}\.\d{2})", re.IGNORECASE),
    # French
    re.compile(r"code\s+(?:d.indication\s+)?suivant[^:]{0,60}:\s*(\d{5}\.\d{2})", re.IGNORECASE),
    re.compile(r"code\s+correspondant[^:]{0,60}:\s*(\d{5}\.\d{2})", re.IGNORECASE),
    # Italian
    re.compile(r"codice[^:]{0,60}:\s*(\d{5}\.\d{2})", re.IGNORECASE),
    re.compile(r"All.assicuratore[^:]{0,60}:\s*(\d{5}\.\d{2})", re.IGNORECASE),
]

# ============================================================
# Text splitting functions (from extract_limitations.py)
# ============================================================

def _is_structural_name(name):
    """Return True if the bold name is a structural marker, not an indication."""
    if not name:
        return True
    stripped = name.strip().rstrip(":")
    if stripped in STRUCTURAL_BOLD_NAMES:
        return True
    if stripped.startswith(STRUCTURAL_PREFIXES):
        return True
    if stripped.replace(".", "").replace(",", "").isdigit():
        return True
    if len(stripped) <= 3 and stripped.islower():
        return True
    return False


def split_text_by_indication(text):
    """Split a limitation text at paragraph-level <b>Name</b> headers."""
    if not text:
        return []
    headers = list(RE_HEADER_BOLD.finditer(text))
    if not headers:
        return []
    segments = []
    for i, m in enumerate(headers):
        name = RE_BOLD.search(m.group(1)).group(1)
        seg_start = m.end()
        seg_end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        seg_text = text[seg_start:seg_end].strip()
        segments.append((name, seg_text))
    return segments


def split_limitation_texts(desc_de, desc_fr, desc_it):
    """Split all 3 language texts and align them by position."""
    segs_de = split_text_by_indication(desc_de)
    segs_fr = split_text_by_indication(desc_fr)
    segs_it = split_text_by_indication(desc_it)
    max_len = max(len(segs_de), len(segs_fr), len(segs_it), 0)
    if max_len == 0:
        return []
    result = []
    for i in range(max_len):
        result.append({
            "order": i,
            "name_de": segs_de[i][0] if i < len(segs_de) else None,
            "name_fr": segs_fr[i][0] if i < len(segs_fr) else None,
            "name_it": segs_it[i][0] if i < len(segs_it) else None,
            "text_de": segs_de[i][1] if i < len(segs_de) else None,
            "text_fr": segs_fr[i][1] if i < len(segs_fr) else None,
            "text_it": segs_it[i][1] if i < len(segs_it) else None,
        })
    return result


# ============================================================
# Database Schema
# ============================================================

SCHEMA_SQL = """
CREATE TABLE extract_info (
    extract_id      INTEGER PRIMARY KEY,
    file_name       TEXT NOT NULL,
    release_date    TEXT NOT NULL,
    file_year       INTEGER NOT NULL
);

CREATE TABLE sku (
    sku_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    gtin                TEXT NOT NULL UNIQUE,
    swissmedic_no8      TEXT,
    swissmedic_no5      TEXT NOT NULL,
    bag_dossier_no      TEXT,
    preparation_id      INTEGER,
    product_name        TEXT,
    atc_code            TEXT,
    org_gen_code        TEXT,
    description_de      TEXT,
    form_type           TEXT,
    form_type_raw       TEXT,
    container_count     INTEGER,
    unit_count          INTEGER,
    volume_per_unit     REAL,
    volume_unit         TEXT,
    total_volume        REAL,
    dose_count          INTEGER,
    total_units         INTEGER,
    substance_name      TEXT,
    substance_qty       REAL,
    substance_unit      TEXT,
    total_substance     REAL,
    public_price        REAL,
    exfactory_price     REAL,
    is_alt              INTEGER DEFAULT 0,
    annotation          TEXT,
    parse_confidence    TEXT,
    parse_pattern       TEXT,
    valid_from          TEXT,
    valid_to            TEXT,
    first_seen_extract  INTEGER,
    last_seen_extract   INTEGER
);

CREATE INDEX idx_sku_gtin ON sku(gtin);
CREATE INDEX idx_sku_sm5 ON sku(swissmedic_no5);
CREATE INDEX idx_sku_prep ON sku(preparation_id);
CREATE INDEX idx_sku_dossier ON sku(bag_dossier_no);

CREATE TABLE limitation_text (
    text_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    content_hash        TEXT NOT NULL UNIQUE,
    limitation_code     TEXT,
    limitation_type     TEXT,
    limitation_niveau   TEXT,
    description_de      TEXT,
    description_fr      TEXT,
    description_it      TEXT,
    is_cashback         INTEGER DEFAULT 0,
    cashback_company    TEXT,
    cashback_patterns   TEXT,
    cashback_calc_type  TEXT,
    cashback_calc_value REAL,
    cashback_unit       TEXT,
    valid_from          TEXT,
    valid_to            TEXT,
    first_seen_extract  INTEGER,
    last_seen_extract   INTEGER
);

CREATE INDEX idx_text_hash ON limitation_text(content_hash);

CREATE TABLE sku_indication (
    link_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    gtin                TEXT NOT NULL,
    indication_code     TEXT NOT NULL,
    text_id             INTEGER NOT NULL REFERENCES limitation_text(text_id),
    code_source         TEXT NOT NULL,
    limitation_level    TEXT NOT NULL,
    is_fallback         INTEGER NOT NULL DEFAULT 0,
    valid_from          TEXT,
    valid_to            TEXT,
    UNIQUE(gtin, indication_code, text_id)
);

CREATE INDEX idx_si_gtin ON sku_indication(gtin);
CREATE INDEX idx_si_code ON sku_indication(indication_code);
CREATE INDEX idx_si_text ON sku_indication(text_id);

CREATE TABLE text_segment (
    segment_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    text_id         INTEGER NOT NULL REFERENCES limitation_text(text_id),
    segment_order   INTEGER NOT NULL,
    indication_name_de TEXT,
    indication_name_fr TEXT,
    indication_name_it TEXT,
    segment_text_de TEXT,
    segment_text_fr TEXT,
    segment_text_it TEXT,
    is_cashback     INTEGER DEFAULT 0,
    cashback_company    TEXT,
    cashback_calc_type  TEXT,
    cashback_calc_value REAL,
    cashback_unit       TEXT,
    UNIQUE(text_id, segment_order)
);

CREATE INDEX idx_ts_text ON text_segment(text_id);

CREATE TABLE indication_code_name (
    map_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    indication_code     TEXT NOT NULL,
    indication_name_de  TEXT,
    indication_name_fr  TEXT,
    indication_name_it  TEXT,
    bag_dossier_no      TEXT,
    match_source        TEXT NOT NULL,
    match_confidence    REAL,
    source_text_id      INTEGER,
    source_segment_id   INTEGER,
    UNIQUE(indication_code, indication_name_de)
);

CREATE INDEX idx_icn_code ON indication_code_name(indication_code);
CREATE INDEX idx_icn_name_de ON indication_code_name(indication_name_de);
CREATE INDEX idx_icn_dossier ON indication_code_name(bag_dossier_no);

CREATE VIEW v_codes_without_names AS
SELECT si.indication_code, si.code_source, si.is_fallback,
       CASE WHEN si.is_fallback = 1 THEN 'FALLBACK'
            WHEN si.indication_code LIKE '%.XX' THEN 'CROSS_INDICATION'
            ELSE 'REAL_CODE' END AS code_type,
       COUNT(*) as n_skus
FROM sku_indication si
WHERE si.indication_code NOT IN (SELECT indication_code FROM indication_code_name)
GROUP BY si.indication_code, si.code_source, si.is_fallback;

CREATE VIEW v_codes_summary AS
SELECT
    CASE WHEN si.is_fallback = 1 THEN 'FALLBACK'
         WHEN si.indication_code LIKE '%.XX' THEN 'CROSS_INDICATION'
         ELSE 'REAL_CODE' END AS code_type,
    COUNT(*) as n_links,
    COUNT(DISTINCT si.indication_code) as n_distinct_codes,
    COUNT(DISTINCT si.gtin) as n_skus
FROM sku_indication si
GROUP BY code_type;

CREATE VIEW v_names_without_codes AS
SELECT ts.segment_id, ts.text_id, ts.indication_name_de, ts.indication_name_fr
FROM text_segment ts
WHERE ts.indication_name_de IS NOT NULL
AND ts.indication_name_de NOT IN (
    SELECT COALESCE(indication_name_de, '') FROM indication_code_name
);

-- Temporary table for preparation-level links (dropped after fan-out)
CREATE TABLE _prep_code_link (
    prep_link_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    preparation_id      INTEGER NOT NULL,
    text_id             INTEGER NOT NULL,
    indication_code     TEXT NOT NULL,
    code_source         TEXT NOT NULL,
    limitation_level    TEXT NOT NULL,
    is_fallback         INTEGER NOT NULL DEFAULT 0,
    first_seen_extract  INTEGER NOT NULL,
    last_seen_extract   INTEGER NOT NULL,
    UNIQUE(preparation_id, text_id, indication_code)
);
"""


# ============================================================
# XML utility functions (adapted from extract_limitations.py)
# ============================================================

def get_text(elem, tag):
    """Safely extract text from a child element."""
    child = elem.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return None


def compute_hash(desc_de, desc_fr, desc_it):
    """Hash limitation description texts for deduplication."""
    combined = f"{desc_de or ''}|{desc_fr or ''}|{desc_it or ''}"
    return hashlib.md5(combined.encode("utf-8")).hexdigest()


def get_price(pack_elem, price_path):
    """Extract a price value from a Pack element."""
    container = pack_elem.find(price_path)
    if container is None:
        return None
    price_text = get_text(container, "Price")
    if price_text:
        try:
            return float(price_text)
        except (ValueError, TypeError):
            pass
    return None


def extract_codes_structured(lim_elem):
    """Extract codes from structured XML elements (2023+)."""
    codes = []
    container = lim_elem.find("IndicationsCodes")
    if container is not None:
        for ic in container.findall("IndicationsCode"):
            code = (ic.get("Code") or "").strip()
            if code:
                codes.append(code)
    if not codes:
        container = lim_elem.find("PmIndications")
        if container is not None:
            for ic in container.findall("PmIndication"):
                code = (ic.get("Code") or "").strip()
                if code:
                    codes.append(code)
    return codes


def extract_codes_from_text(desc_de, desc_fr, desc_it):
    """Extract indication codes from free-text limitation descriptions."""
    codes = set()
    for text in [desc_de, desc_fr, desc_it]:
        if not text:
            continue
        decoded = html.unescape(text)
        for pattern in TEXT_PATTERNS:
            for match in pattern.finditer(decoded):
                raw = match.group(1).rstrip(".")
                if RE_NUMERIC.match(raw):
                    codes.add(raw)
    return list(codes)


def get_release_date(file_path):
    """Extract ReleaseDate from the root element."""
    for event, elem in ET.iterparse(str(file_path), events=("start",)):
        return elem.get("ReleaseDate", "")


def discover_files():
    """Find all Preparations-*.xml files across year directories."""
    files = []
    for year_dir in sorted(EXTRACTED_DIR.iterdir()):
        if year_dir.is_dir() and year_dir.name.isdigit():
            for xml_file in sorted(year_dir.glob("Preparations-*.xml")):
                files.append(xml_file)
    return files


# ============================================================
# Phase 0: Setup
# ============================================================

def setup_database():
    """Create sku_indication.db with the schema."""
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SCHEMA_SQL)
    log.info(f"Created database: {DB_PATH.name}")
    return conn


def build_extract_info(conn, xml_files):
    """Register all XML files and build extract_id → release_date map."""
    extract_map = {}  # extract_id → release_date
    for i, f in enumerate(xml_files, 1):
        release_date = get_release_date(f)
        year = int(f.parent.name)
        conn.execute(
            "INSERT INTO extract_info (extract_id, file_name, release_date, file_year) "
            "VALUES (?, ?, ?, ?)",
            (i, f.name, release_date, year),
        )
        extract_map[i] = release_date
    conn.commit()
    log.info(f"Registered {len(xml_files)} XML extracts")
    return extract_map


# ============================================================
# Phase 1: XML Ingestion
# ============================================================

# In-memory preparation_id counter (no preparation table in this DB)
_prep_counter = 0
_prep_map = {}  # swissmedic_no5 → preparation_id


def get_preparation_id(swissmedic_no5):
    """Get or create a preparation_id for a SwissmedicNo5."""
    global _prep_counter
    if swissmedic_no5 not in _prep_map:
        _prep_counter += 1
        _prep_map[swissmedic_no5] = _prep_counter
    return _prep_map[swissmedic_no5]


def upsert_sku(conn, extract_id, gtin, swissmedic_no8, swissmedic_no5,
               bag_dossier_no, preparation_id, product_name, atc_code,
               org_gen_code, description_de, substance_name,
               substance_qty_raw, substance_unit,
               public_price, exfactory_price):
    """Insert or update a SKU (pack) row."""
    existing = conn.execute(
        "SELECT sku_id FROM sku WHERE gtin = ?", (gtin,)
    ).fetchone()

    if existing:
        # Update last_seen and latest prices
        conn.execute(
            "UPDATE sku SET last_seen_extract = ?, "
            "public_price = COALESCE(?, public_price), "
            "exfactory_price = COALESCE(?, exfactory_price) "
            "WHERE sku_id = ?",
            (extract_id, public_price, exfactory_price, existing[0]),
        )
        return

    # Parse description
    parsed = parse_pack_description(description_de) if description_de else {}
    sub_qty = parse_substance_qty(substance_qty_raw)

    # Compute total_substance
    total_sub = None
    if (sub_qty is not None
            and parsed.get("total_units") is not None
            and substance_unit in COMPUTABLE_SUBSTANCE_UNITS):
        total_sub = sub_qty * parsed["total_units"]

    conn.execute(
        "INSERT INTO sku ("
        "  gtin, swissmedic_no8, swissmedic_no5, bag_dossier_no, preparation_id,"
        "  product_name, atc_code, org_gen_code, description_de,"
        "  form_type, form_type_raw, container_count, unit_count,"
        "  volume_per_unit, volume_unit, total_volume, dose_count, total_units,"
        "  substance_name, substance_qty, substance_unit, total_substance,"
        "  public_price, exfactory_price,"
        "  is_alt, annotation, parse_confidence, parse_pattern,"
        "  first_seen_extract, last_seen_extract"
        ") VALUES (?,?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?, ?,?,?,?, ?,?)",
        (
            gtin, swissmedic_no8, swissmedic_no5, bag_dossier_no, preparation_id,
            product_name, atc_code, org_gen_code, description_de,
            parsed.get("form_type"), parsed.get("form_type_raw"),
            parsed.get("container_count"), parsed.get("unit_count"),
            parsed.get("volume_per_unit"), parsed.get("volume_unit"),
            parsed.get("total_volume"), parsed.get("dose_count"),
            parsed.get("total_units"),
            substance_name, sub_qty, substance_unit, total_sub,
            public_price, exfactory_price,
            parsed.get("is_alt", 0), parsed.get("annotation"),
            parsed.get("parse_confidence"), parsed.get("parse_pattern"),
            extract_id, extract_id,
        ),
    )


def upsert_limitation_text(conn, extract_id, content_hash,
                           lim_code, lim_type, lim_niveau,
                           desc_de, desc_fr, desc_it):
    """Insert or update a unique limitation text. Returns text_id."""
    row = conn.execute(
        "SELECT text_id FROM limitation_text WHERE content_hash = ?",
        (content_hash,),
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE limitation_text SET last_seen_extract = ? WHERE text_id = ?",
            (extract_id, row[0]),
        )
        return row[0]
    cur = conn.execute(
        "INSERT INTO limitation_text "
        "(content_hash, limitation_code, limitation_type, limitation_niveau, "
        " description_de, description_fr, description_it, "
        " first_seen_extract, last_seen_extract) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (content_hash, lim_code, lim_type, lim_niveau,
         desc_de, desc_fr, desc_it, extract_id, extract_id),
    )
    return cur.lastrowid


def upsert_prep_code_link(conn, extract_id, preparation_id, text_id,
                          indication_code, code_source, level, is_fallback=0):
    """Insert or update a preparation-level code link."""
    row = conn.execute(
        "SELECT prep_link_id FROM _prep_code_link "
        "WHERE preparation_id = ? AND text_id = ? AND indication_code = ?",
        (preparation_id, text_id, indication_code),
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE _prep_code_link SET last_seen_extract = ? "
            "WHERE prep_link_id = ?",
            (extract_id, row[0]),
        )
    else:
        conn.execute(
            "INSERT INTO _prep_code_link "
            "(preparation_id, text_id, indication_code, code_source, "
            " limitation_level, is_fallback, first_seen_extract, last_seen_extract) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (preparation_id, text_id, indication_code, code_source,
             level, is_fallback, extract_id, extract_id),
        )


def process_limitation(conn, extract_id, preparation_id, lim_elem,
                       level, bag_dossier_no):
    """Process one <Limitation> element."""
    if level == "ITCODE":
        return

    lim_code = get_text(lim_elem, "LimitationCode")
    lim_type = get_text(lim_elem, "LimitationType")
    lim_niveau = get_text(lim_elem, "LimitationNiveau")
    desc_de = get_text(lim_elem, "DescriptionDe")
    desc_fr = get_text(lim_elem, "DescriptionFr")
    desc_it = get_text(lim_elem, "DescriptionIt")

    # Deduplicate text
    c_hash = compute_hash(desc_de, desc_fr, desc_it)
    text_id = upsert_limitation_text(
        conn, extract_id, c_hash,
        lim_code, lim_type, lim_niveau,
        desc_de, desc_fr, desc_it,
    )

    # Extract indication codes (3-layer strategy)
    codes = extract_codes_structured(lim_elem)
    source = "STRUCTURED_XML"

    if not codes:
        codes = extract_codes_from_text(desc_de, desc_fr, desc_it)
        source = "TEXT_PARSED"

    if not codes and lim_type == "DIA" and bag_dossier_no:
        codes = [f"{bag_dossier_no}.XX"]
        source = "FALLBACK_XX"

    is_fallback = 1 if source == "FALLBACK_XX" else 0
    for code_value in codes:
        link_code = code_value
        if source == "FALLBACK_XX":
            link_code = f"{bag_dossier_no}.XX"
        upsert_prep_code_link(
            conn, extract_id, preparation_id, text_id,
            link_code, source, level, is_fallback=is_fallback,
        )


def process_preparation(conn, extract_id, prep_elem):
    """Process one <Preparation> element."""
    swissmedic_no5 = get_text(prep_elem, "SwissmedicNo5")
    if not swissmedic_no5:
        return

    preparation_id = get_preparation_id(swissmedic_no5)

    name_de = get_text(prep_elem, "NameDe")
    atc_code = get_text(prep_elem, "AtcCode")
    org_gen_code = get_text(prep_elem, "OrgGenCode")

    # Substance (take first one)
    sub_elem = prep_elem.find(".//Substances/Substance")
    sub_name = get_text(sub_elem, "DescriptionLa") if sub_elem is not None else None
    sub_qty_raw = get_text(sub_elem, "Quantity") if sub_elem is not None else None
    sub_unit = get_text(sub_elem, "QuantityUnit") if sub_elem is not None else None

    # Collect bag_dossier_nos for fallback
    all_bag_dossier_nos = []

    # Process packs → SKU
    for pack_elem in prep_elem.findall(".//Packs/Pack"):
        gtin = get_text(pack_elem, "GTIN")
        if not gtin:
            continue
        swissmedic_no8 = get_text(pack_elem, "SwissmedicNo8")
        bag_dossier_no = get_text(pack_elem, "BagDossierNo")
        pack_desc_de = get_text(pack_elem, "DescriptionDe")
        pub_price = get_price(pack_elem, "Prices/PublicPrice")
        exf_price = get_price(pack_elem, "Prices/ExFactoryPrice")

        if bag_dossier_no:
            all_bag_dossier_nos.append(bag_dossier_no)

        upsert_sku(
            conn, extract_id, gtin, swissmedic_no8, swissmedic_no5,
            bag_dossier_no, preparation_id, name_de, atc_code,
            org_gen_code, pack_desc_de, sub_name, sub_qty_raw, sub_unit,
            pub_price, exf_price,
        )

        # Pack-level limitations (rare)
        lims = pack_elem.find("Limitations")
        if lims is not None:
            for lim_elem in lims.findall("Limitation"):
                process_limitation(
                    conn, extract_id, preparation_id, lim_elem,
                    "PACK", bag_dossier_no,
                )

    fallback_bag = all_bag_dossier_nos[0] if all_bag_dossier_nos else None

    # Preparation-level limitations
    prep_lims = prep_elem.find("Limitations")
    if prep_lims is not None:
        for lim_elem in prep_lims.findall("Limitation"):
            process_limitation(
                conn, extract_id, preparation_id, lim_elem,
                "PREPARATION", fallback_bag,
            )


def ingest_xml(conn, xml_files):
    """Phase 1: Parse all XML files and populate sku + limitation_text + _prep_code_link."""
    log.info("Phase 1: Ingesting XML files...")
    for i, f in enumerate(xml_files, 1):
        extract_id = i
        context = ET.iterparse(str(f), events=("end",))
        for event, elem in context:
            if elem.tag == "Preparation":
                process_preparation(conn, extract_id, elem)
                elem.clear()
        conn.commit()
        if i % 10 == 0 or i == len(xml_files):
            log.info(f"  Processed {i}/{len(xml_files)} files")

    sku_count = conn.execute("SELECT COUNT(*) FROM sku").fetchone()[0]
    text_count = conn.execute("SELECT COUNT(*) FROM limitation_text").fetchone()[0]
    link_count = conn.execute("SELECT COUNT(*) FROM _prep_code_link").fetchone()[0]
    log.info(f"  -> {sku_count} SKUs, {text_count} unique texts, {link_count} prep-level links")


# ============================================================
# Phase 2: Fan-out preparation → SKU
# ============================================================

def resolve_dates(conn, extract_map):
    """Resolve first/last_seen_extract to human-readable dates on sku and limitation_text."""
    log.info("Phase 2a: Resolving extract IDs to dates...")

    for table in ("sku", "limitation_text"):
        rows = conn.execute(
            f"SELECT rowid, first_seen_extract, last_seen_extract FROM {table}"
        ).fetchall()
        for rowid, first_ext, last_ext in rows:
            conn.execute(
                f"UPDATE {table} SET valid_from = ?, valid_to = ? WHERE rowid = ?",
                (extract_map.get(first_ext, ""), extract_map.get(last_ext, ""), rowid),
            )
    conn.commit()


def fanout_to_sku(conn, extract_map):
    """Phase 2: Expand preparation-level links to individual SKU links."""
    log.info("Phase 2b: Fan-out preparation links to SKU links...")

    # Pre-load all SKUs grouped by preparation_id
    sku_rows = conn.execute(
        "SELECT gtin, preparation_id, bag_dossier_no, "
        "       first_seen_extract, last_seen_extract "
        "FROM sku"
    ).fetchall()

    skus_by_prep = defaultdict(list)
    for gtin, prep_id, dossier, first_ext, last_ext in sku_rows:
        skus_by_prep[prep_id].append({
            "gtin": gtin,
            "bag_dossier_no": dossier,
            "first_ext": first_ext,
            "last_ext": last_ext,
        })

    # Process all prep-level links
    links = conn.execute(
        "SELECT preparation_id, text_id, indication_code, code_source, "
        "       limitation_level, is_fallback, first_seen_extract, last_seen_extract "
        "FROM _prep_code_link"
    ).fetchall()

    inserted = 0
    skipped = 0

    for prep_id, text_id, code, source, level, is_fb, link_first, link_last in links:
        matching_skus = skus_by_prep.get(prep_id, [])

        for sku in matching_skus:
            # Check temporal overlap
            if sku["last_ext"] < link_first or sku["first_ext"] > link_last:
                continue

            # For PACK-level: also filter by dossier
            if level == "PACK":
                code_dossier = code.split(".")[0] if "." in code else code
                if sku["bag_dossier_no"] != code_dossier:
                    continue

            # Compute effective validity (intersection)
            eff_first = max(sku["first_ext"], link_first)
            eff_last = min(sku["last_ext"], link_last)
            eff_from = extract_map.get(eff_first, "")
            eff_to = extract_map.get(eff_last, "")

            try:
                conn.execute(
                    "INSERT OR IGNORE INTO sku_indication "
                    "(gtin, indication_code, text_id, code_source, "
                    " limitation_level, is_fallback, valid_from, valid_to) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (sku["gtin"], code, text_id, source, level, is_fb, eff_from, eff_to),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                skipped += 1

    conn.commit()

    # Drop temporary table
    conn.execute("DROP TABLE IF EXISTS _prep_code_link")
    conn.commit()

    actual = conn.execute("SELECT COUNT(*) FROM sku_indication").fetchone()[0]
    log.info(f"  -> {actual} SKU-indication links created ({skipped} duplicates skipped)")


# ============================================================
# Phase 3: Cashback detection
# ============================================================

def detect_cashbacks(conn):
    """Phase 3: Run cashback detection on all unique limitation texts."""
    log.info("Phase 3: Detecting cashbacks on unique texts...")

    rows = conn.execute(
        "SELECT text_id, description_fr FROM limitation_text"
    ).fetchall()

    detected = 0
    for text_id, desc_fr in rows:
        if not desc_fr:
            continue

        cleaned = clean_html(html.unescape(desc_fr))
        result = detect_cashback(cleaned)

        if not result["is_cashback"]:
            continue

        detected += 1
        company = result.get("company")
        patterns = ",".join(result.get("patterns_matched", []))

        # Extract detailed cashback info
        calc_type = None
        calc_value = None
        cb_unit = None

        sentence_result = extract_cashback_sentence(cleaned)
        if sentence_result.get("has_cashback") and sentence_result.get("cashback_sentence"):
            sentence = sentence_result["cashback_sentence"]
            calc = extract_calculation(sentence)
            calc_type = calc.get("type")
            calc_value = calc.get("value")
            cb_unit = extract_unit(sentence)
            if not company and sentence_result.get("company"):
                company = sentence_result["company"]

        conn.execute(
            "UPDATE limitation_text SET "
            "is_cashback = 1, cashback_company = ?, cashback_patterns = ?, "
            "cashback_calc_type = ?, cashback_calc_value = ?, cashback_unit = ? "
            "WHERE text_id = ?",
            (company, patterns, calc_type, calc_value, cb_unit, text_id),
        )

    conn.commit()
    total = len(rows)
    log.info(f"  -> {detected}/{total} texts with cashback ({100*detected/total:.1f}%)")


# ============================================================
# Phase 3b: Text segmentation (multi-indication splitting)
# ============================================================

def segment_texts(conn):
    """Phase 3b: Split multi-indication texts into segments with per-segment cashback."""
    log.info("Phase 3b: Segmenting multi-indication texts...")

    rows = conn.execute(
        "SELECT text_id, description_de, description_fr, description_it "
        "FROM limitation_text"
    ).fetchall()

    texts_with_segments = 0
    total_segments = 0
    segments_cashback = 0
    structural_filtered = 0

    for text_id, desc_de, desc_fr, desc_it in rows:
        segments = split_limitation_texts(desc_de, desc_fr, desc_it)
        if not segments:
            continue

        # Filter structural names — keep only non-structural segments
        real_segments = []
        for seg in segments:
            # Check DE name first, then FR, then IT
            name = seg["name_de"] or seg["name_fr"] or seg["name_it"]
            if _is_structural_name(name):
                structural_filtered += 1
                continue
            real_segments.append(seg)

        if not real_segments:
            continue

        texts_with_segments += 1

        for seg in real_segments:
            total_segments += 1

            # Cashback detection on FR segment text
            is_cb = 0
            cb_company = None
            cb_calc_type = None
            cb_calc_value = None
            cb_unit = None

            seg_fr = seg["text_fr"]
            if seg_fr:
                cleaned = clean_html(html.unescape(seg_fr))
                result = detect_cashback(cleaned)
                if result["is_cashback"]:
                    is_cb = 1
                    segments_cashback += 1
                    cb_company = result.get("company")

                    sentence_result = extract_cashback_sentence(cleaned)
                    if sentence_result.get("has_cashback") and sentence_result.get("cashback_sentence"):
                        sentence = sentence_result["cashback_sentence"]
                        calc = extract_calculation(sentence)
                        cb_calc_type = calc.get("type")
                        cb_calc_value = calc.get("value")
                        cb_unit = extract_unit(sentence)
                        if not cb_company and sentence_result.get("company"):
                            cb_company = sentence_result["company"]

            conn.execute(
                "INSERT INTO text_segment "
                "(text_id, segment_order, "
                " indication_name_de, indication_name_fr, indication_name_it, "
                " segment_text_de, segment_text_fr, segment_text_it, "
                " is_cashback, cashback_company, cashback_calc_type, "
                " cashback_calc_value, cashback_unit) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (text_id, seg["order"],
                 seg["name_de"], seg["name_fr"], seg["name_it"],
                 seg["text_de"], seg["text_fr"], seg["text_it"],
                 is_cb, cb_company, cb_calc_type, cb_calc_value, cb_unit),
            )

    conn.commit()
    log.info(f"  -> {total_segments} segments from {texts_with_segments} texts "
             f"({structural_filtered} structural filtered)")
    log.info(f"  -> {segments_cashback} segments with cashback")


# ============================================================
# Phase 3c: Indication code ↔ name mapping
# ============================================================

# Brand canonical mappings (from extract_limitations.py)
BRAND_CANONICAL = {
    "LENALIDOMID SPIRIG HC": "LENALIDOMID", "LENALIDOMID SANDOZ": "LENALIDOMID",
    "LENALIDOMID-TEVA": "LENALIDOMID", "LENALIDOMID ZENTIVA": "LENALIDOMID",
    "LENALIDOMID VIATRIS": "LENALIDOMID", "LENALIDOMID DEVATIS": "LENALIDOMID",
    "LENALIDOMID ACCORD": "LENALIDOMID", "LENALIDOMID BMS": "LENALIDOMID",
    "LENALIDOMID Spirig": "LENALIDOMID", "LENALIDOMID Viatris": "LENALIDOMID",
    "LÉNALIDOMIDE DEVATIS": "LENALIDOMID", "Lenalidomid Spirig": "LENALIDOMID",
    "REVLIMID": "LENALIDOMID",
    "POMALIDOMID SPIRIG HC": "POMALIDOMID", "POMALIDOMID SANDOZ": "POMALIDOMID",
    "POMALIDOMID-TEVA": "POMALIDOMID", "POMALIDOMID ZENTIVA": "POMALIDOMID",
    "POMALIDOMID ACCORD": "POMALIDOMID", "IMNOVID": "POMALIDOMID",
    "AZACITIDIN SPIRIG HC": "AZACITIDIN", "AZACITIDIN ACCORD": "AZACITIDIN",
    "AZACITIDIN MYLAN": "AZACITIDIN", "AZACITIDIN SANDOZ": "AZACITIDIN",
    "AZACITIDIN STADA": "AZACITIDIN", "AZACITIDIN VIATRIS": "AZACITIDIN",
    "AZACITIDIN IDEOGEN": "AZACITIDIN", "VIDAZA": "AZACITIDIN",
    "DECITABIN ACCORD": "DECITABIN", "DECITABIN IDEOGEN": "DECITABIN",
    "DECITABIN SANDOZ": "DECITABIN",
    "OGIVRI": "TRASTUZUMAB", "TRAZIMERA": "TRASTUZUMAB", "KANJINTI": "TRASTUZUMAB",
    "HERZUMA": "TRASTUZUMAB", "HERCEPTIN": "TRASTUZUMAB", "ZERCEPAC": "TRASTUZUMAB",
    "OYAVAS": "BEVACIZUMAB", "ZIRABEV": "BEVACIZUMAB", "AVASTIN": "BEVACIZUMAB",
    "TRUXIMA": "RITUXIMAB", "RIXATHON": "RITUXIMAB",
    "DARZALEX SC": "DARZALEX", "Darzalex": "DARZALEX",
    "Kyprolis": "KYPROLIS",
    "Daratumumab": "DARZALEX", "Nivolumab": "OPDIVO", "Pembrolizumab": "KEYTRUDA",
    "Trastuzumab": "HERCEPTIN", "Bevacizumab": "AVASTIN", "Rituximab": "MABTHERA",
}
_BRAND_SORTED = sorted(BRAND_CANONICAL.items(), key=lambda x: -len(x[0]))


def _normalize_indication_name(name):
    """Normalize an indication name for comparison."""
    if not name:
        return ""
    return re.sub(r"\s+", " ", name.strip().rstrip(":").strip()).lower()


def _normalize_brands(name):
    """Replace brand-specific names with canonical generic form."""
    result = name
    for brand, canonical in _BRAND_SORTED:
        result = result.replace(brand, canonical)
    return result


def _insert_mapping(conn, code, name_de, name_fr, name_it, dossier,
                    source, confidence, text_id=None, seg_id=None):
    """Insert a code-name mapping, ignoring duplicates."""
    try:
        conn.execute(
            "INSERT OR IGNORE INTO indication_code_name "
            "(indication_code, indication_name_de, indication_name_fr, indication_name_it, "
            " bag_dossier_no, match_source, match_confidence, source_text_id, source_segment_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (code, name_de, name_fr, name_it, dossier, source, confidence, text_id, seg_id),
        )
        return conn.execute("SELECT changes()").fetchone()[0]
    except sqlite3.IntegrityError:
        return 0


def build_code_name_mapping(conn):
    """Phase 3c: Build the indication_code ↔ name mapping table."""
    log.info("Phase 3c: Building indication code ↔ name mapping...")
    stats = defaultdict(int)

    # ------------------------------------------------------------------
    # Layer 0: Import reference map from swiss_pharma_limitations.db
    # ------------------------------------------------------------------
    ref_db = BASE_DIR / "swiss_pharma_limitations.db"
    if ref_db.exists():
        conn.execute("ATTACH DATABASE ? AS ref", (str(ref_db),))
        cur = conn.execute("""
            INSERT OR IGNORE INTO indication_code_name
                (indication_code, indication_name_de, indication_name_fr, indication_name_it,
                 bag_dossier_no, match_source, match_confidence)
            SELECT code_value, indication_name_de, indication_name_fr, indication_name_it,
                   bag_dossier_no, 'REF_MAP', 1.0
            FROM ref.indication_name_code_map
        """)
        stats["REF_MAP"] = cur.rowcount
        conn.commit()
        conn.execute("DETACH DATABASE ref")
        log.info(f"  Layer 0 (REF_MAP): {stats['REF_MAP']} mappings imported")
    else:
        log.warning(f"  Layer 0: {ref_db.name} not found, skipping reference import")

    # ------------------------------------------------------------------
    # Layer 1: Direct 1:1 pairing (STRUCTURED_XML code + single segment)
    # ------------------------------------------------------------------
    rows = conn.execute("""
        SELECT si.indication_code, ts.indication_name_de, ts.indication_name_fr,
               ts.indication_name_it, ts.text_id, ts.segment_id
        FROM sku_indication si
        JOIN text_segment ts ON ts.text_id = si.text_id
        WHERE si.code_source = 'STRUCTURED_XML'
        AND si.text_id IN (
            SELECT text_id FROM sku_indication
            WHERE code_source = 'STRUCTURED_XML'
            GROUP BY text_id HAVING COUNT(DISTINCT indication_code) = 1
        )
        AND si.text_id IN (
            SELECT text_id FROM text_segment
            GROUP BY text_id HAVING COUNT(*) = 1
        )
        GROUP BY si.indication_code
    """).fetchall()
    for code, name_de, name_fr, name_it, text_id, seg_id in rows:
        dossier = code.split(".")[0] if "." in code else None
        stats["DIRECT_STRUCT"] += _insert_mapping(
            conn, code, name_de, name_fr, name_it, dossier,
            "DIRECT_STRUCT", 1.0, text_id, seg_id)
    conn.commit()
    log.info(f"  Layer 1 (DIRECT_STRUCT): {stats['DIRECT_STRUCT']} new")

    # ------------------------------------------------------------------
    # Layer 2: Direct 1:1 pairing (TEXT_PARSED code + single segment)
    # ------------------------------------------------------------------
    rows = conn.execute("""
        SELECT si.indication_code, ts.indication_name_de, ts.indication_name_fr,
               ts.indication_name_it, ts.text_id, ts.segment_id
        FROM sku_indication si
        JOIN text_segment ts ON ts.text_id = si.text_id
        WHERE si.code_source = 'TEXT_PARSED'
        AND si.text_id IN (
            SELECT text_id FROM sku_indication
            WHERE code_source IN ('TEXT_PARSED', 'STRUCTURED_XML')
            GROUP BY text_id HAVING COUNT(DISTINCT indication_code) = 1
        )
        AND si.text_id IN (
            SELECT text_id FROM text_segment
            GROUP BY text_id HAVING COUNT(*) = 1
        )
        GROUP BY si.indication_code
    """).fetchall()
    for code, name_de, name_fr, name_it, text_id, seg_id in rows:
        dossier = code.split(".")[0] if "." in code else None
        stats["DIRECT_TEXT"] += _insert_mapping(
            conn, code, name_de, name_fr, name_it, dossier,
            "DIRECT_TEXT", 0.95, text_id, seg_id)
    conn.commit()
    log.info(f"  Layer 2 (DIRECT_TEXT): {stats['DIRECT_TEXT']} new")

    # ------------------------------------------------------------------
    # Layer 3: Multi-code + single segment (biosimilar shared text)
    # ------------------------------------------------------------------
    rows = conn.execute("""
        SELECT si.indication_code, ts.indication_name_de, ts.indication_name_fr,
               ts.indication_name_it, ts.text_id, ts.segment_id
        FROM sku_indication si
        JOIN text_segment ts ON ts.text_id = si.text_id
        WHERE si.code_source IN ('STRUCTURED_XML', 'TEXT_PARSED')
        AND si.text_id IN (
            SELECT text_id FROM text_segment
            GROUP BY text_id HAVING COUNT(*) = 1
        )
        GROUP BY si.indication_code
    """).fetchall()
    for code, name_de, name_fr, name_it, text_id, seg_id in rows:
        dossier = code.split(".")[0] if "." in code else None
        stats["SHARED_TEXT"] += _insert_mapping(
            conn, code, name_de, name_fr, name_it, dossier,
            "SHARED_TEXT", 0.9, text_id, seg_id)
    conn.commit()
    log.info(f"  Layer 3 (SHARED_TEXT): {stats['SHARED_TEXT']} new")

    # ------------------------------------------------------------------
    # Layer 4: Positional matching (N codes = N segments, same dossier)
    # ------------------------------------------------------------------
    # Find text_ids where real code count == segment count
    text_code_counts = conn.execute("""
        SELECT si.text_id,
               GROUP_CONCAT(DISTINCT si.indication_code) as codes,
               COUNT(DISTINCT si.indication_code) as n_codes
        FROM sku_indication si
        WHERE si.code_source IN ('STRUCTURED_XML', 'TEXT_PARSED')
        GROUP BY si.text_id
        HAVING n_codes > 1
    """).fetchall()
    for text_id, codes_str, n_codes in text_code_counts:
        seg_count = conn.execute(
            "SELECT COUNT(*) FROM text_segment WHERE text_id = ?", (text_id,)
        ).fetchone()[0]
        if seg_count != n_codes:
            continue

        # Get codes sorted by their numeric suffix
        codes = sorted(codes_str.split(","), key=lambda c: c.split(".")[-1] if "." in c else c)
        # Check all codes are from the same dossier
        dossiers = set(c.split(".")[0] for c in codes if "." in c)
        if len(dossiers) != 1:
            continue
        dossier = dossiers.pop()

        # Get segments sorted by order
        segs = conn.execute(
            "SELECT segment_id, indication_name_de, indication_name_fr, indication_name_it "
            "FROM text_segment WHERE text_id = ? ORDER BY segment_order",
            (text_id,),
        ).fetchall()

        for i, (seg_id, name_de, name_fr, name_it) in enumerate(segs):
            if i < len(codes):
                stats["ORDINAL_POSITION"] += _insert_mapping(
                    conn, codes[i], name_de, name_fr, name_it, dossier,
                    "ORDINAL_POSITION", 0.8, text_id, seg_id)
    conn.commit()
    log.info(f"  Layer 4 (ORDINAL_POSITION): {stats['ORDINAL_POSITION']} new")

    # ------------------------------------------------------------------
    # Layer 5: Normalized matching of unmatched segment names vs existing map
    # ------------------------------------------------------------------
    # Build lookup from existing mappings
    existing_map = conn.execute(
        "SELECT indication_code, indication_name_de, bag_dossier_no "
        "FROM indication_code_name"
    ).fetchall()
    norm_lookup = {}  # normalized_name → [(code, dossier)]
    brand_lookup = {}  # brand_normalized_name → [(code, dossier)]
    for code, name_de, dossier in existing_map:
        if not name_de:
            continue
        norm = _normalize_indication_name(name_de)
        norm_lookup.setdefault(norm, []).append((code, dossier))
        brand_norm = _normalize_brands(_normalize_indication_name(name_de))
        brand_lookup.setdefault(brand_norm, []).append((code, dossier))

    # Get unmatched segments that have a code on their text_id
    unmatched = conn.execute("""
        SELECT ts.segment_id, ts.text_id, ts.indication_name_de,
               ts.indication_name_fr, ts.indication_name_it,
               si.indication_code
        FROM text_segment ts
        JOIN sku_indication si ON si.text_id = ts.text_id
        WHERE ts.indication_name_de IS NOT NULL
        AND ts.indication_name_de NOT IN (
            SELECT COALESCE(indication_name_de, '') FROM indication_code_name
        )
        GROUP BY ts.segment_id
    """).fetchall()

    for seg_id, text_id, name_de, name_fr, name_it, code in unmatched:
        dossier = code.split(".")[0] if "." in code else None
        norm_name = _normalize_indication_name(name_de)

        # 5a: Exact normalized match, same dossier
        if norm_name in norm_lookup:
            matches = [c for c, d in norm_lookup[norm_name] if d == dossier]
            if matches:
                stats["NORM_SAME"] += _insert_mapping(
                    conn, matches[0], name_de, name_fr, name_it, dossier,
                    "NORM_SAME", 0.95, text_id, seg_id)
                continue

        # 5b: Exact normalized match, cross-dossier (take indication_part only)
        if norm_name in norm_lookup:
            ref_code, ref_dossier = norm_lookup[norm_name][0]
            if "." in ref_code and dossier:
                ind_part = ref_code.split(".")[1]
                new_code = f"{dossier}.{ind_part}"
                stats["NORM_CROSS"] += _insert_mapping(
                    conn, new_code, name_de, name_fr, name_it, dossier,
                    "NORM_CROSS", 0.9, text_id, seg_id)
                continue

        # 5c: Brand-normalized match
        brand_norm = _normalize_brands(norm_name)
        if brand_norm != norm_name and brand_norm in brand_lookup:
            matches = [c for c, d in brand_lookup[brand_norm] if d == dossier]
            if matches:
                stats["BRAND_SAME"] += _insert_mapping(
                    conn, matches[0], name_de, name_fr, name_it, dossier,
                    "BRAND_SAME", 0.9, text_id, seg_id)
                continue
            # Cross-dossier brand
            ref_code, ref_dossier = brand_lookup[brand_norm][0]
            if "." in ref_code and dossier:
                ind_part = ref_code.split(".")[1]
                new_code = f"{dossier}.{ind_part}"
                stats["BRAND_CROSS"] += _insert_mapping(
                    conn, new_code, name_de, name_fr, name_it, dossier,
                    "BRAND_CROSS", 0.85, text_id, seg_id)

    conn.commit()
    for src in ("NORM_SAME", "NORM_CROSS", "BRAND_SAME", "BRAND_CROSS"):
        if stats[src]:
            log.info(f"  Layer 5 ({src}): {stats[src]} new")

    # ------------------------------------------------------------------
    # Layer 6: LLM segment names for FALLBACK_XX codes
    # ------------------------------------------------------------------
    # Check if text_segment_llm table exists (created by llm_segment_texts.py)
    has_llm = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='text_segment_llm'"
    ).fetchone()[0]
    if has_llm:
        rows = conn.execute("""
            SELECT si.indication_code, tsl.indication_name_fr,
                   tsl.text_id, tsl.segment_id
            FROM sku_indication si
            JOIN text_segment_llm tsl ON tsl.text_id = si.text_id
            WHERE si.code_source = 'FALLBACK_XX'
            AND tsl.indication_name_fr IS NOT NULL
            AND si.text_id IN (
                SELECT text_id FROM sku_indication
                WHERE code_source = 'FALLBACK_XX'
                GROUP BY text_id HAVING COUNT(DISTINCT indication_code) = 1
            )
            AND si.text_id IN (
                SELECT text_id FROM text_segment_llm
                WHERE indication_name_fr IS NOT NULL
                GROUP BY text_id HAVING COUNT(*) = 1
            )
            GROUP BY si.indication_code
        """).fetchall()
        for code, name_fr, text_id, seg_id in rows:
            dossier = code.split(".")[0] if "." in code else None
            stats["LLM_SEGMENT"] += _insert_mapping(
                conn, code, None, name_fr, None, dossier,
                "LLM_SEGMENT", 0.85, text_id, seg_id)
        conn.commit()
        log.info(f"  Layer 6 (LLM_SEGMENT): {stats['LLM_SEGMENT']} new")
    else:
        log.info("  Layer 6: text_segment_llm not found, skipping")

    # ------------------------------------------------------------------
    # Layer 7: Bold extraction / description fallback for remaining codes
    # ------------------------------------------------------------------
    unmapped_codes = conn.execute("""
        SELECT DISTINCT si.indication_code, si.text_id
        FROM sku_indication si
        WHERE si.indication_code NOT IN (SELECT indication_code FROM indication_code_name)
        AND si.code_source IN ('STRUCTURED_XML', 'TEXT_PARSED')
    """).fetchall()

    for code, text_id in unmapped_codes:
        dossier = code.split(".")[0] if "." in code else None
        desc_de = conn.execute(
            "SELECT description_de FROM limitation_text WHERE text_id = ?", (text_id,)
        ).fetchone()
        if not desc_de or not desc_de[0]:
            continue
        text = html.unescape(desc_de[0])

        # Try first bold name
        bold_match = RE_BOLD.search(text)
        if bold_match:
            bold_name = bold_match.group(1)
            if not _is_structural_name(bold_name):
                # Also get FR
                desc_fr = conn.execute(
                    "SELECT description_fr FROM limitation_text WHERE text_id = ?", (text_id,)
                ).fetchone()
                name_fr = None
                if desc_fr and desc_fr[0]:
                    fr_bold = RE_BOLD.search(html.unescape(desc_fr[0]))
                    if fr_bold:
                        name_fr = fr_bold.group(1)
                stats["BOLD_EXTRACT"] += _insert_mapping(
                    conn, code, bold_name, name_fr, None, dossier,
                    "BOLD_EXTRACT", 0.7, text_id)
                continue

        # Fallback: first sentence of cleaned text
        cleaned = clean_html(text)
        # Take first sentence (up to first period, or 120 chars)
        end = cleaned.find(".")
        if end > 0 and end < 120:
            truncated = cleaned[:end + 1].strip()
        else:
            truncated = cleaned[:120].strip()
            if len(cleaned) > 120:
                truncated += "..."
        if truncated:
            stats["DESC_TRUNCATED"] += _insert_mapping(
                conn, code, truncated, None, None, dossier,
                "DESC_TRUNCATED", 0.5, text_id)

    conn.commit()
    log.info(f"  Layer 7 (BOLD_EXTRACT): {stats['BOLD_EXTRACT']} new")
    log.info(f"  Layer 7 (DESC_TRUNCATED): {stats['DESC_TRUNCATED']} new")

    # Summary
    total = conn.execute("SELECT COUNT(*) FROM indication_code_name").fetchone()[0]
    distinct_codes = conn.execute(
        "SELECT COUNT(DISTINCT indication_code) FROM indication_code_name"
    ).fetchone()[0]
    all_codes = conn.execute(
        "SELECT COUNT(DISTINCT indication_code) FROM sku_indication"
    ).fetchone()[0]
    log.info(f"  => Total mappings: {total}, distinct codes: {distinct_codes}/{all_codes}")


# ============================================================
# Phase 4: Statistics and Export
# ============================================================

def print_stats(conn):
    """Print summary statistics."""
    log.info("=" * 60)
    log.info("SUMMARY STATISTICS")
    log.info("=" * 60)

    # Table counts
    for table in ("extract_info", "sku", "limitation_text", "sku_indication", "text_segment"):
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info(f"  {table}: {cnt} rows")

    # SKU parse confidence
    log.info("")
    log.info("SKU parse confidence:")
    for conf in ("HIGH", "MEDIUM", "LOW"):
        cnt = conn.execute(
            "SELECT COUNT(*) FROM sku WHERE parse_confidence = ?", (conf,)
        ).fetchone()[0]
        log.info(f"  {conf}: {cnt}")

    # Cashback stats
    log.info("")
    cb_total = conn.execute(
        "SELECT COUNT(*) FROM limitation_text WHERE is_cashback = 1"
    ).fetchone()[0]
    log.info(f"Cashback texts: {cb_total}")

    cb_with_calc = conn.execute(
        "SELECT COUNT(*) FROM limitation_text "
        "WHERE is_cashback = 1 AND cashback_calc_type IS NOT NULL"
    ).fetchone()[0]
    log.info(f"  with calc_type: {cb_with_calc}")

    cb_with_company = conn.execute(
        "SELECT COUNT(*) FROM limitation_text "
        "WHERE is_cashback = 1 AND cashback_company IS NOT NULL"
    ).fetchone()[0]
    log.info(f"  with company: {cb_with_company}")

    # Calc type distribution
    log.info("")
    log.info("Cashback calc_type distribution:")
    rows = conn.execute(
        "SELECT cashback_calc_type, COUNT(*) "
        "FROM limitation_text WHERE is_cashback = 1 "
        "GROUP BY cashback_calc_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    for ct, cnt in rows:
        log.info(f"  {ct or 'NULL'}: {cnt}")

    # Code source distribution in sku_indication
    log.info("")
    log.info("sku_indication code_source distribution:")
    rows = conn.execute(
        "SELECT code_source, COUNT(*) FROM sku_indication "
        "GROUP BY code_source ORDER BY COUNT(*) DESC"
    ).fetchall()
    for src, cnt in rows:
        log.info(f"  {src}: {cnt}")

    # Fallback vs real .XX vs real codes breakdown
    log.info("")
    log.info("Code type breakdown (is_fallback + .XX detection):")
    rows = conn.execute("SELECT code_type, n_links, n_distinct_codes, n_skus FROM v_codes_summary").fetchall()
    for code_type, n_links, n_codes, n_skus in rows:
        log.info(f"  {code_type}: {n_codes} distinct codes, {n_links} links, {n_skus} SKUs")

    # Limitation level distribution
    log.info("")
    log.info("sku_indication limitation_level distribution:")
    rows = conn.execute(
        "SELECT limitation_level, COUNT(*) FROM sku_indication "
        "GROUP BY limitation_level ORDER BY COUNT(*) DESC"
    ).fetchall()
    for lvl, cnt in rows:
        log.info(f"  {lvl}: {cnt}")

    # Code-name mapping stats
    log.info("")
    log.info("indication_code_name mapping:")
    total_map = conn.execute("SELECT COUNT(*) FROM indication_code_name").fetchone()[0]
    distinct_mapped = conn.execute(
        "SELECT COUNT(DISTINCT indication_code) FROM indication_code_name"
    ).fetchone()[0]
    all_codes = conn.execute(
        "SELECT COUNT(DISTINCT indication_code) FROM sku_indication"
    ).fetchone()[0]
    log.info(f"  Total mappings: {total_map}")
    log.info(f"  Distinct codes mapped: {distinct_mapped}/{all_codes} "
             f"({100*distinct_mapped/all_codes:.1f}%)" if all_codes else "")

    log.info("  By match_source:")
    rows = conn.execute(
        "SELECT match_source, COUNT(*), COUNT(DISTINCT indication_code) "
        "FROM indication_code_name GROUP BY match_source ORDER BY COUNT(*) DESC"
    ).fetchall()
    for src, cnt, ucodes in rows:
        log.info(f"    {src}: {cnt} mappings ({ucodes} codes)")

    codes_no_name = conn.execute("SELECT COUNT(*) FROM v_codes_without_names").fetchone()[0]
    names_no_code = conn.execute("SELECT COUNT(*) FROM v_names_without_codes").fetchone()[0]
    log.info(f"  Codes WITHOUT names: {codes_no_name}")
    # Breakdown by code_type
    rows = conn.execute(
        "SELECT code_type, COUNT(*) FROM v_codes_without_names GROUP BY code_type"
    ).fetchall()
    for ct, cnt in rows:
        log.info(f"    {ct}: {cnt}")
    log.info(f"  Names WITHOUT codes: {names_no_code}")


def export_csv(conn):
    """Export denormalized sku_indication view to CSV."""
    log.info("Exporting CSV...")

    df = pd.read_sql("""
        SELECT
            si.gtin, s.product_name, s.atc_code, s.swissmedic_no5,
            s.description_de AS pack_description,
            s.form_type, s.total_units, s.substance_name,
            s.public_price, s.exfactory_price,
            s.valid_from AS sku_valid_from, s.valid_to AS sku_valid_to,
            si.indication_code, si.code_source, si.is_fallback,
            CASE WHEN si.is_fallback = 1 THEN 'FALLBACK'
                 WHEN si.indication_code LIKE '%.XX' THEN 'CROSS_INDICATION'
                 ELSE 'REAL_CODE' END AS code_type,
            si.limitation_level,
            si.valid_from AS link_valid_from, si.valid_to AS link_valid_to,
            lt.limitation_code, lt.limitation_type,
            lt.is_cashback, lt.cashback_company,
            lt.cashback_calc_type, lt.cashback_calc_value, lt.cashback_unit,
            lt.description_fr AS limitation_text_fr
        FROM sku_indication si
        JOIN sku s ON s.gtin = si.gtin
        JOIN limitation_text lt ON lt.text_id = si.text_id
        ORDER BY s.product_name, si.gtin, si.indication_code
    """, conn)

    csv_path = BASE_DIR / "sku_indication_export.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log.info(f"Exported {len(df)} rows to {csv_path.name}")


# ============================================================
# Main
# ============================================================

def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    log.info("=" * 60)
    log.info("Build SKU-Indication Database")
    log.info("=" * 60)

    # Discover XML files
    xml_files = discover_files()
    log.info(f"Found {len(xml_files)} XML files")

    # Phase 0: Setup
    conn = setup_database()
    extract_map = build_extract_info(conn, xml_files)

    # Phase 1: XML Ingestion
    ingest_xml(conn, xml_files)

    # Phase 2: Resolve dates + fan-out
    resolve_dates(conn, extract_map)
    fanout_to_sku(conn, extract_map)

    # Phase 3: Cashback detection
    detect_cashbacks(conn)

    # Phase 3b: Text segmentation
    segment_texts(conn)

    # Phase 3c: Code ↔ name mapping
    build_code_name_mapping(conn)

    # Phase 4: Stats + Export
    print_stats(conn)
    export_csv(conn)

    conn.close()

    log.info("=" * 60)
    log.info("Done!")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

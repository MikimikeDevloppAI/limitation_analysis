"""
Step 01: Parse XML files and create the database from scratch.

Creates sku_indication.db with all raw XML data:
- extract_info: metadata for each XML file
- sku: one row per GTIN with normalized pack attributes
- limitation_text: unique limitation texts (deduplicated by content hash)
- limitation: limitation_code + text version + contiguous period
- indication: master indication table (1 row per unique code, with bag_dossier_no)
- limitation_text_segment: text segments (1 per indication, populated by step_02+)
- sku_limitation: links SKU -> limitation with temporal validity
- company, preparation_company: partner info
"""

import hashlib
import logging
import sqlite3
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

from build_sku_normalized import (
    parse_pack_description, parse_substance_qty, COMPUTABLE_SUBSTANCE_UNITS,
)

log = logging.getLogger(__name__)

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

-- Unique limitation texts (deduplicated by content hash)
CREATE TABLE limitation_text (
    text_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    content_hash        TEXT NOT NULL UNIQUE,
    description_de      TEXT,
    description_fr      TEXT,
    description_it      TEXT,
    text_complexity     TEXT
);

CREATE INDEX idx_text_hash ON limitation_text(content_hash);

-- Limitation context: links a BAG limitation_code to its text version
-- One row per contiguous period (same code+text can reappear after a gap)
CREATE TABLE limitation (
    limitation_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    limitation_code     TEXT NOT NULL,
    limitation_type     TEXT,
    limitation_niveau   TEXT,
    text_id             INTEGER NOT NULL REFERENCES limitation_text(text_id),
    first_seen_extract  INTEGER NOT NULL,
    last_seen_extract   INTEGER NOT NULL,
    valid_from          TEXT,
    valid_to            TEXT
);

CREATE INDEX idx_lim_code ON limitation(limitation_code);
CREATE INDEX idx_lim_text ON limitation(text_id);

-- Master indication table (1 row per unique indication code)
CREATE TABLE indication (
    indication_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    indication_code     TEXT UNIQUE,
    bag_dossier_no      TEXT,
    indication_name_de  TEXT,
    indication_name_fr  TEXT,
    indication_name_it  TEXT,
    name_source         TEXT
);

CREATE INDEX idx_ind_code ON indication(indication_code);
CREATE INDEX idx_ind_dossier ON indication(bag_dossier_no);

-- Segments of limitation texts: 1 row per indication in the text.
-- For simple texts: 1 segment (index=0) = full text.
-- For MULTI_BOLD: N segments, each = one indication section.
-- Replaces the old limitation_indication table.
CREATE TABLE limitation_text_segment (
    segment_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    text_id         INTEGER NOT NULL REFERENCES limitation_text(text_id),
    segment_index   INTEGER NOT NULL,
    description_de  TEXT,
    description_fr  TEXT,
    description_it  TEXT,
    indication_id   INTEGER REFERENCES indication(indication_id),
    code_source     TEXT,
    UNIQUE(text_id, segment_index)
);

CREATE INDEX idx_seg_text ON limitation_text_segment(text_id);
CREATE INDEX idx_seg_ind ON limitation_text_segment(indication_id);

-- SKU <-> Limitation links (with temporal validity)
CREATE TABLE sku_limitation (
    link_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    gtin                TEXT NOT NULL REFERENCES sku(gtin),
    limitation_id       INTEGER NOT NULL REFERENCES limitation(limitation_id),
    limitation_level    TEXT NOT NULL,
    first_seen_extract  INTEGER NOT NULL,
    last_seen_extract   INTEGER NOT NULL,
    valid_from          TEXT,
    valid_to            TEXT,
    UNIQUE(gtin, limitation_id)
);

CREATE INDEX idx_sl_gtin ON sku_limitation(gtin);
CREATE INDEX idx_sl_lim ON sku_limitation(limitation_id);

-- Denormalized view for easy querying
CREATE VIEW v_sku_limitation AS
SELECT
    s.gtin, s.product_name, s.atc_code, s.swissmedic_no5,
    s.description_de AS pack_description,
    s.public_price, s.exfactory_price,
    lim.limitation_code, lim.limitation_type, lim.limitation_niveau,
    seg.code_source,
    ind.indication_code, ind.bag_dossier_no AS indication_dossier,
    ind.indication_name_de, ind.indication_name_fr,
    seg.description_fr AS segment_text_fr,
    seg.description_de AS segment_text_de,
    lt.description_de AS full_text_de,
    lt.description_fr AS full_text_fr,
    sl.limitation_level,
    sl.valid_from, sl.valid_to
FROM sku_limitation sl
JOIN sku s ON s.gtin = sl.gtin
JOIN limitation lim ON lim.limitation_id = sl.limitation_id
JOIN limitation_text lt ON lt.text_id = lim.text_id
LEFT JOIN limitation_text_segment seg ON seg.text_id = lt.text_id
LEFT JOIN indication ind ON ind.indication_id = seg.indication_id;

-- Companies (from XML Partner elements)
CREATE TABLE company (
    company_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name    TEXT NOT NULL UNIQUE,
    partner_type    TEXT,
    street          TEXT,
    zip_code        TEXT,
    place           TEXT
);

CREATE INDEX idx_company_name ON company(company_name);

-- Preparation -> Company link
CREATE TABLE preparation_company (
    preparation_id  INTEGER NOT NULL,
    company_id      INTEGER NOT NULL REFERENCES company(company_id),
    partner_type    TEXT,
    first_seen_extract INTEGER NOT NULL,
    last_seen_extract  INTEGER NOT NULL,
    UNIQUE(preparation_id, company_id)
);

-- Temporary table: text_id -> indication mapping from XML codes (used by step_02)
CREATE TABLE _text_indication_xml (
    text_id         INTEGER NOT NULL,
    indication_id   INTEGER NOT NULL,
    indication_code TEXT NOT NULL,
    UNIQUE(text_id, indication_id)
);

-- Temporary table for preparation-level links (dropped after fan-out)
CREATE TABLE _prep_lim_link (
    prep_link_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    preparation_id      INTEGER NOT NULL,
    limitation_id       INTEGER NOT NULL,
    limitation_level    TEXT NOT NULL,
    first_seen_extract  INTEGER NOT NULL,
    last_seen_extract   INTEGER NOT NULL,
    UNIQUE(preparation_id, limitation_id)
);
"""


# ============================================================
# XML utility functions
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
    """Extract codes from structured XML elements (post-Feb 2023)."""
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


def get_release_date(file_path):
    """Extract ReleaseDate from the root element."""
    for event, elem in ET.iterparse(str(file_path), events=("start",)):
        return elem.get("ReleaseDate", "")


def discover_files(extracted_dir):
    """Find all Preparations-*.xml files across year directories."""
    files = []
    for year_dir in sorted(extracted_dir.iterdir()):
        if year_dir.is_dir() and year_dir.name.isdigit():
            for xml_file in sorted(year_dir.glob("Preparations-*.xml")):
                files.append(xml_file)
    return files


# ============================================================
# Ingestion helpers
# ============================================================

_prep_counter = 0
_prep_map = {}  # swissmedic_no5 -> preparation_id


def _reset_prep_state():
    global _prep_counter, _prep_map
    _prep_counter = 0
    _prep_map = {}


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
        conn.execute(
            "UPDATE sku SET last_seen_extract = ?, "
            "public_price = COALESCE(?, public_price), "
            "exfactory_price = COALESCE(?, exfactory_price) "
            "WHERE sku_id = ?",
            (extract_id, public_price, exfactory_price, existing[0]),
        )
        return

    parsed = parse_pack_description(description_de) if description_de else {}
    sub_qty = parse_substance_qty(substance_qty_raw)

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


def upsert_limitation_text(conn, content_hash, desc_de, desc_fr, desc_it):
    """Insert or return a unique limitation text. Returns text_id."""
    row = conn.execute(
        "SELECT text_id FROM limitation_text WHERE content_hash = ?",
        (content_hash,),
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO limitation_text "
        "(content_hash, description_de, description_fr, description_it) "
        "VALUES (?, ?, ?, ?)",
        (content_hash, desc_de, desc_fr, desc_it),
    )
    return cur.lastrowid


def upsert_limitation(conn, extract_id, lim_code, lim_type, lim_niveau, text_id):
    """Insert or update a limitation period (code + text version).

    If the same (limitation_code, text_id) was seen in the immediately
    preceding extract, extend that period. Otherwise create a new row
    so that non-contiguous appearances become separate periods.
    """
    row = conn.execute(
        "SELECT limitation_id, last_seen_extract FROM limitation "
        "WHERE limitation_code = ? AND text_id = ? "
        "ORDER BY last_seen_extract DESC LIMIT 1",
        (lim_code, text_id),
    ).fetchone()
    if row and row[1] >= extract_id - 1:
        conn.execute(
            "UPDATE limitation SET last_seen_extract = ? WHERE limitation_id = ?",
            (extract_id, row[0]),
        )
        return row[0]
    cur = conn.execute(
        "INSERT INTO limitation "
        "(limitation_code, limitation_type, limitation_niveau, text_id, "
        " first_seen_extract, last_seen_extract) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (lim_code, lim_type, lim_niveau, text_id, extract_id, extract_id),
    )
    return cur.lastrowid


def upsert_prep_lim_link(conn, extract_id, preparation_id,
                          limitation_id, level):
    """Insert or update a preparation-level limitation link."""
    row = conn.execute(
        "SELECT prep_link_id FROM _prep_lim_link "
        "WHERE preparation_id = ? AND limitation_id = ?",
        (preparation_id, limitation_id),
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE _prep_lim_link SET last_seen_extract = ? "
            "WHERE prep_link_id = ?",
            (extract_id, row[0]),
        )
    else:
        conn.execute(
            "INSERT INTO _prep_lim_link "
            "(preparation_id, limitation_id, limitation_level, "
            " first_seen_extract, last_seen_extract) "
            "VALUES (?, ?, ?, ?, ?)",
            (preparation_id, limitation_id, level, extract_id, extract_id),
        )


def upsert_pack_sku_link(conn, extract_id, gtin, limitation_id):
    """Insert/update a PACK-level limitation link directly into sku_limitation.

    PACK-level limitations are specific to a single GTIN and must NOT be
    fanned out to all GTINs in the preparation (unlike PREPARATION-level).
    """
    row = conn.execute(
        "SELECT link_id FROM sku_limitation "
        "WHERE gtin = ? AND limitation_id = ?",
        (gtin, limitation_id),
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE sku_limitation SET last_seen_extract = ? WHERE link_id = ?",
            (extract_id, row[0]),
        )
    else:
        conn.execute(
            "INSERT INTO sku_limitation "
            "(gtin, limitation_id, limitation_level, "
            " first_seen_extract, last_seen_extract) "
            "VALUES (?, ?, 'PACK', ?, ?)",
            (gtin, limitation_id, extract_id, extract_id),
        )


def _get_dossier_from_code(code):
    """Extract bag_dossier_no from indication code: '20461.07' -> '20461'."""
    if code and "." in code:
        return code.split(".")[0]
    return None


def process_limitation(conn, extract_id, preparation_id, lim_elem,
                       level, bag_dossier_no):
    """Process one <Limitation> element. Returns limitation_id or None."""
    if level == "ITCODE":
        return None

    lim_code = get_text(lim_elem, "LimitationCode")
    lim_type = get_text(lim_elem, "LimitationType")
    lim_niveau = get_text(lim_elem, "LimitationNiveau")
    desc_de = get_text(lim_elem, "DescriptionDe")
    desc_fr = get_text(lim_elem, "DescriptionFr")
    desc_it = get_text(lim_elem, "DescriptionIt")

    c_hash = compute_hash(desc_de, desc_fr, desc_it)
    text_id = upsert_limitation_text(conn, c_hash, desc_de, desc_fr, desc_it)

    limitation_id = upsert_limitation(
        conn, extract_id, lim_code, lim_type, lim_niveau, text_id,
    )

    # Structured XML codes (post-Feb 2023) — create indication rows
    # and record text_id -> indication mapping for step_02.
    for code in extract_codes_structured(lim_elem):
        dossier = _get_dossier_from_code(code)
        conn.execute(
            "INSERT OR IGNORE INTO indication (indication_code, bag_dossier_no) "
            "VALUES (?, ?)",
            (code, dossier),
        )
        ind_row = conn.execute(
            "SELECT indication_id FROM indication WHERE indication_code = ?",
            (code,),
        ).fetchone()
        conn.execute(
            "INSERT OR IGNORE INTO _text_indication_xml "
            "(text_id, indication_id, indication_code) VALUES (?, ?, ?)",
            (text_id, ind_row[0], code),
        )

    # Only PREPARATION-level goes to _prep_lim_link for fan-out.
    # PACK-level is inserted directly into sku_limitation by the caller.
    if level == "PREPARATION":
        upsert_prep_lim_link(conn, extract_id, preparation_id,
                             limitation_id, level)

    return limitation_id


def process_preparation(conn, extract_id, prep_elem):
    """Process one <Preparation> element."""
    swissmedic_no5 = get_text(prep_elem, "SwissmedicNo5")
    if not swissmedic_no5:
        return

    preparation_id = get_preparation_id(swissmedic_no5)

    name_de = get_text(prep_elem, "NameDe")
    atc_code = get_text(prep_elem, "AtcCode")
    org_gen_code = get_text(prep_elem, "OrgGenCode")

    sub_elem = prep_elem.find(".//Substances/Substance")
    sub_name = get_text(sub_elem, "DescriptionLa") if sub_elem is not None else None
    sub_qty_raw = get_text(sub_elem, "Quantity") if sub_elem is not None else None
    sub_unit = get_text(sub_elem, "QuantityUnit") if sub_elem is not None else None

    for pack_elem in prep_elem.findall(".//Packs/Pack"):
        gtin = get_text(pack_elem, "GTIN")
        if not gtin:
            continue
        swissmedic_no8 = get_text(pack_elem, "SwissmedicNo8")
        bag_dossier_no = get_text(pack_elem, "BagDossierNo")
        pack_desc_de = get_text(pack_elem, "DescriptionDe")
        pub_price = get_price(pack_elem, "Prices/PublicPrice")
        exf_price = get_price(pack_elem, "Prices/ExFactoryPrice")

        upsert_sku(
            conn, extract_id, gtin, swissmedic_no8, swissmedic_no5,
            bag_dossier_no, preparation_id, name_de, atc_code,
            org_gen_code, pack_desc_de, sub_name, sub_qty_raw, sub_unit,
            pub_price, exf_price,
        )

        # Pack-level limitations → insert directly into sku_limitation
        lims = pack_elem.find("Limitations")
        if lims is not None:
            for lim_elem in lims.findall("Limitation"):
                lim_id = process_limitation(
                    conn, extract_id, preparation_id, lim_elem,
                    "PACK", bag_dossier_no,
                )
                if lim_id is not None:
                    upsert_pack_sku_link(conn, extract_id, gtin, lim_id)

        # Partners (company names)
        pack_partners = pack_elem.find("Partners")
        if pack_partners is not None:
            for partner in pack_partners.findall("Partner"):
                company_name = get_text(partner, "Description")
                if not company_name:
                    continue
                partner_type = get_text(partner, "PartnerType")
                street = get_text(partner, "Street")
                zip_code = get_text(partner, "ZipCode")
                place = get_text(partner, "Place")
                row = conn.execute(
                    "SELECT company_id FROM company WHERE company_name = ?",
                    (company_name,),
                ).fetchone()
                if row:
                    company_id = row[0]
                else:
                    cur = conn.execute(
                        "INSERT INTO company "
                        "(company_name, partner_type, street, zip_code, place) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (company_name, partner_type, street, zip_code, place),
                    )
                    company_id = cur.lastrowid
                # Link to preparation
                existing_link = conn.execute(
                    "SELECT rowid FROM preparation_company "
                    "WHERE preparation_id = ? AND company_id = ?",
                    (preparation_id, company_id),
                ).fetchone()
                if existing_link:
                    conn.execute(
                        "UPDATE preparation_company SET last_seen_extract = ? "
                        "WHERE preparation_id = ? AND company_id = ?",
                        (extract_id, preparation_id, company_id),
                    )
                else:
                    conn.execute(
                        "INSERT INTO preparation_company "
                        "(preparation_id, company_id, partner_type, "
                        " first_seen_extract, last_seen_extract) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (preparation_id, company_id, partner_type,
                         extract_id, extract_id),
                    )

    # Preparation-level limitations
    prep_lims = prep_elem.find("Limitations")
    if prep_lims is not None:
        for lim_elem in prep_lims.findall("Limitation"):
            process_limitation(
                conn, extract_id, preparation_id, lim_elem,
                "PREPARATION", None,
            )


# ============================================================
# Fan-out + resolve dates
# ============================================================

def fanout_to_sku(conn, extract_map):
    """Expand preparation-level limitation links to individual SKU links."""
    log.info("  Fan-out preparation links to SKU links...")

    sku_rows = conn.execute(
        "SELECT gtin, preparation_id, first_seen_extract, last_seen_extract "
        "FROM sku"
    ).fetchall()

    skus_by_prep = defaultdict(list)
    for gtin, prep_id, first_ext, last_ext in sku_rows:
        skus_by_prep[prep_id].append({
            "gtin": gtin,
            "first_ext": first_ext,
            "last_ext": last_ext,
        })

    links = conn.execute(
        "SELECT preparation_id, limitation_id, limitation_level, "
        "       first_seen_extract, last_seen_extract "
        "FROM _prep_lim_link"
    ).fetchall()

    inserted = 0
    skipped = 0

    for prep_id, lim_id, level, link_first, link_last in links:
        for sku in skus_by_prep.get(prep_id, []):
            if sku["last_ext"] < link_first or sku["first_ext"] > link_last:
                continue

            eff_first = max(sku["first_ext"], link_first)
            eff_last = min(sku["last_ext"], link_last)

            try:
                conn.execute(
                    "INSERT OR IGNORE INTO sku_limitation "
                    "(gtin, limitation_id, limitation_level, "
                    " first_seen_extract, last_seen_extract) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (sku["gtin"], lim_id, level, eff_first, eff_last),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                skipped += 1

    conn.commit()
    conn.execute("DROP TABLE IF EXISTS _prep_lim_link")
    conn.commit()

    actual = conn.execute("SELECT COUNT(*) FROM sku_limitation").fetchone()[0]
    log.info(f"    {actual} SKU-limitation links ({skipped} duplicates skipped)")


def resolve_dates(conn, extract_map):
    """Resolve first/last_seen_extract to human-readable dates."""
    log.info("  Resolving extract IDs to dates...")

    for table in ("sku", "limitation", "sku_limitation"):
        rows = conn.execute(
            f"SELECT rowid, first_seen_extract, last_seen_extract FROM {table}"
        ).fetchall()
        for rowid, first_ext, last_ext in rows:
            conn.execute(
                f"UPDATE {table} SET valid_from = ?, valid_to = ? WHERE rowid = ?",
                (extract_map.get(first_ext, ""), extract_map.get(last_ext, ""), rowid),
            )
    conn.commit()

    conn.commit()


# ============================================================
# Main entry point
# ============================================================

def run(db_path, extracted_dir):
    """Run step 01: create DB and ingest all XML files."""
    _reset_prep_state()

    xml_files = discover_files(extracted_dir)
    log.info(f"Step 01: Parse XML — {len(xml_files)} files")

    # Create DB from scratch
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA_SQL)
    log.info(f"  Created database: {db_path.name}")

    # Register extracts
    extract_map = {}
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
    log.info(f"  Registered {len(xml_files)} XML extracts")

    # Ingest XML
    log.info("  Ingesting XML files...")
    for i, f in enumerate(xml_files, 1):
        extract_id = i
        context = ET.iterparse(str(f), events=("end",))
        for event, elem in context:
            if elem.tag == "Preparation":
                process_preparation(conn, extract_id, elem)
                elem.clear()
        conn.commit()
        if i % 10 == 0 or i == len(xml_files):
            log.info(f"    Processed {i}/{len(xml_files)} files")

    sku_count = conn.execute("SELECT COUNT(*) FROM sku").fetchone()[0]
    text_count = conn.execute("SELECT COUNT(*) FROM limitation_text").fetchone()[0]
    lim_count = conn.execute("SELECT COUNT(*) FROM limitation").fetchone()[0]
    ind_count = conn.execute("SELECT COUNT(*) FROM indication").fetchone()[0]
    link_count = conn.execute("SELECT COUNT(*) FROM _prep_lim_link").fetchone()[0]
    log.info(f"    {sku_count} SKUs, {text_count} texts, {lim_count} limitations, "
             f"{ind_count} indications, {link_count} prep-level links")

    # Fan-out + resolve dates
    fanout_to_sku(conn, extract_map)
    resolve_dates(conn, extract_map)

    # Remove orphan limitations (no SKU link after fan-out)
    orphan_lim = conn.execute(
        "SELECT COUNT(*) FROM limitation l "
        "WHERE NOT EXISTS "
        "(SELECT 1 FROM sku_limitation sl WHERE sl.limitation_id = l.limitation_id)"
    ).fetchone()[0]
    if orphan_lim:
        conn.execute(
            "DELETE FROM limitation WHERE limitation_id NOT IN "
            "(SELECT DISTINCT limitation_id FROM sku_limitation)"
        )
        # Remove limitation_text rows no longer referenced by any limitation
        conn.execute(
            "DELETE FROM limitation_text WHERE text_id NOT IN "
            "(SELECT DISTINCT text_id FROM limitation)"
        )
        conn.commit()
        log.info(f"  Removed {orphan_lim} orphan limitations (no SKU link)")

    # Summary
    for table in ("extract_info", "sku", "limitation_text", "limitation",
                  "indication", "limitation_text_segment", "sku_limitation",
                  "company", "preparation_company"):
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info(f"    {table}: {cnt}")

    conn.close()
    log.info("  Step 01 done.")

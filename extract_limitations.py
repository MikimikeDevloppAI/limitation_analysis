"""
Extract limitation text, indication codes, and indication names from BAG Preparations XML files.
Builds a deduplicated SQLite database with change history, name-to-code mapping,
retroactive code assignment, and exports to CSV/Excel.

Run from scratch: deletes and rebuilds the database each time.
"""

import hashlib
import html
import logging
import re
import sqlite3
import xml.etree.ElementTree as ET
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path

import sys

import pandas as pd

# ============================================================
# Configuration
# ============================================================

BASE_DIR = Path(r"c:\Users\micha\OneDrive\Matching_indication_code")
EXTRACTED_DIR = BASE_DIR / "extracted"
DB_PATH = BASE_DIR / "swiss_pharma_limitations.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ============================================================
# Regex patterns
# ============================================================

# Numeric indication code: 5 digits, dot, 2 digits
RE_NUMERIC = re.compile(r"(\d{5}\.\d{2})\.?\b")

# Bold indication name extraction (XML parser already decodes &lt;b&gt; to <b>)
RE_BOLD = re.compile(r"<b>(.+?)</b>")

# Context-aware patterns for code extraction from free text
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
# Database Schema
# ============================================================

SCHEMA_SQL = """
CREATE TABLE extract (
    extract_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name     TEXT NOT NULL UNIQUE,
    release_date  TEXT NOT NULL,
    file_year     INTEGER NOT NULL
);

CREATE TABLE preparation (
    preparation_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    swissmedic_no5      TEXT NOT NULL,
    name_de             TEXT,
    name_fr             TEXT,
    name_it             TEXT,
    description_de      TEXT,
    description_fr      TEXT,
    description_it      TEXT,
    atc_code            TEXT,
    org_gen_code        TEXT,
    flag_it_limitation  TEXT,
    flag_sb             TEXT,
    flag_ggsl           TEXT,
    comment_de          TEXT,
    comment_fr          TEXT,
    comment_it          TEXT,
    vat_in_exf          TEXT,
    first_seen_extract  INTEGER REFERENCES extract(extract_id),
    last_seen_extract   INTEGER REFERENCES extract(extract_id),
    UNIQUE(swissmedic_no5)
);

CREATE TABLE pack (
    pack_id_db          INTEGER PRIMARY KEY AUTOINCREMENT,
    preparation_id      INTEGER NOT NULL REFERENCES preparation(preparation_id),
    gtin                TEXT,
    swissmedic_no8      TEXT,
    bag_dossier_no      TEXT,
    description_de      TEXT,
    description_fr      TEXT,
    description_it      TEXT,
    swissmedic_category TEXT,
    flag_narcosis       TEXT,
    flag_modal          TEXT,
    flag_ggsl           TEXT,
    size_pack           TEXT,
    prev_gtin_code      TEXT,
    swissmedic_no8_parallel_imp TEXT,
    public_price        REAL,
    public_price_valid_from TEXT,
    exfactory_price     REAL,
    exfactory_price_valid_from TEXT,
    wholesale_margin_grp TEXT,
    uniform_wholesale_margin TEXT,
    integration_date    TEXT,
    valid_from_date     TEXT,
    valid_thru_date     TEXT,
    status_type_code    TEXT,
    status_type_desc    TEXT,
    flag_apd            TEXT,
    first_seen_extract  INTEGER REFERENCES extract(extract_id),
    last_seen_extract   INTEGER REFERENCES extract(extract_id),
    UNIQUE(gtin)
);

CREATE TABLE limitation (
    limitation_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    preparation_id      INTEGER NOT NULL REFERENCES preparation(preparation_id),
    limitation_level    TEXT NOT NULL,
    limitation_code     TEXT,
    limitation_type     TEXT,
    limitation_niveau   TEXT,
    indication_name_de  TEXT,
    indication_name_fr  TEXT,
    indication_name_it  TEXT,
    description_de      TEXT,
    description_fr      TEXT,
    description_it      TEXT,
    valid_from_date     TEXT,
    valid_thru_date     TEXT,
    first_seen_extract  INTEGER REFERENCES extract(extract_id),
    last_seen_extract   INTEGER REFERENCES extract(extract_id),
    content_hash        TEXT,
    UNIQUE(preparation_id, limitation_code, limitation_level, content_hash)
);

CREATE TABLE indication_code (
    indication_code_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    limitation_id       INTEGER NOT NULL REFERENCES limitation(limitation_id),
    preparation_id      INTEGER NOT NULL REFERENCES preparation(preparation_id),
    bag_dossier_no      TEXT,
    code_value          TEXT NOT NULL,
    code_source         TEXT NOT NULL,
    dossier_part        TEXT,
    indication_part     TEXT,
    first_seen_extract  INTEGER REFERENCES extract(extract_id),
    last_seen_extract   INTEGER REFERENCES extract(extract_id),
    UNIQUE(limitation_id, code_value)
);

CREATE TABLE indication_name_code_map (
    map_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    indication_name_de  TEXT NOT NULL,
    indication_name_fr  TEXT,
    indication_name_it  TEXT,
    code_value          TEXT NOT NULL,
    bag_dossier_no      TEXT,
    product_name        TEXT,
    source_limitation_code TEXT,
    UNIQUE(indication_name_de, code_value)
);

CREATE TABLE limitation_indication_segment (
    segment_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    limitation_id       INTEGER NOT NULL REFERENCES limitation(limitation_id),
    preparation_id      INTEGER NOT NULL REFERENCES preparation(preparation_id),
    segment_order       INTEGER NOT NULL,
    indication_name_de  TEXT,
    indication_name_fr  TEXT,
    indication_name_it  TEXT,
    segment_text_de     TEXT,
    segment_text_fr     TEXT,
    segment_text_it     TEXT,
    matched_code_value  TEXT,
    matched_code_source TEXT,
    UNIQUE(limitation_id, segment_order)
);

CREATE TABLE substance (
    substance_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    preparation_id    INTEGER NOT NULL REFERENCES preparation(preparation_id),
    description_la    TEXT,
    quantity          TEXT,
    quantity_unit     TEXT,
    UNIQUE(preparation_id, description_la)
);

CREATE TABLE pack_partner (
    partner_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    pack_id_db     INTEGER NOT NULL REFERENCES pack(pack_id_db),
    partner_type   TEXT,
    description    TEXT,
    street         TEXT,
    zip_code       TEXT,
    place          TEXT,
    phone          TEXT,
    UNIQUE(pack_id_db, partner_type, description)
);

CREATE TABLE limitation_text (
    text_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    content_hash      TEXT NOT NULL UNIQUE,
    limitation_code   TEXT,
    limitation_type   TEXT,
    limitation_niveau TEXT,
    description_de    TEXT,
    description_fr    TEXT,
    description_it    TEXT,
    has_cashback      INTEGER DEFAULT 0,
    first_seen_extract INTEGER REFERENCES extract(extract_id),
    last_seen_extract  INTEGER REFERENCES extract(extract_id)
);

CREATE TABLE limitation_code_link (
    link_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    text_id             INTEGER NOT NULL REFERENCES limitation_text(text_id),
    preparation_id      INTEGER NOT NULL REFERENCES preparation(preparation_id),
    indication_code     TEXT NOT NULL,
    code_source         TEXT NOT NULL,
    limitation_level    TEXT NOT NULL,
    first_seen_extract  INTEGER REFERENCES extract(extract_id),
    last_seen_extract   INTEGER REFERENCES extract(extract_id),
    UNIQUE(text_id, preparation_id, indication_code)
);

CREATE VIEW v_sku_indications AS
SELECT
    pr.name_de AS product_name,
    pr.atc_code,
    pr.swissmedic_no5,
    pk.swissmedic_no8,
    pk.gtin,
    pk.bag_dossier_no,
    pk.description_de AS pack_desc,
    -- Pack temporal validity
    e_pk_first.release_date AS pack_first_seen,
    e_pk_last.release_date  AS pack_last_seen,
    -- Limitation info
    l.limitation_code,
    l.limitation_type,
    l.limitation_level,
    l.indication_name_de,
    l.indication_name_fr,
    l.indication_name_it,
    l.description_de AS limitation_text,
    l.valid_from_date AS limitation_valid_from,
    l.valid_thru_date AS limitation_valid_thru,
    -- Limitation temporal validity
    e_lim_first.release_date AS limitation_first_seen,
    e_lim_last.release_date  AS limitation_last_seen,
    -- Indication code info
    ic.code_value AS indication_code,
    ic.code_source,
    ic.dossier_part,
    ic.indication_part,
    -- Indication code temporal validity
    e_ic_first.release_date AS code_first_seen,
    e_ic_last.release_date  AS code_last_seen,
    -- Effective validity: the overlap period where pack + code both existed
    -- (use extract_id for correct chronological MAX/MIN, then resolve to dates)
    e_eff_from.release_date AS effective_from,
    e_eff_to.release_date   AS effective_to
FROM indication_code ic
JOIN limitation l  ON ic.limitation_id  = l.limitation_id
JOIN preparation pr ON ic.preparation_id = pr.preparation_id
LEFT JOIN pack pk
    ON  pk.preparation_id = pr.preparation_id
    AND pk.bag_dossier_no = ic.bag_dossier_no
    AND pk.first_seen_extract <= ic.last_seen_extract
    AND pk.last_seen_extract  >= ic.first_seen_extract
LEFT JOIN extract e_pk_first  ON pk.first_seen_extract = e_pk_first.extract_id
LEFT JOIN extract e_pk_last   ON pk.last_seen_extract  = e_pk_last.extract_id
LEFT JOIN extract e_lim_first ON l.first_seen_extract  = e_lim_first.extract_id
LEFT JOIN extract e_lim_last  ON l.last_seen_extract   = e_lim_last.extract_id
LEFT JOIN extract e_ic_first  ON ic.first_seen_extract = e_ic_first.extract_id
LEFT JOIN extract e_ic_last   ON ic.last_seen_extract  = e_ic_last.extract_id
LEFT JOIN extract e_eff_from  ON e_eff_from.extract_id = MAX(pk.first_seen_extract, ic.first_seen_extract)
LEFT JOIN extract e_eff_to    ON e_eff_to.extract_id   = MIN(pk.last_seen_extract, ic.last_seen_extract);
"""


# ============================================================
# Helper functions
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


def split_code(code_value):
    """Split an indication code into dossier and indication parts."""
    if "." in code_value:
        parts = code_value.split(".", 1)
        return parts[0], parts[1]
    return code_value, None


def extract_indication_names(lim_type, desc_de, desc_fr, desc_it):
    """Extract bold indication names from all 3 language descriptions.

    Only extracts from DIA-type limitations. Returns (name_de, name_fr, name_it).
    """
    if lim_type != "DIA":
        return None, None, None

    names = {}
    for lang, text in [("de", desc_de), ("fr", desc_fr), ("it", desc_it)]:
        if text:
            matches = RE_BOLD.findall(text)
            names[lang] = " | ".join(matches) if matches else None
        else:
            names[lang] = None
    return names["de"], names["fr"], names["it"]


# ============================================================
# Text splitting: decompose multi-indication texts into segments
# ============================================================

# Bold names that are structural markers, not indication names.
# These appear as <b>UND</b>, <b>ODER</b>, etc. in criteria text.
STRUCTURAL_BOLD_NAMES = {
    "UND", "ODER", "AND", "OR", "ET", "OU",
    "und", "oder", "and", "or", "et", "ou",
}

# Bold names starting with these prefixes are therapy conditions, not indications.
STRUCTURAL_PREFIXES = (
    "Vor Therapiebeginn",
    "Therapiefortführung", "Therapiefortsetzung",
    "Therapieabbruch",
    "nach AJCC",
    "Fr. ",    # Swiss franc amounts
    "CHF ",
    "Maximal ",
    "Dosierungsschema",
    "Für alle vergütungspflichtigen",
    "Rückerstattungen",    # Reimbursement section headers
    "Erwachsene",          # Age category headers (Saxenda, Wegovy)
    "Kriterien für die Vergütung",  # Reimbursement criteria (Evrysdi, Spinraza)
)


def _is_structural_name(name):
    """Return True if the bold name is a structural marker, not an indication."""
    if not name:
        return True
    stripped = name.strip().rstrip(":")
    if stripped in STRUCTURAL_BOLD_NAMES:
        return True
    if stripped.startswith(STRUCTURAL_PREFIXES):
        return True
    # Pure numbers (e.g., "80", "20") are not indication names
    if stripped.replace(".", "").replace(",", "").isdigit():
        return True
    # Very short tokens (1-3 chars, lowercase) are likely structural
    if len(stripped) <= 3 and stripped.islower():
        return True
    return False

# Limitation codes that use bold text for non-indication purposes
NON_INDICATION_LIM_CODES = {"KLEINPACKUNG"}


# Match bold markers that act as paragraph-level indication headers.
# These appear at the start of text or after a line break (<br>, <br/>, newline).
# Inline bold (mid-sentence emphasis) is NOT matched.
RE_HEADER_BOLD = re.compile(
    r"(?:^|<br\s*/?>[\s\n]*(?:<br\s*/?>[\s\n]*)*)"  # start or <br>\n (possibly double)
    r"(<b>.+?</b>)",
    re.MULTILINE,
)


def split_text_by_indication(text):
    """Split a limitation text at paragraph-level <b>Name</b> headers.

    Only bold markers at the start of a paragraph (after <br>\\n or at pos 0)
    are treated as indication headers.  Inline bold is kept as part of the
    segment text.

    Returns list of (indication_name, segment_text) tuples.
    """
    if not text:
        return []

    # Find all paragraph-level bold headers
    headers = list(RE_HEADER_BOLD.finditer(text))
    if not headers:
        return []

    segments = []
    for i, m in enumerate(headers):
        name = RE_BOLD.search(m.group(1)).group(1)

        # Segment text runs from end of this header to start of next header
        # (or end of text for the last one)
        seg_start = m.end()
        if i + 1 < len(headers):
            seg_end = headers[i + 1].start()
        else:
            seg_end = len(text)

        seg_text = text[seg_start:seg_end].strip()
        segments.append((name, seg_text))

    return segments


def split_limitation_texts(desc_de, desc_fr, desc_it):
    """Split all 3 language texts and align them by position.

    Returns list of dicts: [{order, name_de, name_fr, name_it,
                             text_de, text_fr, text_it}, ...]
    The alignment is positional (segment 0 in DE matches segment 0 in FR/IT).
    """
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
# Indication code extraction
# ============================================================

def extract_codes_structured(lim_elem):
    """Extract codes from structured XML elements (2023+)."""
    codes = []

    # Feb 2023+: <IndicationsCodes><IndicationsCode Code="..."/></IndicationsCodes>
    container = lim_elem.find("IndicationsCodes")
    if container is not None:
        for ic in container.findall("IndicationsCode"):
            code = (ic.get("Code") or "").strip()
            if code:
                codes.append(code)

    # Jan 2023 only: <PmIndications><PmIndication Code="..."/></PmIndications>
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


# ============================================================
# Database operations
# ============================================================

def upsert_preparation(conn, extract_id, swissmedic_no5, name_de, atc_code,
                        name_fr=None, name_it=None,
                        description_de=None, description_fr=None, description_it=None,
                        org_gen_code=None, flag_it_limitation=None,
                        flag_sb=None, flag_ggsl=None,
                        comment_de=None, comment_fr=None, comment_it=None,
                        vat_in_exf=None):
    """Insert or update a preparation record. Returns preparation_id."""
    cur = conn.execute(
        "SELECT preparation_id FROM preparation WHERE swissmedic_no5 = ?",
        (swissmedic_no5,),
    )
    row = cur.fetchone()
    if row:
        conn.execute(
            "UPDATE preparation SET last_seen_extract = ?, "
            "name_de = ?, name_fr = ?, name_it = ?, "
            "description_de = ?, description_fr = ?, description_it = ?, "
            "atc_code = ?, org_gen_code = ?, "
            "flag_it_limitation = ?, flag_sb = ?, flag_ggsl = ?, "
            "comment_de = ?, comment_fr = ?, comment_it = ?, "
            "vat_in_exf = ? "
            "WHERE preparation_id = ?",
            (extract_id,
             name_de, name_fr, name_it,
             description_de, description_fr, description_it,
             atc_code, org_gen_code,
             flag_it_limitation, flag_sb, flag_ggsl,
             comment_de, comment_fr, comment_it,
             vat_in_exf,
             row[0]),
        )
        return row[0]
    else:
        cur = conn.execute(
            "INSERT INTO preparation (swissmedic_no5, "
            "name_de, name_fr, name_it, "
            "description_de, description_fr, description_it, "
            "atc_code, org_gen_code, "
            "flag_it_limitation, flag_sb, flag_ggsl, "
            "comment_de, comment_fr, comment_it, "
            "vat_in_exf, "
            "first_seen_extract, last_seen_extract) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (swissmedic_no5,
             name_de, name_fr, name_it,
             description_de, description_fr, description_it,
             atc_code, org_gen_code,
             flag_it_limitation, flag_sb, flag_ggsl,
             comment_de, comment_fr, comment_it,
             vat_in_exf,
             extract_id, extract_id),
        )
        return cur.lastrowid


def upsert_pack(conn, extract_id, preparation_id, gtin, swissmedic_no8,
                bag_dossier_no, description_de,
                description_fr=None, description_it=None,
                swissmedic_category=None, flag_narcosis=None,
                flag_modal=None, flag_ggsl=None,
                size_pack=None, prev_gtin_code=None,
                swissmedic_no8_parallel_imp=None,
                public_price=None, public_price_valid_from=None,
                exfactory_price=None, exfactory_price_valid_from=None,
                wholesale_margin_grp=None, uniform_wholesale_margin=None,
                integration_date=None, valid_from_date=None,
                valid_thru_date=None, status_type_code=None,
                status_type_desc=None, flag_apd=None):
    """Insert or update a pack record. Returns pack_id_db."""
    if not gtin:
        return None
    cur = conn.execute("SELECT pack_id_db FROM pack WHERE gtin = ?", (gtin,))
    row = cur.fetchone()
    if row:
        conn.execute(
            "UPDATE pack SET last_seen_extract = ?, "
            "bag_dossier_no = ?, swissmedic_no8 = ?, "
            "description_de = ?, description_fr = ?, description_it = ?, "
            "swissmedic_category = ?, flag_narcosis = ?, "
            "flag_modal = ?, flag_ggsl = ?, "
            "size_pack = ?, prev_gtin_code = ?, "
            "swissmedic_no8_parallel_imp = ?, "
            "public_price = ?, public_price_valid_from = ?, "
            "exfactory_price = ?, exfactory_price_valid_from = ?, "
            "wholesale_margin_grp = ?, uniform_wholesale_margin = ?, "
            "integration_date = ?, valid_from_date = ?, "
            "valid_thru_date = ?, status_type_code = ?, "
            "status_type_desc = ?, flag_apd = ? "
            "WHERE pack_id_db = ?",
            (extract_id,
             bag_dossier_no, swissmedic_no8,
             description_de, description_fr, description_it,
             swissmedic_category, flag_narcosis,
             flag_modal, flag_ggsl,
             size_pack, prev_gtin_code,
             swissmedic_no8_parallel_imp,
             public_price, public_price_valid_from,
             exfactory_price, exfactory_price_valid_from,
             wholesale_margin_grp, uniform_wholesale_margin,
             integration_date, valid_from_date,
             valid_thru_date, status_type_code,
             status_type_desc, flag_apd,
             row[0]),
        )
        return row[0]
    else:
        cur = conn.execute(
            "INSERT INTO pack (preparation_id, gtin, swissmedic_no8, "
            "bag_dossier_no, "
            "description_de, description_fr, description_it, "
            "swissmedic_category, flag_narcosis, "
            "flag_modal, flag_ggsl, "
            "size_pack, prev_gtin_code, "
            "swissmedic_no8_parallel_imp, "
            "public_price, public_price_valid_from, "
            "exfactory_price, exfactory_price_valid_from, "
            "wholesale_margin_grp, uniform_wholesale_margin, "
            "integration_date, valid_from_date, "
            "valid_thru_date, status_type_code, "
            "status_type_desc, flag_apd, "
            "first_seen_extract, last_seen_extract) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (preparation_id, gtin, swissmedic_no8,
             bag_dossier_no,
             description_de, description_fr, description_it,
             swissmedic_category, flag_narcosis,
             flag_modal, flag_ggsl,
             size_pack, prev_gtin_code,
             swissmedic_no8_parallel_imp,
             public_price, public_price_valid_from,
             exfactory_price, exfactory_price_valid_from,
             wholesale_margin_grp, uniform_wholesale_margin,
             integration_date, valid_from_date,
             valid_thru_date, status_type_code,
             status_type_desc, flag_apd,
             extract_id, extract_id),
        )
        return cur.lastrowid


def upsert_substance(conn, preparation_id, description_la, quantity,
                     quantity_unit):
    """Insert or ignore a substance record for a preparation."""
    if not description_la:
        return
    conn.execute(
        "INSERT OR IGNORE INTO substance "
        "(preparation_id, description_la, quantity, quantity_unit) "
        "VALUES (?, ?, ?, ?)",
        (preparation_id, description_la, quantity, quantity_unit),
    )


def upsert_pack_partner(conn, pack_id_db, partner_type, description,
                        street, zip_code, place, phone):
    """Insert or ignore a pack partner record."""
    if not pack_id_db or not description:
        return
    conn.execute(
        "INSERT OR IGNORE INTO pack_partner "
        "(pack_id_db, partner_type, description, street, zip_code, place, phone) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (pack_id_db, partner_type, description, street, zip_code, place, phone),
    )


def upsert_limitation(conn, extract_id, preparation_id, level, lim_code,
                      lim_type, lim_niveau, name_de, name_fr, name_it,
                      desc_de, desc_fr, desc_it, valid_from, valid_thru):
    """Insert or update a limitation record. Returns limitation_id."""
    c_hash = compute_hash(desc_de, desc_fr, desc_it)

    cur = conn.execute(
        "SELECT limitation_id FROM limitation "
        "WHERE preparation_id = ? AND limitation_code = ? "
        "AND limitation_level = ? AND content_hash = ?",
        (preparation_id, lim_code, level, c_hash),
    )
    row = cur.fetchone()
    if row:
        conn.execute(
            "UPDATE limitation SET last_seen_extract = ? WHERE limitation_id = ?",
            (extract_id, row[0]),
        )
        return row[0]
    else:
        cur = conn.execute(
            "INSERT INTO limitation (preparation_id, limitation_level, limitation_code, "
            "limitation_type, limitation_niveau, "
            "indication_name_de, indication_name_fr, indication_name_it, "
            "description_de, description_fr, description_it, "
            "valid_from_date, valid_thru_date, "
            "first_seen_extract, last_seen_extract, content_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (preparation_id, level, lim_code, lim_type, lim_niveau,
             name_de, name_fr, name_it,
             desc_de, desc_fr, desc_it, valid_from, valid_thru,
             extract_id, extract_id, c_hash),
        )
        return cur.lastrowid


def upsert_indication_code(conn, extract_id, limitation_id, preparation_id,
                           bag_dossier_no, code_value, code_source):
    """Insert or update an indication code record."""
    dossier_part, indication_part = split_code(code_value)

    cur = conn.execute(
        "SELECT indication_code_id FROM indication_code "
        "WHERE limitation_id = ? AND code_value = ?",
        (limitation_id, code_value),
    )
    row = cur.fetchone()
    if row:
        conn.execute(
            "UPDATE indication_code SET last_seen_extract = ? "
            "WHERE indication_code_id = ?",
            (extract_id, row[0]),
        )
    else:
        conn.execute(
            "INSERT INTO indication_code (limitation_id, preparation_id, "
            "bag_dossier_no, code_value, code_source, dossier_part, indication_part, "
            "first_seen_extract, last_seen_extract) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (limitation_id, preparation_id, bag_dossier_no, code_value,
             code_source, dossier_part, indication_part, extract_id, extract_id),
        )


def _detect_cashback_flag(desc_fr):
    """Detect if a French limitation text contains cashback rules. Returns 0 or 1."""
    if not desc_fr:
        return 0
    from cashback_extractor import detect_cashback
    text = html.unescape(desc_fr)
    result = detect_cashback(text)
    return 1 if result['is_cashback'] else 0


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
    else:
        has_cb = _detect_cashback_flag(desc_fr)
        cur = conn.execute(
            "INSERT INTO limitation_text "
            "(content_hash, limitation_code, limitation_type, limitation_niveau, "
            "description_de, description_fr, description_it, has_cashback, "
            "first_seen_extract, last_seen_extract) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (content_hash, lim_code, lim_type, lim_niveau,
             desc_de, desc_fr, desc_it, has_cb, extract_id, extract_id),
        )
        return cur.lastrowid


def upsert_code_link(conn, extract_id, text_id, preparation_id,
                     indication_code, code_source, level):
    """Insert or update a limitation text → indication code link."""
    row = conn.execute(
        "SELECT link_id FROM limitation_code_link "
        "WHERE text_id = ? AND preparation_id = ? AND indication_code = ?",
        (text_id, preparation_id, indication_code),
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE limitation_code_link SET last_seen_extract = ? "
            "WHERE link_id = ?",
            (extract_id, row[0]),
        )
    else:
        conn.execute(
            "INSERT INTO limitation_code_link "
            "(text_id, preparation_id, indication_code, code_source, "
            "limitation_level, first_seen_extract, last_seen_extract) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (text_id, preparation_id, indication_code, code_source,
             level, extract_id, extract_id),
        )


# ============================================================
# Process a single Limitation element
# ============================================================

def process_limitation(conn, extract_id, preparation_id, lim_elem,
                       level, bag_dossier_no):
    """Process one <Limitation> element: store text, names, and extract codes."""
    lim_code = get_text(lim_elem, "LimitationCode")
    lim_type = get_text(lim_elem, "LimitationType")
    lim_niveau = get_text(lim_elem, "LimitationNiveau")
    desc_de = get_text(lim_elem, "DescriptionDe")
    desc_fr = get_text(lim_elem, "DescriptionFr")
    desc_it = get_text(lim_elem, "DescriptionIt")
    valid_from = get_text(lim_elem, "ValidFromDate")
    valid_thru = get_text(lim_elem, "ValidThruDate")

    # Extract bold indication names (only for DIA type)
    name_de, name_fr, name_it = extract_indication_names(
        lim_type, desc_de, desc_fr, desc_it,
    )

    limitation_id = upsert_limitation(
        conn, extract_id, preparation_id, level, lim_code,
        lim_type, lim_niveau, name_de, name_fr, name_it,
        desc_de, desc_fr, desc_it, valid_from, valid_thru,
    )

    # Layer 1: Structured XML codes
    codes = extract_codes_structured(lim_elem)
    source = "STRUCTURED_XML"

    # Layer 2: Free-text parsing
    if not codes:
        codes = extract_codes_from_text(desc_de, desc_fr, desc_it)
        source = "TEXT_PARSED"

    # Layer 3: Fallback with BagDossierNo.xx
    if not codes and lim_type == "DIA" and bag_dossier_no:
        codes = [f"{bag_dossier_no}.xx"]
        source = "FALLBACK_XX"

    for code_value in codes:
        upsert_indication_code(
            conn, extract_id, limitation_id, preparation_id,
            bag_dossier_no, code_value, source,
        )

    # --- Populate limitation_text / limitation_code_link (skip ITCODE) ---
    if level != "ITCODE":
        c_hash = compute_hash(desc_de, desc_fr, desc_it)
        text_id = upsert_limitation_text(
            conn, extract_id, c_hash,
            lim_code, lim_type, lim_niveau,
            desc_de, desc_fr, desc_it,
        )
        for code_value in codes:
            # Normalize fallback codes to uppercase .XX in the new tables
            link_code = code_value
            link_source = source
            if source == "FALLBACK_XX":
                link_code = f"{bag_dossier_no}.XX"
                link_source = "FALLBACK_XX"
            upsert_code_link(
                conn, extract_id, text_id, preparation_id,
                link_code, link_source, level,
            )


# ============================================================
# Process a single Preparation element
# ============================================================

def _get_price(pack_elem, price_path):
    """Extract a price value and its ValidFromDate from a Pack element.

    price_path is e.g. "Prices/PublicPrice" or "Prices/ExFactoryPrice".
    Returns (price_float_or_None, valid_from_str_or_None).
    """
    container = pack_elem.find(price_path)
    if container is None:
        return None, None
    price_text = get_text(container, "Price")
    valid_from = get_text(container, "ValidFromDate")
    if price_text:
        try:
            return float(price_text), valid_from
        except (ValueError, TypeError):
            pass
    return None, valid_from


def process_preparation(conn, extract_id, prep_elem):
    """Process one <Preparation> element with all its packs and limitations."""
    swissmedic_no5 = get_text(prep_elem, "SwissmedicNo5")
    if not swissmedic_no5:
        return

    # --- Preparation-level fields ---
    name_de = get_text(prep_elem, "NameDe")
    name_fr = get_text(prep_elem, "NameFr")
    name_it = get_text(prep_elem, "NameIt")
    prep_desc_de = get_text(prep_elem, "DescriptionDe")
    prep_desc_fr = get_text(prep_elem, "DescriptionFr")
    prep_desc_it = get_text(prep_elem, "DescriptionIt")
    atc_code = get_text(prep_elem, "AtcCode")
    org_gen_code = get_text(prep_elem, "OrgGenCode")
    flag_it_lim = get_text(prep_elem, "FlagItLimitation")
    flag_sb = get_text(prep_elem, "FlagSB")
    flag_ggsl = get_text(prep_elem, "FlagGGSL")
    comment_de = get_text(prep_elem, "CommentDe")
    comment_fr = get_text(prep_elem, "CommentFr")
    comment_it = get_text(prep_elem, "CommentIt")
    vat_in_exf = get_text(prep_elem, "VatInEXF")

    preparation_id = upsert_preparation(
        conn, extract_id, swissmedic_no5, name_de, atc_code,
        name_fr=name_fr, name_it=name_it,
        description_de=prep_desc_de, description_fr=prep_desc_fr,
        description_it=prep_desc_it,
        org_gen_code=org_gen_code, flag_it_limitation=flag_it_lim,
        flag_sb=flag_sb, flag_ggsl=flag_ggsl,
        comment_de=comment_de, comment_fr=comment_fr, comment_it=comment_it,
        vat_in_exf=vat_in_exf,
    )

    # --- Substances ---
    for sub_elem in prep_elem.findall(".//Substances/Substance"):
        upsert_substance(
            conn, preparation_id,
            description_la=get_text(sub_elem, "DescriptionLa"),
            quantity=get_text(sub_elem, "Quantity"),
            quantity_unit=get_text(sub_elem, "QuantityUnit"),
        )

    # Collect all BagDossierNos from packs for fallback
    all_bag_dossier_nos = []

    # Process packs
    for pack_elem in prep_elem.findall(".//Packs/Pack"):
        gtin = get_text(pack_elem, "GTIN")
        swissmedic_no8 = get_text(pack_elem, "SwissmedicNo8")
        bag_dossier_no = get_text(pack_elem, "BagDossierNo")
        pack_desc_de = get_text(pack_elem, "DescriptionDe")
        pack_desc_fr = get_text(pack_elem, "DescriptionFr")
        pack_desc_it = get_text(pack_elem, "DescriptionIt")

        # Additional pack fields
        swissmedic_cat = get_text(pack_elem, "SwissmedicCategory")
        flag_narcosis = get_text(pack_elem, "FlagNarcosis")
        flag_modal = get_text(pack_elem, "FlagModal")
        pk_flag_ggsl = get_text(pack_elem, "FlagGGSL")
        size_pack = get_text(pack_elem, "SizePack")
        prev_gtin = get_text(pack_elem, "PrevGTINcode")
        sm8_parallel = get_text(pack_elem, "SwissmedicNo8ParallelImp")

        # Prices
        pub_price, pub_price_from = _get_price(pack_elem, "Prices/PublicPrice")
        exf_price, exf_price_from = _get_price(pack_elem, "Prices/ExFactoryPrice")

        # Wholesale
        ws_margin_grp = get_text(pack_elem, "WholesaleMarginGrp")
        ws_uniform = get_text(pack_elem, "UniformWholesaleMargin")

        # Status
        status_elem = pack_elem.find("Status")
        pk_integration = get_text(status_elem, "IntegrationDate") if status_elem is not None else None
        pk_valid_from = get_text(status_elem, "ValidFromDate") if status_elem is not None else None
        pk_valid_thru = get_text(status_elem, "ValidThruDate") if status_elem is not None else None
        pk_status_code = get_text(status_elem, "StatusTypeCodeSl") if status_elem is not None else None
        pk_status_desc = get_text(status_elem, "StatusTypeDescriptionSl") if status_elem is not None else None
        pk_flag_apd = get_text(status_elem, "FlagApd") if status_elem is not None else None

        if bag_dossier_no:
            all_bag_dossier_nos.append(bag_dossier_no)

        pack_id = upsert_pack(
            conn, extract_id, preparation_id, gtin, swissmedic_no8,
            bag_dossier_no, pack_desc_de,
            description_fr=pack_desc_fr, description_it=pack_desc_it,
            swissmedic_category=swissmedic_cat,
            flag_narcosis=flag_narcosis, flag_modal=flag_modal,
            flag_ggsl=pk_flag_ggsl,
            size_pack=size_pack, prev_gtin_code=prev_gtin,
            swissmedic_no8_parallel_imp=sm8_parallel,
            public_price=pub_price, public_price_valid_from=pub_price_from,
            exfactory_price=exf_price, exfactory_price_valid_from=exf_price_from,
            wholesale_margin_grp=ws_margin_grp,
            uniform_wholesale_margin=ws_uniform,
            integration_date=pk_integration,
            valid_from_date=pk_valid_from, valid_thru_date=pk_valid_thru,
            status_type_code=pk_status_code, status_type_desc=pk_status_desc,
            flag_apd=pk_flag_apd,
        )

        # Partners
        for partner_elem in pack_elem.findall(".//Partners/Partner"):
            upsert_pack_partner(
                conn, pack_id,
                partner_type=get_text(partner_elem, "PartnerType"),
                description=get_text(partner_elem, "Description"),
                street=get_text(partner_elem, "Street"),
                zip_code=get_text(partner_elem, "ZipCode"),
                place=get_text(partner_elem, "Place"),
                phone=get_text(partner_elem, "Phone"),
            )

        # Pack-level limitations (rare)
        lims = pack_elem.find("Limitations")
        if lims is not None:
            for lim_elem in lims.findall("Limitation"):
                process_limitation(
                    conn, extract_id, preparation_id, lim_elem,
                    "PACK", bag_dossier_no,
                )

    # Use first BagDossierNo for preparation-level fallback
    fallback_bag = all_bag_dossier_nos[0] if all_bag_dossier_nos else None

    # Preparation-level limitations (main source of indication codes)
    prep_lims = prep_elem.find("Limitations")
    if prep_lims is not None:
        for lim_elem in prep_lims.findall("Limitation"):
            process_limitation(
                conn, extract_id, preparation_id, lim_elem,
                "PREPARATION", fallback_bag,
            )

    # ItCode-level limitations
    for itcode_elem in prep_elem.findall(".//ItCodes/ItCode"):
        it_lims = itcode_elem.find("Limitations")
        if it_lims is not None:
            for lim_elem in it_lims.findall("Limitation"):
                process_limitation(
                    conn, extract_id, preparation_id, lim_elem,
                    "ITCODE", fallback_bag,
                )


# ============================================================
# Parse a single XML file
# ============================================================

def get_release_date(file_path):
    """Extract ReleaseDate from the root element without parsing the whole file."""
    for event, elem in ET.iterparse(str(file_path), events=("start",)):
        return elem.get("ReleaseDate", "")


def parse_file(file_path, conn, extract_id):
    """Parse one Preparations XML file using iterparse for memory efficiency."""
    context = ET.iterparse(str(file_path), events=("end",))
    for event, elem in context:
        if elem.tag == "Preparation":
            process_preparation(conn, extract_id, elem)
            elem.clear()


# ============================================================
# File discovery
# ============================================================

def discover_files():
    """Find all Preparations-*.xml files across year directories, sorted chronologically."""
    files = []
    for year_dir in sorted(EXTRACTED_DIR.iterdir()):
        if year_dir.is_dir() and year_dir.name.isdigit():
            for xml_file in sorted(year_dir.glob("Preparations-*.xml")):
                files.append(xml_file)
    return files


# ============================================================
# Phase 3: Build name-to-code mapping table
# ============================================================

def build_name_code_map(conn):
    """Populate indication_name_code_map from limitations with both name and structured code."""
    log.info("Building indication name-to-code mapping table...")
    conn.execute("""
        INSERT OR IGNORE INTO indication_name_code_map
            (indication_name_de, indication_name_fr, indication_name_it,
             code_value, bag_dossier_no, product_name, source_limitation_code)
        SELECT l.indication_name_de, l.indication_name_fr, l.indication_name_it,
               ic.code_value, ic.bag_dossier_no,
               pr.name_de, l.limitation_code
        FROM indication_code ic
        JOIN limitation l ON ic.limitation_id = l.limitation_id
        JOIN preparation pr ON l.preparation_id = pr.preparation_id
        WHERE ic.code_source = 'STRUCTURED_XML'
        AND l.indication_name_de IS NOT NULL
    """)
    count = conn.execute("SELECT COUNT(*) FROM indication_name_code_map").fetchone()[0]
    log.info(f"  -> {count} name-to-code mappings created")
    conn.commit()


# ============================================================
# Phase 4: Retroactive code assignment
# ============================================================

def retroactive_code_assignment(conn):
    """Update FALLBACK_XX codes using the name-to-code mapping table."""
    log.info("Retroactively assigning codes to FALLBACK_XX entries...")

    # Match on indication_name_de + bag_dossier_no
    rows = conn.execute("""
        SELECT ic.indication_code_id, ic.limitation_id, m.code_value
        FROM indication_code ic
        JOIN limitation l ON ic.limitation_id = l.limitation_id
        JOIN indication_name_code_map m
            ON l.indication_name_de = m.indication_name_de
            AND ic.bag_dossier_no = m.bag_dossier_no
        WHERE ic.code_source = 'FALLBACK_XX'
    """).fetchall()

    updated = 0
    deleted_dupes = 0
    for ic_id, lim_id, mapped_code in rows:
        # Check if this limitation already has this code (from STRUCTURED_XML or TEXT_PARSED)
        existing = conn.execute(
            "SELECT 1 FROM indication_code "
            "WHERE limitation_id = ? AND code_value = ? AND indication_code_id != ?",
            (lim_id, mapped_code, ic_id),
        ).fetchone()

        if existing:
            # The real code already exists — just remove the FALLBACK_XX duplicate
            conn.execute(
                "DELETE FROM indication_code WHERE indication_code_id = ?",
                (ic_id,),
            )
            deleted_dupes += 1
        else:
            dossier_part, indication_part = split_code(mapped_code)
            conn.execute(
                "UPDATE indication_code "
                "SET code_value = ?, code_source = 'NAME_MAPPED', "
                "    dossier_part = ?, indication_part = ? "
                "WHERE indication_code_id = ?",
                (mapped_code, dossier_part, indication_part, ic_id),
            )
            updated += 1

    log.info(f"  -> {updated} codes retroactively mapped from names")
    log.info(f"  -> {deleted_dupes} duplicate FALLBACK_XX entries removed")
    conn.commit()


# ============================================================
# Phase 4b: Build indication segments from multi-indication texts
# ============================================================

def build_indication_segments(conn):
    """Split multi-indication limitation texts into per-indication segments."""
    log.info("Building indication segments from multi-indication texts...")

    # Get all DIA limitations that have indication names (bold text),
    # excluding pack-size limitation codes (KLEINPACKUNG etc.)
    placeholders = ",".join("?" for _ in NON_INDICATION_LIM_CODES)
    rows = conn.execute(f"""
        SELECT l.limitation_id, l.preparation_id,
               l.description_de, l.description_fr, l.description_it,
               l.indication_name_de, l.limitation_code
        FROM limitation l
        WHERE l.limitation_type = 'DIA'
        AND l.indication_name_de IS NOT NULL
        AND COALESCE(l.limitation_code, '') NOT IN ({placeholders})
    """, tuple(NON_INDICATION_LIM_CODES)).fetchall()

    total_segments = 0
    skipped_structural = 0
    for lim_id, prep_id, desc_de, desc_fr, desc_it, name_de, lim_code in rows:
        segments = split_limitation_texts(desc_de, desc_fr, desc_it)
        if not segments:
            continue

        # Filter out structural bold names (UND, ODER, Vor Therapiebeginn, etc.)
        # If ALL segments are structural, skip entirely (not a multi-indication text)
        real_segments = [s for s in segments if not _is_structural_name(s["name_de"])]
        skipped_structural += len(segments) - len(real_segments)

        if not real_segments:
            continue  # All structural — not a real indication segmentation

        segments_to_store = real_segments

        # Re-number segment order to be contiguous
        for i, seg in enumerate(segments_to_store):
            conn.execute(
                "INSERT OR IGNORE INTO limitation_indication_segment "
                "(limitation_id, preparation_id, segment_order, "
                " indication_name_de, indication_name_fr, indication_name_it, "
                " segment_text_de, segment_text_fr, segment_text_it) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (lim_id, prep_id, i,
                 seg["name_de"], seg["name_fr"], seg["name_it"],
                 seg["text_de"], seg["text_fr"], seg["text_it"]),
            )
            total_segments += 1

    conn.commit()
    log.info(f"  -> {total_segments} segments created from {len(rows)} limitations")
    log.info(f"  -> {skipped_structural} structural segments filtered out (UND/ODER/etc.)")

    # Stats
    multi = conn.execute("""
        SELECT COUNT(DISTINCT limitation_id) FROM limitation_indication_segment
        WHERE limitation_id IN (
            SELECT limitation_id FROM limitation_indication_segment
            GROUP BY limitation_id HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    log.info(f"  -> {multi} limitations have multiple segments (multi-indication)")


# ============================================================
# Phase 4c: Retroactive mapping using individual segment names
# ============================================================

def retroactive_segment_mapping(conn):
    """Map FALLBACK_XX codes using individual segment names (not concatenated).

    For multi-indication limitations, the concatenated name (e.g.
    "Kolorektalkarzinom | Lungenkarzinom") doesn't match the mapping table.
    But each individual segment name might match.  For each match, we create
    a new indication_code row with the matched code, and annotate the segment.
    """
    log.info("Retroactive mapping using individual segment names...")

    # Find FALLBACK_XX entries whose limitation has segments
    fallback_rows = conn.execute("""
        SELECT ic.indication_code_id, ic.limitation_id, ic.bag_dossier_no,
               ic.code_value
        FROM indication_code ic
        WHERE ic.code_source = 'FALLBACK_XX'
        AND ic.limitation_id IN (
            SELECT DISTINCT limitation_id FROM limitation_indication_segment
        )
    """).fetchall()

    mapped_new = 0
    deleted_fallback = 0

    for ic_id, lim_id, bag_dossier_no, fallback_code in fallback_rows:
        # Get individual segment names for this limitation
        segments = conn.execute("""
            SELECT segment_id, indication_name_de
            FROM limitation_indication_segment
            WHERE limitation_id = ?
            ORDER BY segment_order
        """, (lim_id,)).fetchall()

        matched_codes = []
        for seg_id, seg_name_de in segments:
            if not seg_name_de or _is_structural_name(seg_name_de):
                continue
            # Look up this individual name in the mapping table
            mapping = conn.execute("""
                SELECT code_value FROM indication_name_code_map
                WHERE indication_name_de = ?
                AND bag_dossier_no = ?
            """, (seg_name_de, bag_dossier_no)).fetchone()

            if mapping:
                matched_code = mapping[0]
                matched_codes.append((seg_id, seg_name_de, matched_code))

        if not matched_codes:
            continue

        # We found codes for some segments. Insert new indication_code rows
        # for each matched code, then delete the original FALLBACK_XX.
        for seg_id, seg_name_de, code_value in matched_codes:
            dossier_part, indication_part = split_code(code_value)

            # Check if this code already exists for this limitation
            existing = conn.execute(
                "SELECT 1 FROM indication_code "
                "WHERE limitation_id = ? AND code_value = ?",
                (lim_id, code_value),
            ).fetchone()

            if not existing:
                prep_id = conn.execute(
                    "SELECT preparation_id FROM limitation WHERE limitation_id = ?",
                    (lim_id,),
                ).fetchone()[0]
                conn.execute(
                    "INSERT INTO indication_code "
                    "(limitation_id, preparation_id, bag_dossier_no, "
                    " code_value, code_source, dossier_part, indication_part, "
                    " first_seen_extract, last_seen_extract) "
                    "SELECT ?, ?, ?, ?, 'SEGMENT_MAPPED', ?, ?, "
                    "       first_seen_extract, last_seen_extract "
                    "FROM indication_code WHERE indication_code_id = ?",
                    (lim_id, prep_id, bag_dossier_no,
                     code_value, dossier_part, indication_part, ic_id),
                )
                mapped_new += 1

            # Annotate the segment with the matched code
            conn.execute(
                "UPDATE limitation_indication_segment "
                "SET matched_code_value = ?, matched_code_source = 'SEGMENT_MAPPED' "
                "WHERE segment_id = ?",
                (code_value, seg_id),
            )

        # Delete the original FALLBACK_XX entry since we've mapped individual codes
        conn.execute(
            "DELETE FROM indication_code WHERE indication_code_id = ?",
            (ic_id,),
        )
        deleted_fallback += 1

    conn.commit()
    log.info(f"  -> {mapped_new} new indication codes from segment name matching")
    log.info(f"  -> {deleted_fallback} FALLBACK_XX entries replaced")

    # Also annotate segments for limitations that already have structured codes
    annotated = _annotate_segments_with_existing_codes(conn)
    log.info(f"  -> {annotated} segments annotated with existing structured codes")


def _annotate_segments_with_existing_codes(conn):
    """For segments whose limitation already has structured/text-parsed codes,
    try to match individual segment names to those codes via the mapping table."""
    rows = conn.execute("""
        SELECT s.segment_id, s.limitation_id, s.indication_name_de
        FROM limitation_indication_segment s
        WHERE s.matched_code_value IS NULL
        AND s.indication_name_de IS NOT NULL
    """).fetchall()

    annotated = 0
    for seg_id, lim_id, seg_name_de in rows:
        if _is_structural_name(seg_name_de):
            continue
        # Look for a code on this same limitation that corresponds to this name
        # via the mapping table (match on name, ignoring dossier since it's same limitation)
        mapping = conn.execute("""
            SELECT m.code_value
            FROM indication_name_code_map m
            WHERE m.indication_name_de = ?
            AND m.code_value IN (
                SELECT code_value FROM indication_code WHERE limitation_id = ?
            )
        """, (seg_name_de, lim_id)).fetchone()

        if mapping:
            conn.execute(
                "UPDATE limitation_indication_segment "
                "SET matched_code_value = ?, matched_code_source = 'EXISTING' "
                "WHERE segment_id = ?",
                (mapping[0], seg_id),
            )
            annotated += 1

    conn.commit()
    return annotated


# ============================================================
# Phase 4d: Similarity matching for unmatched segments
# ============================================================

# Brand-name canonical mapping: biosimilars/generics -> canonical form
# Sorted by length at runtime so longer names match first
BRAND_CANONICAL = {
    # Lenalidomide
    "LENALIDOMID SPIRIG HC": "LENALIDOMID", "LENALIDOMID SANDOZ": "LENALIDOMID",
    "LENALIDOMID-TEVA": "LENALIDOMID", "LENALIDOMID ZENTIVA": "LENALIDOMID",
    "LENALIDOMID VIATRIS": "LENALIDOMID", "LENALIDOMID DEVATIS": "LENALIDOMID",
    "LENALIDOMID ACCORD": "LENALIDOMID", "LENALIDOMID BMS": "LENALIDOMID",
    "LENALIDOMID Spirig": "LENALIDOMID", "LENALIDOMID Viatris": "LENALIDOMID",
    "LÉNALIDOMIDE DEVATIS": "LENALIDOMID", "Lenalidomid Spirig": "LENALIDOMID",
    "REVLIMID": "LENALIDOMID",
    # Pomalidomide
    "POMALIDOMID SPIRIG HC": "POMALIDOMID", "POMALIDOMID SANDOZ": "POMALIDOMID",
    "POMALIDOMID-TEVA": "POMALIDOMID", "POMALIDOMID ZENTIVA": "POMALIDOMID",
    "POMALIDOMID ACCORD": "POMALIDOMID", "IMNOVID": "POMALIDOMID",
    # Azacitidine
    "AZACITIDIN SPIRIG HC": "AZACITIDIN", "AZACITIDIN ACCORD": "AZACITIDIN",
    "AZACITIDIN MYLAN": "AZACITIDIN", "AZACITIDIN SANDOZ": "AZACITIDIN",
    "AZACITIDIN STADA": "AZACITIDIN", "AZACITIDIN VIATRIS": "AZACITIDIN",
    "AZACITIDIN IDEOGEN": "AZACITIDIN", "VIDAZA": "AZACITIDIN",
    # Decitabine
    "DECITABIN ACCORD": "DECITABIN", "DECITABIN IDEOGEN": "DECITABIN",
    "DECITABIN SANDOZ": "DECITABIN",
    # Trastuzumab biosimilars
    "OGIVRI": "TRASTUZUMAB", "TRAZIMERA": "TRASTUZUMAB", "KANJINTI": "TRASTUZUMAB",
    "HERZUMA": "TRASTUZUMAB", "HERCEPTIN": "TRASTUZUMAB", "ZERCEPAC": "TRASTUZUMAB",
    # Bevacizumab biosimilars
    "OYAVAS": "BEVACIZUMAB", "ZIRABEV": "BEVACIZUMAB", "AVASTIN": "BEVACIZUMAB",
    # Rituximab biosimilars
    "TRUXIMA": "RITUXIMAB", "RIXATHON": "RITUXIMAB",
    # Daratumumab
    "DARZALEX SC": "DARZALEX", "Darzalex": "DARZALEX",
    # Carfilzomib
    "Kyprolis": "KYPROLIS",
    # INN (generic substance names) -> brand canonical
    "Daratumumab": "DARZALEX",
    "Nivolumab": "OPDIVO",
    "Pembrolizumab": "KEYTRUDA",
    "Trastuzumab": "HERCEPTIN",
    "Bevacizumab": "AVASTIN",
    "Rituximab": "MABTHERA",
}
# Pre-sort by length (longest first) so multi-word brands match before shorter ones
_BRAND_SORTED = sorted(BRAND_CANONICAL.items(), key=lambda x: -len(x[0]))


def _normalize_indication_name(name):
    """Normalize an indication name for fuzzy comparison."""
    if not name:
        return ""
    return re.sub(r"\s+", " ", name.strip().rstrip(":").strip())


def _normalize_brands(name):
    """Replace brand-specific names with canonical generic form."""
    result = name
    for brand, canonical in _BRAND_SORTED:
        result = result.replace(brand, canonical)
    return result


def _normalize_kombination(name):
    """Normalize 'Kombination BRAND, X und Y' -> 'BRAND in Kombination mit X und Y'.

    Old BAG texts (e.g. Revlimid) use 'Kombination REVLIMID, Elotuzumab und Dexamethason'
    while newer texts use 'REVLIMID in Kombination mit Elotuzumab und Dexamethason'.
    Also handles 'Kombination VIDAZA und Venetoclax' (no comma).
    """
    m = re.match(r"^Kombination\s+(\S+),?\s*(.*)", name)
    if m:
        brand = m.group(1).rstrip(",")
        rest = m.group(2)
        return f"{brand} in Kombination mit {rest}"
    return name


def similarity_segment_mapping(conn):
    """Phase 4d: Multi-layered similarity matching for unmatched segments."""
    log.info("Phase 4d: Similarity matching for unmatched segments...")

    # Collect all unmatched segments with their dossier info.
    # Use a subquery to pick ONE dossier per preparation (avoid pack-join duplication).
    unmatched = conn.execute("""
        SELECT s.segment_id, s.limitation_id, s.indication_name_de,
               (SELECT pk.bag_dossier_no FROM pack pk
                WHERE pk.preparation_id = s.preparation_id LIMIT 1) AS bag_dossier_no
        FROM limitation_indication_segment s
        WHERE s.matched_code_value IS NULL
        AND s.indication_name_de IS NOT NULL
    """).fetchall()
    log.info(f"  {len(unmatched)} unmatched segments to process")

    # Collect mapping table entries (single-name only, no concatenated)
    mapping = conn.execute("""
        SELECT indication_name_de, code_value, bag_dossier_no
        FROM indication_name_code_map
        WHERE indication_name_de NOT LIKE '%|%'
    """).fetchall()

    # Also collect pipe-containing entries for pipe-part matching (S1+S2)
    mapping_piped = conn.execute("""
        SELECT indication_name_de, code_value, bag_dossier_no
        FROM indication_name_code_map
        WHERE indication_name_de LIKE '%|%'
    """).fetchall()

    # Build lookup structures
    map_by_dossier = defaultdict(list)  # dossier -> [(name, code)]
    map_by_norm_name = defaultdict(list)  # normalized_name -> [(code, dossier)]
    map_by_brand_norm = defaultdict(list)  # brand_normalized -> [(code, dossier)]
    # Pipe-part lookup: normalized_part -> [(code, dossier)]
    map_by_pipe_part = defaultdict(list)

    for name, code, bag in mapping:
        map_by_dossier[bag].append((name, code))
        norm = _normalize_indication_name(name).lower()
        map_by_norm_name[norm].append((code, bag))
        brand_norm = _normalize_brands(_normalize_indication_name(name)).lower()
        map_by_brand_norm[brand_norm].append((code, bag))
        # Also index "Kombination"-normalized forms
        kombi_norm = _normalize_brands(_normalize_kombination(
            _normalize_indication_name(name))).lower()
        if kombi_norm != brand_norm:
            map_by_brand_norm[kombi_norm].append((code, bag))

    # Build pipe-part index from piped entries
    for name, code, bag in mapping_piped:
        parts = name.split("|")
        for part in parts:
            part_norm = _normalize_indication_name(part).lower()
            if part_norm:
                map_by_pipe_part[part_norm].append((code, bag))
                # Also index brand-normalized variant
                part_brand = _normalize_brands(
                    _normalize_indication_name(part)).lower()
                if part_brand != part_norm:
                    map_by_pipe_part[part_brand].append((code, bag))

    match_log = []  # (seg_id, seg_name, matched_code, match_type, matched_to, score)
    matched_ids = set()

    # --- Layer 1: Embedded code extraction ---
    log.info("  Layer 1: Embedded code extraction...")
    layer1_count = 0
    for seg_id, lim_id, name_de, bag in unmatched:
        code_match = RE_NUMERIC.search(name_de)
        if code_match:
            code_value = code_match.group(1)
            conn.execute(
                "UPDATE limitation_indication_segment "
                "SET matched_code_value = ?, matched_code_source = 'EMBEDDED_IN_NAME' "
                "WHERE segment_id = ?",
                (code_value, seg_id),
            )
            matched_ids.add(seg_id)
            match_log.append((seg_id, name_de, code_value, "EMBEDDED_IN_NAME", name_de, 1.0))
            layer1_count += 1
    log.info(f"    -> {layer1_count} segments matched via embedded codes")

    # --- Layer 2: Text normalization ---
    log.info("  Layer 2: Text normalization matching...")
    layer2_count = 0
    for seg_id, lim_id, name_de, bag in unmatched:
        if seg_id in matched_ids:
            continue
        norm = _normalize_indication_name(name_de).lower()

        # 2a: Same-dossier normalized match
        matched = False
        for map_code, map_bag in map_by_norm_name.get(norm, []):
            if map_bag == bag:
                conn.execute(
                    "UPDATE limitation_indication_segment "
                    "SET matched_code_value = ?, matched_code_source = 'NORMALIZED_MATCH' "
                    "WHERE segment_id = ?",
                    (map_code, seg_id),
                )
                matched_ids.add(seg_id)
                match_log.append((seg_id, name_de, map_code, "NORMALIZED_MATCH", f"same dossier {bag}", 1.0))
                layer2_count += 1
                matched = True
                break

        if matched:
            continue

        # 2b: Cross-dossier — only assign if the indication_part (.XX) is unique
        # across all dossiers for this name. Use the segment's own dossier prefix.
        candidates = map_by_norm_name.get(norm, [])
        if candidates:
            # Extract just the indication parts (.XX) from all candidate codes
            indication_parts = set(c.split(".")[1] if "." in c else c for c, _ in candidates)
            if len(indication_parts) == 1:
                ind_part = indication_parts.pop()
                # Build code using segment's own dossier + the matched indication part
                if bag:
                    code_value = f"{bag}.{ind_part}"
                    conn.execute(
                        "UPDATE limitation_indication_segment "
                        "SET matched_code_value = ?, matched_code_source = 'NORMALIZED_CROSS' "
                        "WHERE segment_id = ?",
                        (code_value, seg_id),
                    )
                    matched_ids.add(seg_id)
                    ref_code = candidates[0][0]
                    match_log.append((seg_id, name_de, code_value, "NORMALIZED_CROSS",
                                     f"ind_part .{ind_part} from {ref_code}", 0.95))
                    layer2_count += 1

    log.info(f"    -> {layer2_count} segments matched via normalization")

    # --- Layer 2c: Pipe-part matching ---
    # Map entries with "|" (e.g. "Name1 | Name2") are split; match against each part.
    # Also applies "Kombination" normalization to segment names.
    log.info("  Layer 2c: Pipe-part and Kombination matching...")
    layer2c_count = 0
    for seg_id, lim_id, name_de, bag in unmatched:
        if seg_id in matched_ids:
            continue
        seg_norm = _normalize_indication_name(name_de).lower()
        # Also try Kombination-normalized form
        seg_kombi = _normalize_brands(_normalize_kombination(
            _normalize_indication_name(name_de))).lower()

        # Try each variant against pipe-part index
        for variant in (seg_norm, seg_kombi):
            if variant in map_by_pipe_part:
                # 2c-a: Same-dossier pipe-part match
                matched = False
                for map_code, map_bag in map_by_pipe_part[variant]:
                    if map_bag == bag:
                        conn.execute(
                            "UPDATE limitation_indication_segment "
                            "SET matched_code_value = ?, matched_code_source = 'PIPE_PART_MATCH' "
                            "WHERE segment_id = ?",
                            (map_code, seg_id),
                        )
                        matched_ids.add(seg_id)
                        match_log.append((seg_id, name_de, map_code, "PIPE_PART_MATCH",
                                         f"same dossier {bag}", 0.95))
                        layer2c_count += 1
                        matched = True
                        break
                if matched:
                    break

                # 2c-b: Cross-dossier pipe-part — only if indication_part is unique
                candidates = map_by_pipe_part[variant]
                indication_parts = set(
                    c.split(".")[1] if "." in c else c for c, _ in candidates)
                if len(indication_parts) == 1 and bag:
                    ind_part = indication_parts.pop()
                    code_value = f"{bag}.{ind_part}"
                    conn.execute(
                        "UPDATE limitation_indication_segment "
                        "SET matched_code_value = ?, matched_code_source = 'PIPE_PART_CROSS' "
                        "WHERE segment_id = ?",
                        (code_value, seg_id),
                    )
                    matched_ids.add(seg_id)
                    ref_code = candidates[0][0]
                    match_log.append((seg_id, name_de, code_value, "PIPE_PART_CROSS",
                                     f"ind_part .{ind_part} from {ref_code}", 0.90))
                    layer2c_count += 1
                    break

        # Also try Kombination-normalized against standard map (not just pipe)
        if seg_id not in matched_ids and seg_kombi != seg_norm:
            # Same-dossier
            matched = False
            for map_code, map_bag in map_by_brand_norm.get(seg_kombi, []):
                if map_bag == bag:
                    conn.execute(
                        "UPDATE limitation_indication_segment "
                        "SET matched_code_value = ?, matched_code_source = 'KOMBI_NORMALIZED' "
                        "WHERE segment_id = ?",
                        (map_code, seg_id),
                    )
                    matched_ids.add(seg_id)
                    match_log.append((seg_id, name_de, map_code, "KOMBI_NORMALIZED",
                                     f"same dossier {bag}", 0.90))
                    layer2c_count += 1
                    matched = True
                    break

            # Cross-dossier
            if not matched:
                candidates = map_by_brand_norm.get(seg_kombi, [])
                if candidates:
                    indication_parts = set(
                        c.split(".")[1] if "." in c else c for c, _ in candidates)
                    if len(indication_parts) == 1 and bag:
                        ind_part = indication_parts.pop()
                        code_value = f"{bag}.{ind_part}"
                        conn.execute(
                            "UPDATE limitation_indication_segment "
                            "SET matched_code_value = ?, matched_code_source = 'KOMBI_CROSS' "
                            "WHERE segment_id = ?",
                            (code_value, seg_id),
                        )
                        matched_ids.add(seg_id)
                        ref_code = candidates[0][0]
                        match_log.append((seg_id, name_de, code_value, "KOMBI_CROSS",
                                         f"ind_part .{ind_part} from {ref_code}", 0.85))
                        layer2c_count += 1

    log.info(f"    -> {layer2c_count} segments matched via pipe-part/Kombination")

    # --- Layer 3: Brand name normalization ---
    log.info("  Layer 3: Brand name normalization...")
    layer3_count = 0
    for seg_id, lim_id, name_de, bag in unmatched:
        if seg_id in matched_ids:
            continue
        brand_norm = _normalize_brands(_normalize_indication_name(name_de)).lower()

        # 3a: Same-dossier brand-normalized match
        matched = False
        for map_code, map_bag in map_by_brand_norm.get(brand_norm, []):
            if map_bag == bag:
                conn.execute(
                    "UPDATE limitation_indication_segment "
                    "SET matched_code_value = ?, matched_code_source = 'BRAND_NORMALIZED' "
                    "WHERE segment_id = ?",
                    (map_code, seg_id),
                )
                matched_ids.add(seg_id)
                match_log.append((seg_id, name_de, map_code, "BRAND_NORMALIZED", f"same dossier {bag}", 0.9))
                layer3_count += 1
                matched = True
                break

        if matched:
            continue

        # 3b: Cross-dossier brand match — only if indication_part is unique.
        # Use the segment's own dossier prefix.
        candidates = map_by_brand_norm.get(brand_norm, [])
        if candidates:
            indication_parts = set(c.split(".")[1] if "." in c else c for c, _ in candidates)
            if len(indication_parts) == 1:
                ind_part = indication_parts.pop()
                if bag:
                    code_value = f"{bag}.{ind_part}"
                    conn.execute(
                        "UPDATE limitation_indication_segment "
                        "SET matched_code_value = ?, matched_code_source = 'BRAND_CROSS' "
                        "WHERE segment_id = ?",
                        (code_value, seg_id),
                    )
                    matched_ids.add(seg_id)
                    ref_code = candidates[0][0]
                    match_log.append((seg_id, name_de, code_value, "BRAND_CROSS",
                                     f"ind_part .{ind_part} from {ref_code}", 0.85))
                    layer3_count += 1

    log.info(f"    -> {layer3_count} segments matched via brand normalization")

    # --- Layer 4: Fuzzy SequenceMatcher (same-dossier only) ---
    log.info("  Layer 4: Fuzzy matching (same-dossier, ratio >= 0.90)...")
    layer4_count = 0
    for seg_id, lim_id, name_de, bag in unmatched:
        if seg_id in matched_ids:
            continue
        if bag not in map_by_dossier:
            continue

        seg_norm = _normalize_indication_name(name_de).lower()
        best_ratio = 0
        best_code = None
        best_map_name = None
        second_best = 0

        for map_name, map_code in map_by_dossier[bag]:
            map_norm = _normalize_indication_name(map_name).lower()
            ratio = SequenceMatcher(None, seg_norm, map_norm).ratio()
            if ratio > best_ratio:
                second_best = best_ratio
                best_ratio = ratio
                best_code = map_code
                best_map_name = map_name
            elif ratio > second_best:
                second_best = ratio

        # Accept high-confidence matches with clear gap,
        # OR prefix matches with ratio >= 0.92 (relaxed gap for suffix-only diffs)
        best_map_norm = _normalize_indication_name(best_map_name).lower() if best_map_name else ""
        is_prefix = (best_map_norm.startswith(seg_norm) or seg_norm.startswith(best_map_norm))
        gap_ok = (best_ratio - second_best) >= 0.05
        prefix_ok = is_prefix and best_ratio >= 0.92
        if best_ratio >= 0.90 and (gap_ok or prefix_ok):
            conn.execute(
                "UPDATE limitation_indication_segment "
                "SET matched_code_value = ?, matched_code_source = 'FUZZY_MATCHED' "
                "WHERE segment_id = ?",
                (best_code, seg_id),
            )
            matched_ids.add(seg_id)
            match_log.append((seg_id, name_de, best_code, "FUZZY_MATCHED", best_map_name, best_ratio))
            layer4_count += 1

    log.info(f"    -> {layer4_count} segments matched via fuzzy matching")

    # --- Layer 5: Single-segment-single-code deduction ---
    log.info("  Layer 5: Single-segment-single-code deduction...")
    layer5_count = 0

    for seg_id, lim_id, name_de, bag in unmatched:
        if seg_id in matched_ids:
            continue
        # Count total segments for this limitation
        total_segs = conn.execute(
            "SELECT COUNT(*) FROM limitation_indication_segment WHERE limitation_id = ?",
            (lim_id,)
        ).fetchone()[0]
        if total_segs != 1:
            continue
        # Get non-FALLBACK codes
        codes = conn.execute(
            "SELECT code_value FROM indication_code "
            "WHERE limitation_id = ? AND code_value NOT LIKE 'FALLBACK%%'",
            (lim_id,)
        ).fetchall()
        if len(codes) != 1:
            continue
        code_value = codes[0][0]
        conn.execute(
            "UPDATE limitation_indication_segment "
            "SET matched_code_value = ?, matched_code_source = 'SINGLE_SEGMENT_CODE' "
            "WHERE segment_id = ?",
            (code_value, seg_id),
        )
        matched_ids.add(seg_id)
        match_log.append((seg_id, name_de, code_value, "SINGLE_SEGMENT_CODE",
                          f"only code for lim {lim_id}", 1.0))
        layer5_count += 1

    log.info(f"    -> {layer5_count} segments matched via single-segment-single-code deduction")

    # --- Layer 6: Positional ordinal matching (N segments = N codes) ---
    log.info("  Layer 6: Positional ordinal matching...")
    layer6_count = 0

    unmatched_by_lim = defaultdict(list)
    for seg_id, lim_id, name_de, bag in unmatched:
        if seg_id in matched_ids:
            continue
        unmatched_by_lim[lim_id].append((seg_id, name_de, bag))

    for lim_id, segs in unmatched_by_lim.items():
        if len(segs) < 2:
            continue
        # Only if ALL segments for this limitation are unmatched
        total_segs = conn.execute(
            "SELECT COUNT(*) FROM limitation_indication_segment WHERE limitation_id = ?",
            (lim_id,)
        ).fetchone()[0]
        if total_segs != len(segs):
            continue
        # Get non-FALLBACK codes ordered
        codes = conn.execute(
            "SELECT code_value FROM indication_code "
            "WHERE limitation_id = ? AND code_value NOT LIKE 'FALLBACK%%' "
            "ORDER BY code_value",
            (lim_id,)
        ).fetchall()
        if len(codes) != len(segs):
            continue
        # Sort segments by segment_order
        seg_orders = []
        for seg_id, name_de, bag in segs:
            order = conn.execute(
                "SELECT segment_order FROM limitation_indication_segment WHERE segment_id = ?",
                (seg_id,)
            ).fetchone()[0]
            seg_orders.append((order, seg_id, name_de))
        seg_orders.sort()

        for i, (order, seg_id, name_de) in enumerate(seg_orders):
            code_value = codes[i][0]
            conn.execute(
                "UPDATE limitation_indication_segment "
                "SET matched_code_value = ?, matched_code_source = 'ORDINAL_POSITION' "
                "WHERE segment_id = ?",
                (code_value, seg_id),
            )
            matched_ids.add(seg_id)
            match_log.append((seg_id, name_de, code_value, "ORDINAL_POSITION",
                              f"position {i} of {len(segs)} in lim {lim_id}", 0.80))
            layer6_count += 1

    log.info(f"    -> {layer6_count} segments matched via positional ordinal matching")

    conn.commit()

    # Store match log for export
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _similarity_match_log (
            segment_id INTEGER, segment_name TEXT, matched_code TEXT,
            match_type TEXT, matched_to TEXT, score REAL
        )
    """)
    conn.execute("DELETE FROM _similarity_match_log")
    conn.executemany(
        "INSERT INTO _similarity_match_log VALUES (?, ?, ?, ?, ?, ?)",
        match_log,
    )
    conn.commit()

    total = len(matched_ids)
    remaining = len(unmatched) - total
    log.info(f"  Total similarity matches: {total} ({layer1_count} embedded + "
             f"{layer2_count} normalized + {layer2c_count} pipe/kombi + "
             f"{layer3_count} brand + {layer4_count} fuzzy + "
             f"{layer5_count} single-code + {layer6_count} ordinal)")
    log.info(f"  Remaining unmatched: {remaining}")


# ============================================================
# HTML cleaning for CSV export
# ============================================================

_RE_BR = re.compile(r"<br\s*/?>")
_RE_TAG = re.compile(r"<[^>]+>")


def _clean_html(text):
    """Strip HTML tags for readable CSV export. Tags removed, no newlines."""
    if not isinstance(text, str):
        return text
    text = _RE_BR.sub(" ", text)
    text = _RE_TAG.sub("", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = text.replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"  +", " ", text)
    return text.strip()


def _clean_html_columns(df, columns):
    """Apply _clean_html to specified columns of a DataFrame."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(_clean_html)
    return df


# ============================================================
# Phase 6: Cashback extraction (segment-level)
# ============================================================

def run_cashback_extraction(conn):
    """Run cashback extraction at segment level using cashback_extractor.

    For segmented limitations: each segment's text_fr is analyzed individually.
    For unsegmented limitations: the full description_fr is used.
    Results go into cashback_segment and cashback tables respectively.
    """
    # Import cashback_extractor (sibling file in same directory)
    sys.path.insert(0, str(BASE_DIR))
    from cashback_extractor import (
        CashbackExtractor, clean_html, detect_cashback, extract_cashback_sentence,
        extract_calculation, extract_unit, extract_threshold, extract_conditions,
        extract_cotreatments, ReferenceDataLoader,
    )

    db_path = str(DB_PATH)

    # Use a separate connection via the extractor (it manages its own)
    extractor = CashbackExtractor(db_path=db_path)

    try:
        extractor.process_segments(dry_run=False, verbose=False)

        # Log summary
        seg_count = len(getattr(extractor, 'segment_results', []))
        lim_count = len(getattr(extractor, 'results', []))
        log.info(f"Cashback extraction: {seg_count} segment-level + {lim_count} limitation-level")
    finally:
        extractor.close()

    # Re-attach the tables to our existing connection by re-reading counts
    seg_total = conn.execute(
        "SELECT COUNT(*) FROM cashback_segment"
    ).fetchone()[0]
    lim_total = conn.execute(
        "SELECT COUNT(*) FROM cashback"
    ).fetchone()[0]
    log.info(f"  -> cashback_segment: {seg_total} rows, cashback: {lim_total} rows")


# ============================================================
# Main
# ============================================================

def main():
    log.info("=" * 60)
    log.info("BAG Preparations - Limitation & Indication Code Extractor")
    log.info("=" * 60)

    # Phase 1: Clean start - delete and rebuild database
    if DB_PATH.exists():
        DB_PATH.unlink()
        log.info("Deleted existing database")

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA_SQL)
    log.info("Created fresh database")

    # Phase 2: XML ingestion
    log.info("-" * 60)
    log.info("PHASE 2: XML Ingestion")
    log.info("-" * 60)

    files = discover_files()
    log.info(f"Found {len(files)} Preparations XML files")

    for i, file_path in enumerate(files, 1):
        file_name = file_path.name
        log.info(f"[{i}/{len(files)}] Processing {file_name}...")

        release_date = get_release_date(file_path)
        file_year = int(file_name.split("-")[1][:4])

        cur = conn.execute(
            "INSERT INTO extract (file_name, release_date, file_year) VALUES (?, ?, ?)",
            (file_name, release_date, file_year),
        )
        extract_id = cur.lastrowid

        parse_file(file_path, conn, extract_id)
        conn.commit()

        # Log progress stats
        prep_count = conn.execute(
            "SELECT COUNT(*) FROM preparation WHERE last_seen_extract = ?",
            (extract_id,),
        ).fetchone()[0]
        code_count = conn.execute(
            "SELECT COUNT(*) FROM indication_code WHERE last_seen_extract = ?",
            (extract_id,),
        ).fetchone()[0]
        log.info(f"  -> {prep_count} preparations, {code_count} indication codes")

    # Ingestion stats
    total_preps = conn.execute("SELECT COUNT(*) FROM preparation").fetchone()[0]
    total_packs = conn.execute("SELECT COUNT(*) FROM pack").fetchone()[0]
    total_lims = conn.execute("SELECT COUNT(*) FROM limitation").fetchone()[0]
    total_codes = conn.execute("SELECT COUNT(*) FROM indication_code").fetchone()[0]
    named_lims = conn.execute(
        "SELECT COUNT(*) FROM limitation WHERE indication_name_de IS NOT NULL"
    ).fetchone()[0]
    log.info(f"Ingestion totals: {total_preps} preparations, {total_packs} packs, "
             f"{total_lims} limitations ({named_lims} with indication names), "
             f"{total_codes} indication codes")

    # Code source breakdown before mapping
    log.info("Code sources before retroactive mapping:")
    for row in conn.execute(
        "SELECT code_source, COUNT(*) FROM indication_code GROUP BY code_source ORDER BY COUNT(*) DESC"
    ):
        log.info(f"  {row[0]}: {row[1]}")

    # Phase 3: Build name-to-code mapping
    log.info("-" * 60)
    log.info("PHASE 3: Build Name-to-Code Mapping")
    log.info("-" * 60)
    build_name_code_map(conn)

    # Phase 4: Retroactive code assignment (single-name matching)
    log.info("-" * 60)
    log.info("PHASE 4: Retroactive Code Assignment (single-name)")
    log.info("-" * 60)
    retroactive_code_assignment(conn)

    # Phase 4b: Build indication segments
    log.info("-" * 60)
    log.info("PHASE 4b: Build Indication Segments")
    log.info("-" * 60)
    build_indication_segments(conn)

    # Phase 4c: Retroactive mapping using individual segment names
    log.info("-" * 60)
    log.info("PHASE 4c: Retroactive Segment-Level Mapping")
    log.info("-" * 60)
    retroactive_segment_mapping(conn)

    # Phase 4d: Similarity matching for unmatched segments
    log.info("-" * 60)
    log.info("PHASE 4d: Similarity Matching")
    log.info("-" * 60)
    similarity_segment_mapping(conn)

    # Final code source breakdown
    log.info("Code sources after all mapping phases:")
    for row in conn.execute(
        "SELECT code_source, COUNT(*) FROM indication_code GROUP BY code_source ORDER BY COUNT(*) DESC"
    ):
        log.info(f"  {row[0]}: {row[1]}")

    # Phase 5: Export
    log.info("-" * 60)
    log.info("PHASE 5: Export")
    log.info("-" * 60)

    # Export main view
    df = pd.read_sql("SELECT * FROM v_sku_indications", conn)
    csv_path = BASE_DIR / "sku_indication_codes.csv"
    xlsx_path = BASE_DIR / "sku_indication_codes.xlsx"
    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    log.info(f"Exported {len(df)} rows to {csv_path.name} and {xlsx_path.name}")

    # Export mapping table
    df_map = pd.read_sql("SELECT * FROM indication_name_code_map", conn)
    map_csv = BASE_DIR / "indication_name_code_map.csv"
    df_map.to_csv(map_csv, index=False)
    log.info(f"Exported {len(df_map)} name-to-code mappings to {map_csv.name}")

    # Export segments table
    df_seg = pd.read_sql("""
        SELECT s.segment_id, s.limitation_id, s.segment_order,
               pr.name_de AS product_name, l.limitation_code,
               s.indication_name_de, s.indication_name_fr, s.indication_name_it,
               s.segment_text_de, s.segment_text_fr, s.segment_text_it,
               s.matched_code_value, s.matched_code_source
        FROM limitation_indication_segment s
        JOIN limitation l ON s.limitation_id = l.limitation_id
        JOIN preparation pr ON s.preparation_id = pr.preparation_id
        ORDER BY pr.name_de, l.limitation_code, s.segment_order
    """, conn)
    seg_csv = BASE_DIR / "limitation_indication_segments.csv"
    df_seg.to_csv(seg_csv, index=False)
    log.info(f"Exported {len(df_seg)} indication segments to {seg_csv.name}")

    # Export similarity match log
    df_log = pd.read_sql("SELECT * FROM _similarity_match_log", conn)
    log_csv = BASE_DIR / "similarity_match_log.csv"
    df_log.to_csv(log_csv, index=False)
    log.info(f"Exported {len(df_log)} similarity match entries to {log_csv.name}")

    # Export unmatched indication names
    df_unmatched = pd.read_sql("""
        SELECT s.indication_name_de, s.indication_name_fr, s.indication_name_it,
               pr.name_de AS product_name, pk.bag_dossier_no,
               COUNT(*) AS segment_count
        FROM limitation_indication_segment s
        JOIN preparation pr ON s.preparation_id = pr.preparation_id
        LEFT JOIN pack pk ON pk.preparation_id = s.preparation_id
        WHERE s.matched_code_value IS NULL
        AND s.indication_name_de IS NOT NULL
        GROUP BY s.indication_name_de, pk.bag_dossier_no
        ORDER BY segment_count DESC, s.indication_name_de
    """, conn)
    unmatched_csv = BASE_DIR / "unmatched_indication_names.csv"
    df_unmatched.to_csv(unmatched_csv, index=False)
    log.info(f"Exported {len(df_unmatched)} unmatched indication names to {unmatched_csv.name}")

    # Phase 6: Cashback extraction (segment-level)
    log.info("-" * 60)
    log.info("PHASE 6: Cashback Extraction (segment-level)")
    log.info("-" * 60)
    run_cashback_extraction(conn)

    # Export cashback results
    df_cb_seg = pd.read_sql("""
        SELECT cs.*, s.indication_name_fr, s.segment_order
        FROM cashback_segment cs
        JOIN limitation_indication_segment s ON cs.segment_id = s.segment_id
        ORDER BY cs.product_name, cs.limitation_code, s.segment_order
    """, conn)
    cb_seg_csv = BASE_DIR / "cashback_segments.csv"
    df_cb_seg.to_csv(cb_seg_csv, index=False)
    log.info(f"Exported {len(df_cb_seg)} segment-level cashback entries to {cb_seg_csv.name}")

    df_cb_lim = pd.read_sql("SELECT * FROM cashback ORDER BY product_name", conn)
    cb_lim_csv = BASE_DIR / "cashback_limitations.csv"
    df_cb_lim.to_csv(cb_lim_csv, index=False)
    log.info(f"Exported {len(df_cb_lim)} limitation-level cashback entries to {cb_lim_csv.name}")

    # Export comprehensive cashback analysis CSVs (denormalized with all SKUs)
    log.info("Exporting comprehensive cashback analysis CSVs...")

    # CSV 1: Segment-level — all segments × packs, with cashback + full limitation text
    df_analysis_seg = pd.read_sql("""
        SELECT
            pr.name_de AS product_name,
            pr.atc_code,
            pr.swissmedic_no5,
            pk.swissmedic_no8,
            pk.gtin,
            pk.bag_dossier_no,
            pk.description_de AS pack_desc,
            e_pk_first.release_date AS pack_first_seen,
            e_pk_last.release_date  AS pack_last_seen,
            l.limitation_code,
            l.limitation_type,
            l.limitation_level,
            l.valid_from_date AS limitation_valid_from,
            l.valid_thru_date AS limitation_valid_thru,
            e_lf.release_date AS limitation_first_seen,
            e_ll.release_date AS limitation_last_seen,
            s.segment_id,
            s.segment_order,
            s.indication_name_de,
            s.indication_name_fr,
            s.indication_name_it,
            s.matched_code_value AS indication_code,
            s.matched_code_source AS code_match_source,
            CASE WHEN cs.cashback_segment_id IS NOT NULL THEN 1 ELSE 0 END AS has_cashback,
            cs.cashback_company,
            cs.detection_patterns AS cashback_detection,
            cs.rule_calc_type AS cashback_calc_type,
            cs.rule_calc_value AS cashback_calc_value,
            cs.rule_unit AS cashback_unit,
            cs.rule_threshold_type AS cashback_threshold_type,
            cs.rule_threshold_value AS cashback_threshold_value,
            cs.rule_threshold_unit AS cashback_threshold_unit,
            cs.rule_thresholds_all AS cashback_thresholds_all,
            cs.rule_cond_treatment_stop AS cond_treatment_stop,
            cs.rule_cond_adverse_effects AS cond_adverse_effects,
            cs.rule_cond_treatment_failure AS cond_treatment_failure,
            cs.rule_cotreatments AS cashback_cotreatments,
            cs.cashback_extract AS cashback_text_fr,
            s.segment_text_fr,
            s.segment_text_de,
            l.description_de AS limitation_text_de,
            l.description_fr AS limitation_text_fr
        FROM limitation_indication_segment s
        JOIN limitation l ON s.limitation_id = l.limitation_id
        JOIN preparation pr ON s.preparation_id = pr.preparation_id
        LEFT JOIN pack pk ON pk.preparation_id = pr.preparation_id
        LEFT JOIN cashback_segment cs ON cs.segment_id = s.segment_id
        LEFT JOIN extract e_pk_first ON pk.first_seen_extract = e_pk_first.extract_id
        LEFT JOIN extract e_pk_last  ON pk.last_seen_extract  = e_pk_last.extract_id
        LEFT JOIN extract e_lf ON l.first_seen_extract = e_lf.extract_id
        LEFT JOIN extract e_ll ON l.last_seen_extract  = e_ll.extract_id
        ORDER BY pr.name_de, l.limitation_code, s.segment_order, pk.gtin
    """, conn)
    _clean_html_columns(df_analysis_seg, [
        "segment_text_fr", "segment_text_de",
        "limitation_text_de", "limitation_text_fr",
        "cashback_text_fr",
    ])
    seg_analysis_csv = BASE_DIR / "cashback_analysis_segments.csv"
    df_analysis_seg.to_csv(seg_analysis_csv, index=False, encoding="utf-8-sig")
    log.info(f"Exported {len(df_analysis_seg)} rows to {seg_analysis_csv.name} "
             f"({df_analysis_seg['segment_id'].nunique()} segments, "
             f"{df_analysis_seg['product_name'].nunique()} products)")

    # CSV 2: Limitation-level — unsegmented limitations with cashback × packs
    df_analysis_lim = pd.read_sql("""
        SELECT
            pr.name_de AS product_name,
            pr.atc_code,
            pr.swissmedic_no5,
            pk.swissmedic_no8,
            pk.gtin,
            pk.bag_dossier_no,
            pk.description_de AS pack_desc,
            e_pk_first.release_date AS pack_first_seen,
            e_pk_last.release_date  AS pack_last_seen,
            c.limitation_id,
            l.limitation_code,
            l.limitation_type,
            l.limitation_level,
            l.indication_name_de,
            l.indication_name_fr,
            l.valid_from_date AS limitation_valid_from,
            l.valid_thru_date AS limitation_valid_thru,
            e_lf.release_date AS limitation_first_seen,
            e_ll.release_date AS limitation_last_seen,
            1 AS has_cashback,
            c.cashback_company,
            c.detection_patterns AS cashback_detection,
            c.rule_calc_type AS cashback_calc_type,
            c.rule_calc_value AS cashback_calc_value,
            c.rule_unit AS cashback_unit,
            c.rule_threshold_type AS cashback_threshold_type,
            c.rule_threshold_value AS cashback_threshold_value,
            c.rule_threshold_unit AS cashback_threshold_unit,
            c.rule_thresholds_all AS cashback_thresholds_all,
            c.rule_cond_treatment_stop AS cond_treatment_stop,
            c.rule_cond_adverse_effects AS cond_adverse_effects,
            c.rule_cond_treatment_failure AS cond_treatment_failure,
            c.rule_cotreatments AS cashback_cotreatments,
            c.cashback_extract AS cashback_text_fr,
            l.description_de AS limitation_text_de,
            l.description_fr AS limitation_text_fr
        FROM cashback c
        JOIN limitation l ON c.limitation_id = l.limitation_id
        JOIN preparation pr ON c.preparation_id = pr.preparation_id
        LEFT JOIN pack pk ON pk.preparation_id = pr.preparation_id
        LEFT JOIN extract e_pk_first ON pk.first_seen_extract = e_pk_first.extract_id
        LEFT JOIN extract e_pk_last  ON pk.last_seen_extract  = e_pk_last.extract_id
        LEFT JOIN extract e_lf ON l.first_seen_extract = e_lf.extract_id
        LEFT JOIN extract e_ll ON l.last_seen_extract  = e_ll.extract_id
        WHERE c.limitation_id NOT IN (
            SELECT DISTINCT limitation_id FROM limitation_indication_segment
        )
        ORDER BY pr.name_de, l.limitation_code, pk.gtin
    """, conn)
    _clean_html_columns(df_analysis_lim, [
        "limitation_text_de", "limitation_text_fr",
        "cashback_text_fr",
    ])
    lim_analysis_csv = BASE_DIR / "cashback_analysis_limitations.csv"
    df_analysis_lim.to_csv(lim_analysis_csv, index=False, encoding="utf-8-sig")
    log.info(f"Exported {len(df_analysis_lim)} rows to {lim_analysis_csv.name} "
             f"({df_analysis_lim['limitation_id'].nunique()} limitations, "
             f"{df_analysis_lim['product_name'].nunique()} products)")

    conn.close()
    log.info("=" * 60)
    log.info("Done!")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

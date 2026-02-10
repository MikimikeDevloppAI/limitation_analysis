"""
build_sku_normalized.py
=======================
Parse pack descriptions from swiss_pharma_limitations.db into structured
SKU attributes (form type, unit count, volume, substance dosage, etc.)
and write a normalised `sku_normalized` table + CSV export.

Standalone script — reads from and writes to the existing database.
"""

import logging
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

# ============================================================
# Configuration
# ============================================================

BASE_DIR = Path(r"c:\Users\micha\OneDrive\Matching_indication_code")
DB_PATH = BASE_DIR / "swiss_pharma_limitations.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# Form type mapping: German abbreviations → standardised types
# ============================================================

FORM_TYPE_MAP = {
    # Syringes
    "Fertigspr COP":    "SYRINGE",
    "Plastik-Fertspr":  "SYRINGE",
    "Fertigspr":        "SYRINGE",
    "Fertspr":          "SYRINGE",
    "Spr":              "SYRINGE",
    "Spritzamp":        "SYRINGE_AMPOULE",
    "Dosierspr":        "METERED_SYRINGE",
    "Zyl Amp":          "SYRINGE_AMPOULE",
    # Pens
    "Fertigpen":        "PEN",
    "Fertpen":          "PEN",
    # Injectors
    "vorgef Injektor":  "INJECTOR",
    "Injektor":         "INJECTOR",
    "Fertinj":          "INJECTOR",
    "Inj kit":          "INJECTION_KIT",
    # Vials
    "Act O Vial":       "VIAL",
    "Durchstfl":        "VIAL",
    "Durchstf":         "VIAL",
    "Onco-Tain":        "VIAL",
    "Cytosafe":         "VIAL",
    "Vial":             "VIAL",
    # Ampoules
    "Zweik Amp":        "AMPOULE",
    "Trinkamp":         "ORAL_AMPOULE",
    "Amp":              "AMPOULE",
    # Bottles
    "Plast Fl":         "BOTTLE",
    "Glasfl":           "BOTTLE",
    "Glas Fl":          "BOTTLE",
    "Tropffl":          "BOTTLE",
    "Dosierfl":         "BOTTLE",
    "PP Fl":            "BOTTLE",
    "Glas":             "BOTTLE",
    "Fl":               "BOTTLE",
    # Blisters
    "PocketPack":       "BLISTER",
    "Blist":            "BLISTER",
    # Sachets / bags
    "Doppel Btl":       "SACHET",
    "Dppl Btl":         "SACHET",
    "KabiPac":          "SACHET",
    "Stick":            "SACHET",
    "Btl":              "SACHET",
    # Infusion bags
    "Infusionsbtl":     "BAG",
    "Polybag":          "BAG",
    "Freeflex":         "BAG",
    # Tubes
    "Tb":               "TUBE",
    # Dispensers
    "Dosierpumpe":      "DISPENSER",
    "Disp":             "DISPENSER",
    "Ds":               "DISPENSER",
    # Aerosols
    "Dosieraeros":      "AEROSOL",
    # Monodose
    "Monodos":          "MONODOSE",
    "Unidos":           "MONODOSE",
    "Respule":          "MONODOSE",
    # Cartridges
    "Patronen":         "CARTRIDGE",
    "Patrone":          "CARTRIDGE",
    # Sets
    "Set":              "SET",
    # Other
    "Tagesdosen":       "DAILY_DOSE",
    "Topf":             "JAR",
}

# Build regex alternation: longest tokens first so "Plast Fl" matches before "Fl"
_FORM_TOKENS_SORTED = sorted(FORM_TYPE_MAP.keys(), key=len, reverse=True)
_FORM_RE = "|".join(re.escape(f) for f in _FORM_TOKENS_SORTED)

# ============================================================
# Regex patterns (compiled)
# ============================================================

_NUM = r"(\d+(?:\.\d+)?)"
_UNIT = r"(ml|g|MBq|GBq|kBq|Stk|Dos|Dosen)"

# Pre-processing: extract parenthetical annotations
RE_ANNOTATION = re.compile(r"\s*\(([^)]+)\)")

# P1: Form N x N x N Unit  — "Blist 10 x 10 x 1 Stk"
RE_P1 = re.compile(
    rf"^({_FORM_RE})\s+{_NUM}\s*x\s*{_NUM}\s*x\s*{_NUM}\s+{_UNIT}$"
)

# P1b: Form N x N Unit  — "Ds 3 x 30 Stk", "Blist 4 x 16 Stk"
RE_P1B = re.compile(
    rf"^({_FORM_RE})\s+{_NUM}\s*x\s*{_NUM}\s+{_UNIT}$"
)

# P2: N x Form N Unit  — "2x Blist 14 Stk", "3 x Fl 5 ml"
RE_P2 = re.compile(
    rf"^{_NUM}\s*x\s*({_FORM_RE})\s+{_NUM}\s+{_UNIT}$"
)

# P3: N x N Unit  — "3 x 30 Dosen", "5 x 3 ml"
RE_P3 = re.compile(
    rf"^{_NUM}\s*x\s*{_NUM}\s+{_UNIT}$"
)

# P4: N Form N Unit  — "5 Fertspr 3 ml", "10 Amp 2 ml"
RE_P4 = re.compile(
    rf"^{_NUM}\s+({_FORM_RE})\s+{_NUM}\s+{_UNIT}$"
)

# P5: Form N Stk  — "Durchstf 1 Stk", "Fertspr 4 Stk"
RE_P5 = re.compile(
    rf"^({_FORM_RE})\s+{_NUM}\s+(Stk)$"
)

# P6: Form N Unit (non-Stk)  — "Durchstf 4 ml", "Tb 30 g"
RE_P6 = re.compile(
    rf"^({_FORM_RE})\s+{_NUM}\s+{_UNIT}$"
)

# P7: Form N Dos/Dosen  — "Dosierspr 140 Dos"
RE_P7 = re.compile(
    rf"^({_FORM_RE})\s+{_NUM}\s+(Dos|Dosen)$"
)

# P8: N Form (no unit)  — "10 Durchstf", "2 x 2 Fertspr"
RE_P8 = re.compile(
    rf"^{_NUM}\s+({_FORM_RE})$"
)

# P8b: N x N Form (no unit)  — "2 x 2 Fertspr"
RE_P8B = re.compile(
    rf"^{_NUM}\s*x\s*{_NUM}\s+({_FORM_RE})$"
)

# P9: N Unit  — "30 Stk", "240 ml", "37 MBq", "120 Dos"
RE_P9 = re.compile(
    rf"^{_NUM}\s+{_UNIT}$"
)

# P10: N N Unit  — "30 0.3 ml" (positional: N containers of N unit each)
RE_P10 = re.compile(
    rf"^{_NUM}\s+{_NUM}\s+{_UNIT}$"
)

# P11: N Unit zur Text  — "10 ml zur Prophylaxe"
RE_P11 = re.compile(
    rf"^{_NUM}\s+(ml|g)\s+zur\s+(.+)$"
)

# P12: Form N Stk N Unit  — "Durchstf 1 Stk 10 ml"
RE_P12 = re.compile(
    rf"^({_FORM_RE})\s+{_NUM}\s+Stk\s+{_NUM}\s+{_UNIT}$"
)

# P13: N Unit N Stk  — "3 ml 5 Stk", "10 ml 5 Stk" (volume then count)
RE_P13 = re.compile(
    rf"^{_NUM}\s+(ml|g)\s+{_NUM}\s+Stk$"
)

# P14: N Form N Btl à/a N Stk  — "90 Monodos 9 Btl à 10 Stk"
RE_P14 = re.compile(
    rf"^{_NUM}\s+({_FORM_RE})\s+{_NUM}\s+Btl\s+[àa]\s+{_NUM}\s+Stk$"
)


# ============================================================
# Parser
# ============================================================

def _int(val):
    """Convert to int, rounding floats."""
    if val is None:
        return None
    return int(float(val))


def _float(val):
    """Convert to float or None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def parse_pack_description(raw_desc):
    """Parse a pack description_de into structured fields.

    Returns dict with keys: form_type, form_type_raw, container_count,
    unit_count, volume_per_unit, volume_unit, total_volume, dose_count,
    multiplier, multiplied_count, total_units, is_alt, annotation,
    parse_confidence, parse_pattern.
    """
    result = {
        "form_type": None, "form_type_raw": None,
        "container_count": None, "unit_count": None,
        "volume_per_unit": None, "volume_unit": None,
        "total_volume": None, "dose_count": None,
        "multiplier": None, "multiplied_count": None,
        "total_units": None,
        "is_alt": 0, "annotation": None,
        "parse_confidence": "LOW", "parse_pattern": "UNMATCHED",
    }

    if not raw_desc:
        return result

    desc = raw_desc.strip()

    # --- Step 1: Extract parenthetical annotations ---
    annotations = RE_ANNOTATION.findall(desc)
    desc_clean = RE_ANNOTATION.sub("", desc).strip()

    if annotations:
        non_alt = []
        for a in annotations:
            if a.strip().lower() in ("alt", "ancien"):
                result["is_alt"] = 1
            else:
                non_alt.append(a.strip())
        # Keep meaningful text annotations (skip sub-pack info like "3x 50 Stk")
        text_annots = [a for a in non_alt if not re.match(r"^\d+\s*x\s*\d+", a)]
        if text_annots:
            result["annotation"] = "; ".join(text_annots)

    # --- Step 1b: Normalize trailing periods and qualifier words ---
    # "Durchstf 1 Stk." → "Durchstf 1 Stk"
    desc_clean = re.sub(r"\.\s*$", "", desc_clean)
    # "Fertspr." → "Fertspr"  (period after form abbreviation)
    desc_clean = re.sub(r"(\w)\.\s", r"\1 ", desc_clean)
    desc_clean = re.sub(r"\.$", "", desc_clean)
    # "Fertspr Safe-Sys 2 Stk" → strip qualifier words between form and number
    desc_clean = re.sub(r"(Fertspr|Durchstf|Fertpen)\s+\S+[-]\S+\s+", r"\1 ", desc_clean)

    # --- Step 2: Try patterns in cascade ---

    # P1: Form N x N x N Unit
    m = RE_P1.match(desc_clean)
    if m:
        form_raw, n1, n2, n3, unit = m.groups()
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["multiplier"] = _int(n1)
        result["multiplied_count"] = _int(n2)
        if unit in ("Stk", "Dos", "Dosen"):
            result["unit_count"] = _int(n3)
            result["total_units"] = _int(n1) * _int(n2) * _int(n3)
            if unit in ("Dos", "Dosen"):
                result["dose_count"] = result["total_units"]
        else:
            result["volume_per_unit"] = _float(n3)
            result["volume_unit"] = unit
            result["total_volume"] = _float(n1) * _float(n2) * _float(n3)
            result["total_units"] = _int(n1) * _int(n2)
        result["parse_confidence"] = "HIGH"
        result["parse_pattern"] = "P1_TRIPLE"
        return result

    # P1b: Form N x N Unit  — "Ds 3 x 30 Stk", "Blist 4 x 16 Stk"
    m = RE_P1B.match(desc_clean)
    if m:
        form_raw, n1, n2, unit = m.groups()
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["multiplier"] = _int(n1)
        result["multiplied_count"] = _int(n2)
        if unit == "Stk":
            result["unit_count"] = _int(n2)
            result["total_units"] = _int(n1) * _int(n2)
        elif unit in ("Dos", "Dosen"):
            result["dose_count"] = _int(n1) * _int(n2)
            result["total_units"] = _int(n1)
        else:
            result["volume_per_unit"] = _float(n2)
            result["volume_unit"] = unit
            result["total_volume"] = _float(n1) * _float(n2)
            result["total_units"] = _int(n1)
        result["parse_confidence"] = "HIGH"
        result["parse_pattern"] = "P1B_FORM_MULT"
        return result

    # P8b: N x N Form (no unit) — "2 x 2 Fertspr"
    m = RE_P8B.match(desc_clean)
    if m:
        n1, n2, form_raw = m.groups()
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["multiplier"] = _int(n1)
        result["container_count"] = _int(n2)
        result["total_units"] = _int(n1) * _int(n2)
        result["parse_confidence"] = "MEDIUM"
        result["parse_pattern"] = "P8B_MULT_FORM_BARE"
        return result

    # P2: N x Form N Unit
    m = RE_P2.match(desc_clean)
    if m:
        n1, form_raw, n2, unit = m.groups()
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["multiplier"] = _int(n1)
        if unit == "Stk":
            result["unit_count"] = _int(n2)
            result["total_units"] = _int(n1) * _int(n2)
        elif unit in ("Dos", "Dosen"):
            result["dose_count"] = _int(n1) * _int(n2)
            result["total_units"] = _int(n1)
        else:
            result["volume_per_unit"] = _float(n2)
            result["volume_unit"] = unit
            result["total_volume"] = _float(n1) * _float(n2)
            result["container_count"] = _int(n1)
            result["total_units"] = _int(n1)
        result["parse_confidence"] = "HIGH"
        result["parse_pattern"] = "P2_MULT_FORM_UNIT"
        return result

    # P3: N x N Unit
    m = RE_P3.match(desc_clean)
    if m:
        n1, n2, unit = m.groups()
        result["multiplier"] = _int(n1)
        result["multiplied_count"] = _int(n2)
        if unit == "Stk":
            result["unit_count"] = _int(n2)
            result["total_units"] = _int(n1) * _int(n2)
        elif unit in ("Dos", "Dosen"):
            result["dose_count"] = _int(n1) * _int(n2)
            result["total_units"] = _int(n1)
        else:
            result["volume_per_unit"] = _float(n2)
            result["volume_unit"] = unit
            result["total_volume"] = _float(n1) * _float(n2)
            result["total_units"] = _int(n1)
        result["parse_confidence"] = "HIGH"
        result["parse_pattern"] = "P3_MULT_UNIT"
        return result

    # P4: N Form N Unit
    m = RE_P4.match(desc_clean)
    if m:
        n1, form_raw, n2, unit = m.groups()
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["container_count"] = _int(n1)
        if unit == "Stk":
            result["unit_count"] = _int(n2)
            result["total_units"] = _int(n1) * _int(n2)
        elif unit in ("Dos", "Dosen"):
            result["dose_count"] = _int(n1) * _int(n2)
            result["total_units"] = _int(n1)
        else:
            result["volume_per_unit"] = _float(n2)
            result["volume_unit"] = unit
            result["total_volume"] = _float(n1) * _float(n2)
            result["total_units"] = _int(n1)
        result["parse_confidence"] = "HIGH"
        result["parse_pattern"] = "P4_N_FORM_VOL"
        return result

    # P5: Form N Stk
    m = RE_P5.match(desc_clean)
    if m:
        form_raw, n1, _ = m.groups()
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["unit_count"] = _int(n1)
        result["container_count"] = _int(n1)
        result["total_units"] = _int(n1)
        result["parse_confidence"] = "HIGH"
        result["parse_pattern"] = "P5_FORM_STK"
        return result

    # P7: Form N Dos/Dosen (check before P6 to catch Dos specifically)
    m = RE_P7.match(desc_clean)
    if m:
        form_raw, n1, _ = m.groups()
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["dose_count"] = _int(n1)
        result["container_count"] = 1
        result["total_units"] = 1
        result["parse_confidence"] = "HIGH"
        result["parse_pattern"] = "P7_FORM_DOS"
        return result

    # P6: Form N Unit (non-Stk volume/weight)
    m = RE_P6.match(desc_clean)
    if m:
        form_raw, n1, unit = m.groups()
        # If unit is Stk, this was already handled by P5
        if unit == "Stk":
            pass  # fall through (shouldn't happen due to P5)
        else:
            result["form_type_raw"] = form_raw
            result["form_type"] = FORM_TYPE_MAP.get(form_raw)
            result["volume_per_unit"] = _float(n1)
            result["volume_unit"] = unit
            result["container_count"] = 1
            result["total_volume"] = _float(n1)
            result["total_units"] = 1
            result["parse_confidence"] = "HIGH"
            result["parse_pattern"] = "P6_FORM_VOL"
            return result

    # P8: N Form (no unit)
    m = RE_P8.match(desc_clean)
    if m:
        n1, form_raw = m.groups()
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["container_count"] = _int(n1)
        result["total_units"] = _int(n1)
        result["parse_confidence"] = "MEDIUM"
        result["parse_pattern"] = "P8_N_FORM"
        return result

    # P11: N Unit zur Text
    m = RE_P11.match(desc_clean)
    if m:
        n1, unit, text = m.groups()
        result["volume_per_unit"] = _float(n1)
        result["volume_unit"] = unit
        result["total_volume"] = _float(n1)
        result["total_units"] = 1
        if result["annotation"]:
            result["annotation"] += f"; zur {text}"
        else:
            result["annotation"] = f"zur {text}"
        result["parse_confidence"] = "MEDIUM"
        result["parse_pattern"] = "P11_VOL_ZUR"
        return result

    # P10: N N Unit (positional)
    m = RE_P10.match(desc_clean)
    if m:
        n1, n2, unit = m.groups()
        result["container_count"] = _int(n1)
        if unit == "Stk":
            result["unit_count"] = _int(n2)
            result["total_units"] = _int(n1) * _int(n2)
        elif unit in ("Dos", "Dosen"):
            result["dose_count"] = _int(n1) * _int(n2)
            result["total_units"] = _int(n1)
        else:
            result["volume_per_unit"] = _float(n2)
            result["volume_unit"] = unit
            result["total_volume"] = _float(n1) * _float(n2)
            result["total_units"] = _int(n1)
        result["parse_confidence"] = "MEDIUM"
        result["parse_pattern"] = "P10_N_N_UNIT"
        return result

    # P9: N Unit (simple)
    m = RE_P9.match(desc_clean)
    if m:
        n1, unit = m.groups()
        if unit == "Stk":
            result["unit_count"] = _int(n1)
            result["total_units"] = _int(n1)
        elif unit in ("Dos", "Dosen"):
            result["dose_count"] = _int(n1)
            result["total_units"] = 1
        else:
            result["volume_per_unit"] = _float(n1)
            result["volume_unit"] = unit
            result["total_volume"] = _float(n1)
            result["total_units"] = 1
        result["parse_confidence"] = "HIGH"
        result["parse_pattern"] = "P9_SIMPLE"
        return result

    # P14: N Form N Btl à/a N Stk  — "90 Monodos 9 Btl à 10 Stk"
    m = RE_P14.match(desc_clean)
    if m:
        n1, form_raw, n2, n3 = m.groups()
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["container_count"] = _int(n1)
        result["unit_count"] = _int(n3)
        result["total_units"] = _int(n1)
        result["annotation"] = f"{_int(n2)} Btl à {_int(n3)} Stk"
        result["parse_confidence"] = "MEDIUM"
        result["parse_pattern"] = "P14_MONODOS_BTL"
        return result

    # P12: Form N Stk N Unit  — "Durchstf 1 Stk 10 ml"
    m = RE_P12.match(desc_clean)
    if m:
        form_raw, n1, n2, unit = m.groups()
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["container_count"] = _int(n1)
        result["total_units"] = _int(n1)
        result["volume_per_unit"] = _float(n2)
        result["volume_unit"] = unit
        result["total_volume"] = _float(n1) * _float(n2)
        result["parse_confidence"] = "HIGH"
        result["parse_pattern"] = "P12_FORM_STK_VOL"
        return result

    # P13: N Unit N Stk  — "3 ml 5 Stk" (volume then count, reversed)
    m = RE_P13.match(desc_clean)
    if m:
        n1, unit, n2 = m.groups()
        result["volume_per_unit"] = _float(n1)
        result["volume_unit"] = unit
        result["container_count"] = _int(n2)
        result["total_units"] = _int(n2)
        result["total_volume"] = _float(n1) * _float(n2)
        result["parse_confidence"] = "MEDIUM"
        result["parse_pattern"] = "P13_VOL_STK"
        return result

    # Bare form: just a form name with no numbers — "Durchstf"
    bare_form = re.match(rf"^({_FORM_RE})$", desc_clean)
    if bare_form:
        form_raw = bare_form.group(1)
        result["form_type_raw"] = form_raw
        result["form_type"] = FORM_TYPE_MAP.get(form_raw)
        result["container_count"] = 1
        result["total_units"] = 1
        result["parse_confidence"] = "MEDIUM"
        result["parse_pattern"] = "BARE_FORM"
        return result

    # Fallback: UNMATCHED
    result["parse_confidence"] = "LOW"
    result["parse_pattern"] = "UNMATCHED"
    return result


# ============================================================
# Substance quantity parser
# ============================================================

def parse_substance_qty(qty_str):
    """Parse substance quantity string to numeric float.

    Handles: '150', '12.5', '<0.007', 'ca. 100', 'min. 30'.
    """
    if not qty_str:
        return None
    cleaned = re.sub(r"^(<|ca\.\s*|min\.\s*|max\.\s*|~\s*)", "", qty_str.strip())
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


# ============================================================
# SQL
# ============================================================

SKU_SCHEMA = """
DROP TABLE IF EXISTS sku_normalized;

CREATE TABLE sku_normalized (
    sku_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pack_id_db          INTEGER NOT NULL REFERENCES pack(pack_id_db),
    gtin                TEXT NOT NULL,
    swissmedic_no8      TEXT,
    preparation_id      INTEGER NOT NULL,
    product_name        TEXT,
    description_de      TEXT,
    form_type           TEXT,
    form_type_raw       TEXT,
    container_count     INTEGER,
    unit_count          INTEGER,
    volume_per_unit     REAL,
    volume_unit         TEXT,
    total_volume        REAL,
    dose_count          INTEGER,
    multiplier          INTEGER,
    multiplied_count    INTEGER,
    total_units         INTEGER,
    substance_name      TEXT,
    substance_qty       REAL,
    substance_qty_raw   TEXT,
    substance_unit      TEXT,
    total_substance     REAL,
    is_alt              INTEGER DEFAULT 0,
    annotation          TEXT,
    parse_confidence    TEXT,
    parse_pattern       TEXT,
    org_gen_code        TEXT,
    atc_code            TEXT,
    public_price        REAL,
    exfactory_price     REAL
);

CREATE INDEX idx_sku_norm_gtin ON sku_normalized(gtin);
CREATE INDEX idx_sku_norm_prep ON sku_normalized(preparation_id);
CREATE INDEX idx_sku_norm_form ON sku_normalized(form_type);
CREATE INDEX idx_sku_norm_atc  ON sku_normalized(atc_code);
"""

MAIN_QUERY = """
SELECT
    pk.pack_id_db, pk.gtin, pk.swissmedic_no8, pk.preparation_id,
    pr.name_de AS product_name, pk.description_de,
    pr.org_gen_code, pr.atc_code, pk.public_price, pk.exfactory_price,
    s.description_la AS substance_name,
    s.quantity AS substance_qty_raw,
    s.quantity_unit AS substance_unit
FROM pack pk
JOIN preparation pr ON pk.preparation_id = pr.preparation_id
LEFT JOIN substance s ON s.preparation_id = pr.preparation_id
    AND s.substance_id = (
        SELECT MIN(s2.substance_id)
        FROM substance s2
        WHERE s2.preparation_id = pr.preparation_id
    )
ORDER BY pr.name_de, pk.gtin
"""

INSERT_SQL = """
INSERT INTO sku_normalized (
    pack_id_db, gtin, swissmedic_no8, preparation_id, product_name,
    description_de, form_type, form_type_raw,
    container_count, unit_count, volume_per_unit, volume_unit,
    total_volume, dose_count, multiplier, multiplied_count, total_units,
    substance_name, substance_qty, substance_qty_raw, substance_unit,
    total_substance, is_alt, annotation,
    parse_confidence, parse_pattern,
    org_gen_code, atc_code, public_price, exfactory_price
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Units where total_substance = substance_qty * total_units makes sense
COMPUTABLE_SUBSTANCE_UNITS = {
    "mg", "mcg", "g", "UI", "U", "mmol", "Mio U", "Mio UI",
}


# ============================================================
# Main build function
# ============================================================

def build_sku_normalized(conn):
    """Build the sku_normalized table from pack + preparation + substance data."""
    log.info("Creating sku_normalized table...")
    conn.executescript(SKU_SCHEMA)

    rows = conn.execute(MAIN_QUERY).fetchall()
    col_names = [d[0] for d in conn.execute(MAIN_QUERY).description]
    log.info(f"Processing {len(rows)} packs...")

    stats_confidence = Counter()
    stats_pattern = Counter()
    stats_form = Counter()
    inserted = 0

    for row in rows:
        d = dict(zip(col_names, row))

        # Parse description
        parsed = parse_pack_description(d["description_de"])

        # Parse substance quantity
        sub_qty = parse_substance_qty(d["substance_qty_raw"])

        # Compute total_substance
        total_sub = None
        if (sub_qty is not None
                and parsed["total_units"] is not None
                and d["substance_unit"] in COMPUTABLE_SUBSTANCE_UNITS):
            total_sub = sub_qty * parsed["total_units"]

        conn.execute(INSERT_SQL, (
            d["pack_id_db"], d["gtin"], d["swissmedic_no8"],
            d["preparation_id"], d["product_name"],
            d["description_de"],
            parsed["form_type"], parsed["form_type_raw"],
            parsed["container_count"], parsed["unit_count"],
            parsed["volume_per_unit"], parsed["volume_unit"],
            parsed["total_volume"], parsed["dose_count"],
            parsed["multiplier"], parsed["multiplied_count"],
            parsed["total_units"],
            d["substance_name"], sub_qty, d["substance_qty_raw"],
            d["substance_unit"], total_sub,
            parsed["is_alt"], parsed["annotation"],
            parsed["parse_confidence"], parsed["parse_pattern"],
            d["org_gen_code"], d["atc_code"],
            d["public_price"], d["exfactory_price"],
        ))

        stats_confidence[parsed["parse_confidence"]] += 1
        stats_pattern[parsed["parse_pattern"]] += 1
        if parsed["form_type"]:
            stats_form[parsed["form_type"]] += 1
        inserted += 1

    conn.commit()
    log.info(f"Inserted {inserted} rows into sku_normalized")

    # Log statistics
    log.info("Parse confidence distribution:")
    for conf in ("HIGH", "MEDIUM", "LOW"):
        log.info(f"  {conf}: {stats_confidence.get(conf, 0)}")

    log.info("Pattern distribution:")
    for pat, cnt in stats_pattern.most_common():
        log.info(f"  {pat}: {cnt}")

    log.info("Form type distribution:")
    for form, cnt in stats_form.most_common():
        log.info(f"  {form}: {cnt}")

    # Substance stats
    with_sub = conn.execute(
        "SELECT COUNT(*) FROM sku_normalized WHERE substance_name IS NOT NULL"
    ).fetchone()[0]
    with_total = conn.execute(
        "SELECT COUNT(*) FROM sku_normalized WHERE total_substance IS NOT NULL"
    ).fetchone()[0]
    log.info(f"Substance data: {with_sub}/{inserted} packs have substance info")
    log.info(f"Total substance computable: {with_total}/{inserted}")


def export_csv(conn):
    """Export sku_normalized to CSV."""
    df = pd.read_sql(
        "SELECT * FROM sku_normalized ORDER BY product_name, gtin", conn
    )
    csv_path = BASE_DIR / "sku_normalized.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log.info(f"Exported {len(df)} rows to {csv_path.name}")


# ============================================================
# Main
# ============================================================

def main():
    log.info("=" * 60)
    log.info("SKU Normalisation — Pack Description Parser")
    log.info("=" * 60)

    if not DB_PATH.exists():
        log.error(f"Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(str(DB_PATH))

    try:
        build_sku_normalized(conn)
        export_csv(conn)
    finally:
        conn.close()

    log.info("=" * 60)
    log.info("Done!")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

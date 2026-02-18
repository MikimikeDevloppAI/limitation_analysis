"""
Step 04: Detect cashback on limitation texts.

Detection uses GERMAN text (description_de) because German vocabulary
distinguishes clearly between insurance coverage ("vergütet") and
manufacturer cashback ("erstattet", "zurückerstattet", "Rückerstattung").

Calculation details (%, CHF, unit) are extracted from FRENCH text
(description_fr) using existing patterns in cashback_extractor.py.

Company name extraction: 3 levels
  1. Regex on DE text (e.g. "von der [Company AG] die Kosten erstattet")
  2. Fuzzy match of DB-linked company names in DE text
  3. Fallback "Zulassungsinhaberin" if word present but no name found
"""

import html
import logging
import re
import sqlite3

from cashback_extractor import (
    extract_cashback_sentence,
    extract_calculation, extract_unit, clean_html,
)

log = logging.getLogger(__name__)


# ============================================================
# German cashback detection patterns
# ============================================================

CASHBACK_DE_PATTERNS = [
    ("ERSTATTET", re.compile(
        r"(?:zur\xfcck)?erstattet", re.IGNORECASE)),
    ("ZULASSUNGSINHABERIN", re.compile(
        r"Zulassungsinhaberin")),
    ("RUECKERSTATTUNG", re.compile(
        r"R\xfcck(?:erstattung|verg\xfctung)")),
    ("RUECKVERGUETET", re.compile(
        r"r\xfcck(?:verg\xfctet|erstattet)", re.IGNORECASE)),
    ("VERGUETET_DEM_KV", re.compile(
        r"verg\xfctet\s+dem\s+(?:Krankenversicherer|Versicherer)")),
    ("VERGUETET_ZURUECK", re.compile(
        r"verg\xfctet[^<]{0,300}zur\xfcck")),
    ("COMPANY_VERGUETET", re.compile(
        r"(?:"
        r"\b(?:AG|SA|GmbH|S\xe0rl)\b\s+verg\xfctet"
        r"|verg\xfctet\s+(?:die\s+)?"
        r"(?:[A-Z][A-Za-z\xe0-\xff\-]+(?:\s+[A-Za-z\xe0-\xff\-\(\)&.]+)*"
        r"\s+(?:AG|SA|GmbH|S\xe0rl)\b))")),
    ("ZAHLT_ZURUECK", re.compile(
        r"zahlt[^<]{0,300}zur\xfcck")),
    ("MIT_PREISMODELL", re.compile(
        r"\(mit\s+Preismodell\)")),
    ("VERTRAULICH", re.compile(
        r"vertraulich")),
    ("AUF_ANTRAG", re.compile(
        r"auf\s+Antrag\s+des\s+Versicherers")),
    ("AUFFORDERUNG", re.compile(
        r"(?:nach|auf)\s+Aufforderung\s+durch\s+"
        r"de(?:n|njenigen)\s+Krankenversicherer")),
    ("TEILZAHLUNG", re.compile(
        r"Teilzahlung")),
]

# False positive filters — each one selectively kills specific patterns
DE_FALSE_POS = [
    ("FP_OHNE_PREIS", re.compile(r"ohne\s+Preismodell"),
     {"MIT_PREISMODELL"}),
    ("FP_NICHT_ERSTATTET", re.compile(
        r"nicht\s+(?:mehr\s+)?(?:r\xfcck)?erstattet"),
     {"ERSTATTET", "RUECKVERGUETET"}),
    ("FP_NICHT_VERGUETET", re.compile(
        r"wird\s+nicht\s+(?:mehr\s+)?(?:r\xfcck)?verg\xfctet"),
     {"VERGUETET_DEM_KV", "VERGUETET_ZURUECK", "COMPANY_VERGUETET"}),
    ("FP_ERSTATTBAR", re.compile(r"\berstattbar\b"),
     {"ERSTATTET"}),
]

# Company name extraction from German text
COMPANY_PATTERNS_DE = [
    # "von der [Firma] [Company AG/SA/GmbH] die Kosten ... erstattet"
    re.compile(
        r"von\s+(?:der\s+)?(?:Firma\s+)?"
        r"([A-Z][A-Za-z\xe0-\xff\-]+(?:\s+[A-Za-z\xe0-\xff\-\(\)&.]+)*"
        r"\s+(?:AG|SA|GmbH|S\xe0rl))"
    ),
    # "Die Zulassungsinhaberin [Company AG] vergütet"
    re.compile(
        r"Zulassungsinhaberin\s+"
        r"([A-Z][A-Za-z\xe0-\xff\-]+(?:\s+[A-Za-z\xe0-\xff\-\(\)&.]+)*"
        r"\s+(?:AG|SA|GmbH|S\xe0rl))"
    ),
    # "vergütet [die] [Company AG] nach Aufforderung"
    re.compile(
        r"verg\xfctet\s+(?:die\s+)?"
        r"([A-Z][A-Za-z\xe0-\xff\-]+(?:\s+[A-Za-z\xe0-\xff\-\(\)&.]+)*"
        r"\s+(?:AG|SA|GmbH|S\xe0rl))"
    ),
    # "[Company AG] zahlt ... zurück"
    re.compile(
        r"([A-Z][A-Za-z\xe0-\xff\-]+(?:\s+[A-Za-z\xe0-\xff\-\(\)&.]+)*"
        r"\s+(?:AG|SA|GmbH|S\xe0rl))\s+zahlt"
    ),
    # "[Company AG] vergütet dem/nach"
    re.compile(
        r"([A-Z][A-Za-z\xe0-\xff\-]+(?:\s+[A-Za-z\xe0-\xff\-\(\)&.]+)*"
        r"\s+(?:AG|SA|GmbH|S\xe0rl))\s+verg\xfctet\s+(?:dem|nach)"
    ),
]

_LEGAL_SUFFIX_RE = re.compile(r"\s+(?:AG|SA|GmbH|S\xe0rl|S\.A\.|Ltd).*$")


# ============================================================
# Detection
# ============================================================

def detect_cashback_de(text_de):
    """Detect cashback from German text.

    Returns dict with:
      is_cashback: bool
      patterns: list of matched pattern names
      company: str or None (from regex extraction)
    """
    matched = []
    for name, pat in CASHBACK_DE_PATTERNS:
        if pat.search(text_de):
            matched.append(name)

    if not matched:
        return {"is_cashback": False, "patterns": [], "company": None}

    # Apply false positive filters (each kills only specific patterns)
    killed = set()
    for _fp_name, fp_pat, kills in DE_FALSE_POS:
        if fp_pat.search(text_de):
            killed |= kills

    surviving = [p for p in matched if p not in killed]
    if not surviving:
        return {"is_cashback": False, "patterns": [], "company": None}

    # Extract company name from DE text
    company = None
    for cpat in COMPANY_PATTERNS_DE:
        m = cpat.search(text_de)
        if m:
            company = m.group(1).strip()
            break

    return {"is_cashback": True, "patterns": surviving, "company": company}


def _fuzzy_match_company(text_de, db_companies):
    """Try to find a DB company name in the DE text.

    Levels:
      1. Exact match of full company_name
      2. Base name match (without AG/SA/GmbH/Sàrl suffix)
    Returns matched company_name or None.
    """
    for company_name in db_companies:
        if company_name in text_de:
            return company_name

    for company_name in db_companies:
        base = _LEGAL_SUFFIX_RE.sub("", company_name).strip()
        if len(base) > 3 and base in text_de:
            return company_name

    return None


# Strong cashback verbs near company name (not "vergütet" alone — too ambiguous)
_COMPANY_CASHBACK_VERBS_RE = re.compile(
    r"(?:\xfcbernommen|erstattet|r\xfcckerstattet|r\xfcckverg\xfctet"
    r"|zahlt|Kosten|R\xfcckerstattung|R\xfcckverg\xfctung)",
    re.IGNORECASE,
)


def _company_cashback_signal(text_de, db_companies):
    """Check if a DB company name appears near a cashback verb in the text.

    Uses strong verbs only (not 'vergütet' which means insurance coverage).
    Returns company_name if found, else None.
    """
    for company_name in db_companies:
        # Check full name
        pos = text_de.find(company_name)
        if pos == -1:
            base = _LEGAL_SUFFIX_RE.sub("", company_name).strip()
            if len(base) > 3:
                pos = text_de.find(base)
        if pos == -1:
            continue

        # Look for cashback verb within 200 chars around the company name
        start = max(0, pos - 200)
        end = min(len(text_de), pos + len(company_name) + 200)
        context = text_de[start:end]
        if _COMPANY_CASHBACK_VERBS_RE.search(context):
            return company_name

    return None


# ============================================================
# Helpers
# ============================================================

def _has_column(conn, table, column):
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(c[1] == column for c in cols)


# ============================================================
# Main
# ============================================================

def run(db_path):
    """Run step 04: detect cashback on limitation_text."""
    log.info("Step 04: Detect cashback (German detection, French extraction)")

    conn = sqlite3.connect(str(db_path))

    # Add cashback columns if missing
    cashback_cols = [
        ("is_cashback", "INTEGER DEFAULT 0"),
        ("cashback_company", "TEXT"),
        ("cashback_patterns", "TEXT"),
        ("cashback_calc_type", "TEXT"),
        ("cashback_calc_value", "REAL"),
        ("cashback_unit", "TEXT"),
    ]
    for col_name, col_type in cashback_cols:
        if not _has_column(conn, "limitation_text", col_name):
            conn.execute(
                f"ALTER TABLE limitation_text ADD COLUMN {col_name} {col_type}"
            )

    # Pre-load company map: text_id -> [company_names]
    company_map = {}
    for text_id, company_name in conn.execute("""
        SELECT DISTINCT lim.text_id, c.company_name
        FROM limitation lim
        JOIN sku_limitation sl ON sl.limitation_id = lim.limitation_id
        JOIN sku s ON s.gtin = sl.gtin
        JOIN preparation_company pc ON pc.preparation_id = s.preparation_id
        JOIN company c ON c.company_id = pc.company_id
    """).fetchall():
        company_map.setdefault(text_id, []).append(company_name)
    log.info(f"  Loaded company map for {len(company_map)} texts")

    # Reset cashback
    conn.execute(
        "UPDATE limitation_text SET is_cashback = 0, cashback_company = NULL, "
        "cashback_patterns = NULL, cashback_calc_type = NULL, "
        "cashback_calc_value = NULL, cashback_unit = NULL"
    )

    rows = conn.execute(
        "SELECT text_id, description_de, description_fr FROM limitation_text"
    ).fetchall()

    detected = 0
    detected_by_company = 0
    company_from_regex = 0
    company_from_db = 0
    company_fallback_zih = 0

    for text_id, desc_de, desc_fr in rows:
        if not desc_de:
            continue

        cleaned_de = re.sub(r"<[^>]+>", " ", html.unescape(desc_de))
        result = detect_cashback_de(cleaned_de)

        db_companies = company_map.get(text_id, [])

        if not result["is_cashback"]:
            # Try DB company name + cashback verb as additional detection
            if not db_companies:
                continue
            company_match = _company_cashback_signal(cleaned_de, db_companies)
            if not company_match:
                continue
            detected += 1
            detected_by_company += 1
            company = company_match
            company_from_db += 1
            patterns = "DB_COMPANY_VERB"
        else:
            detected += 1
            company = result["company"]
            patterns = ",".join(result["patterns"])

            # Company resolution: regex → fuzzy DB → DB direct → "Zulassungsinhaberin"
            if company:
                company_from_regex += 1
            else:
                db_company_in_text = _fuzzy_match_company(cleaned_de, db_companies) if db_companies else None
                if db_company_in_text:
                    company = db_company_in_text
                    company_from_db += 1
                elif db_companies:
                    company = db_companies[0]
                    company_from_db += 1
                elif "Zulassungsinhaberin" in cleaned_de:
                    company = "Zulassungsinhaberin"
                    company_fallback_zih += 1

        # Extract calculation details from French text
        calc_type = calc_value = cb_unit = None
        if desc_fr:
            cleaned_fr = clean_html(html.unescape(desc_fr))
            sentence_result = extract_cashback_sentence(cleaned_fr)
            if sentence_result.get("has_cashback") and sentence_result.get("cashback_sentence"):
                sentence = sentence_result["cashback_sentence"]
                calc = extract_calculation(sentence)
                calc_type = calc.get("type")
                calc_value = calc.get("value")
                cb_unit = extract_unit(sentence)

        conn.execute(
            "UPDATE limitation_text SET "
            "is_cashback = 1, cashback_company = ?, cashback_patterns = ?, "
            "cashback_calc_type = ?, cashback_calc_value = ?, cashback_unit = ? "
            "WHERE text_id = ?",
            (company, patterns, calc_type, calc_value, cb_unit, text_id),
        )

    conn.commit()

    log.info(f"  Texts with cashback: {detected}/{len(rows)}")
    log.info(f"    Detected by DE patterns:  {detected - detected_by_company}")
    log.info(f"    Detected by company+verb: {detected_by_company}")
    log.info(f"  Company from regex:  {company_from_regex}")
    log.info(f"  Company from DB:     {company_from_db}")
    log.info(f"  Company fallback:    {company_fallback_zih}")
    log.info(f"  No company:          "
             f"{detected - company_from_regex - company_from_db - company_fallback_zih}")

    conn.close()
    log.info("  Step 04 done.")

"""
Step 04: Detect cashback on limitation texts.

Uses cashback_extractor.py to detect cashback patterns in French limitation
texts and extract company, calculation type, value, and unit.
"""

import html
import logging
import sqlite3

from cashback_extractor import (
    detect_cashback, extract_cashback_sentence,
    extract_calculation, extract_unit, clean_html,
    ReferenceDataLoader,
)

log = logging.getLogger(__name__)


def _has_column(conn, table, column):
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(c[1] == column for c in cols)


def run(db_path):
    """Run step 04: detect cashback on limitation_text."""
    log.info("Step 04: Detect cashback on limitation texts")

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

    # Load reference data for fuzzy company matching
    ref_data = ReferenceDataLoader(conn)
    if ref_data.load_all():
        stats = ref_data.get_stats()
        log.info(f"  Loaded ref data: {stats['companies']} companies, "
                 f"{stats['company_bases']} bases, "
                 f"{stats['preparations']} preparations")
    else:
        log.info("  No ref data loaded, fuzzy matching disabled")
        ref_data = None

    # Reset cashback
    conn.execute(
        "UPDATE limitation_text SET is_cashback = 0, cashback_company = NULL, "
        "cashback_patterns = NULL, cashback_calc_type = NULL, "
        "cashback_calc_value = NULL, cashback_unit = NULL"
    )

    rows = conn.execute(
        "SELECT text_id, description_fr FROM limitation_text"
    ).fetchall()

    detected = 0
    for text_id, desc_fr in rows:
        if not desc_fr:
            continue
        cleaned = clean_html(html.unescape(desc_fr))
        result = detect_cashback(cleaned, ref_data=ref_data)
        if not result["is_cashback"]:
            continue

        detected += 1
        company = result.get("company")
        patterns = ",".join(result.get("patterns_matched", []))

        calc_type = calc_value = cb_unit = None
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
    log.info(f"  Texts with cashback: {detected}/{len(rows)}")

    conn.close()
    log.info("  Step 04 done.")

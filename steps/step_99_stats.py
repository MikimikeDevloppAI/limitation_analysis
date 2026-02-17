"""
Step 99: Print comprehensive statistics about the database.
"""

import logging
import sqlite3

log = logging.getLogger(__name__)


def run(db_path):
    """Run step 99: print summary statistics."""
    log.info("Step 99: Statistics")

    conn = sqlite3.connect(str(db_path))

    # ---- Table counts ----
    log.info("")
    log.info("=" * 60)
    log.info("TABLE COUNTS")
    log.info("=" * 60)
    for table in ("extract_info", "sku", "limitation_text", "limitation",
                  "indication", "limitation_indication", "sku_limitation",
                  "company", "preparation_company"):
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info(f"  {table}: {cnt}")

    # ---- SKU parse confidence ----
    log.info("")
    log.info("SKU parse confidence:")
    for conf in ("HIGH", "MEDIUM", "LOW"):
        cnt = conn.execute(
            "SELECT COUNT(*) FROM sku WHERE parse_confidence = ?", (conf,)
        ).fetchone()[0]
        if cnt:
            log.info(f"  {conf}: {cnt}")

    # ---- Limitation level distribution ----
    log.info("")
    log.info("Limitation level (sku_limitation):")
    rows = conn.execute(
        "SELECT limitation_level, COUNT(*) FROM sku_limitation "
        "GROUP BY limitation_level ORDER BY COUNT(*) DESC"
    ).fetchall()
    for lvl, cnt in rows:
        log.info(f"  {lvl}: {cnt}")

    # ---- Indication table breakdown ----
    log.info("")
    log.info("Indication table:")
    ind_stats = conn.execute("""
        SELECT
            SUM(CASE WHEN indication_code IS NOT NULL AND indication_name_fr IS NOT NULL
                THEN 1 ELSE 0 END) as code_and_name,
            SUM(CASE WHEN indication_code IS NOT NULL AND indication_name_fr IS NULL
                THEN 1 ELSE 0 END) as code_only,
            SUM(CASE WHEN indication_code IS NULL AND indication_name_fr IS NOT NULL
                THEN 1 ELSE 0 END) as name_only,
            SUM(CASE WHEN indication_code IS NULL AND indication_name_fr IS NULL
                THEN 1 ELSE 0 END) as neither
        FROM indication
    """).fetchone()
    log.info(f"  code + name: {ind_stats[0]}")
    log.info(f"  code only:   {ind_stats[1]}")
    log.info(f"  name only:   {ind_stats[2]}")
    log.info(f"  neither:     {ind_stats[3]} (NOCODE)")

    # ---- limitation_indication by code_source ----
    log.info("")
    log.info("limitation_indication by code_source:")
    rows = conn.execute(
        "SELECT li.code_source, COUNT(*), COUNT(DISTINCT li.indication_id), "
        "COUNT(DISTINCT li.limitation_id) "
        "FROM limitation_indication li "
        "GROUP BY li.code_source ORDER BY COUNT(*) DESC"
    ).fetchall()
    for src, cnt, n_inds, n_lims in rows:
        log.info(f"  {src}: {cnt} rows ({n_inds} indications, {n_lims} limitations)")

    # ---- text_complexity distribution ----
    log.info("")
    log.info("text_complexity distribution:")
    rows = conn.execute(
        "SELECT text_complexity, COUNT(*) "
        "FROM limitation_text GROUP BY text_complexity ORDER BY COUNT(*) DESC"
    ).fetchall()
    for comp, cnt in rows:
        log.info(f"  {comp or 'NULL'}: {cnt}")

    # ---- Cashback ----
    log.info("")
    cb_texts = conn.execute(
        "SELECT COUNT(*) FROM limitation_text WHERE is_cashback = 1"
    ).fetchone()[0]
    total_texts = conn.execute("SELECT COUNT(*) FROM limitation_text").fetchone()[0]
    log.info(f"Cashback: {cb_texts}/{total_texts} texts")

    # Cashback with specific indication code (not NOCODE)
    cb_with_code = conn.execute("""
        SELECT COUNT(DISTINCT lt.text_id)
        FROM limitation_text lt
        JOIN limitation l ON l.text_id = lt.text_id
        JOIN limitation_indication li ON li.limitation_id = l.limitation_id
        JOIN indication ind ON ind.indication_id = li.indication_id
        WHERE lt.is_cashback = 1 AND ind.indication_code IS NOT NULL
    """).fetchone()[0]
    log.info(f"  Cashback texts with specific code: {cb_with_code}/{cb_texts}")

    # ---- Uncovered limitations ----
    uncovered = conn.execute("""
        SELECT COUNT(*) FROM limitation l
        WHERE l.limitation_id NOT IN (
            SELECT limitation_id FROM limitation_indication
        )
    """).fetchone()[0]
    log.info("")
    log.info(f"Uncovered limitations: {uncovered} (should be 0)")

    # ---- Multi-period limitation codes ----
    multi = conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT limitation_code FROM limitation "
        "  GROUP BY limitation_code HAVING COUNT(*) > 1"
        ")"
    ).fetchone()[0]
    log.info(f"Limitation codes with >1 period: {multi}")

    log.info("")
    log.info("=" * 60)

    conn.close()

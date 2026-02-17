"""
Step 02: Initialize indications for limitations without XML codes.

- Creates 1 NOCODE indication (code=NULL, name=NULL)
- Inserts limitation_indication rows pointing to NOCODE for all limitations
  that have no XML indication codes (pre-2023 texts)
- Sets text_complexity on limitation_text for texts with XML codes
"""

import logging
import sqlite3

log = logging.getLogger(__name__)


def run(db_path):
    """Run step 02: NOCODE indication + link pre-2023 limitations."""
    log.info("Step 02: Initialize indications for pre-2023 limitations")

    conn = sqlite3.connect(str(db_path))

    # Create a single NOCODE indication (no code, no name)
    nocode_row = conn.execute(
        "SELECT indication_id FROM indication WHERE indication_code IS NULL "
        "AND indication_name_de IS NULL AND indication_name_fr IS NULL"
    ).fetchone()
    if nocode_row:
        nocode_id = nocode_row[0]
    else:
        cur = conn.execute(
            "INSERT INTO indication (indication_code) VALUES (NULL)"
        )
        nocode_id = cur.lastrowid
    log.info(f"  NOCODE indication_id: {nocode_id}")

    # Insert NOCODE rows for all limitations without any indication link
    lims_without = conn.execute("""
        SELECT l.limitation_id, l.valid_from, l.valid_to, l.text_id
        FROM limitation l
        WHERE l.limitation_id NOT IN (
            SELECT limitation_id FROM limitation_indication
        )
    """).fetchall()

    inserted = 0
    for lim_id, vf, vt, tid in lims_without:
        conn.execute(
            "INSERT OR IGNORE INTO limitation_indication "
            "(limitation_id, indication_id, code_source, valid_from, valid_to, text_id) "
            "VALUES (?, ?, 'NOCODE', ?, ?, ?)",
            (lim_id, nocode_id, vf, vt, tid),
        )
        inserted += 1

    conn.commit()

    # Mark XML texts complexity
    # Texts with exactly 1 XML code -> will be handled in step_03
    # Texts with >1 XML code -> XML_MULTI_CODE
    text_to_codes = {}
    rows = conn.execute("""
        SELECT l.text_id, COUNT(DISTINCT li.indication_id) as n_codes
        FROM limitation_indication li
        JOIN limitation l ON l.limitation_id = li.limitation_id
        WHERE li.code_source = 'STRUCTURED_XML'
        GROUP BY l.text_id
    """).fetchall()
    for text_id, n_codes in rows:
        text_to_codes[text_id] = n_codes

    xml_multi = 0
    for text_id, n_codes in text_to_codes.items():
        if n_codes > 1:
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'XML_MULTI_CODE' "
                "WHERE text_id = ?", (text_id,),
            )
            xml_multi += 1

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM limitation_indication").fetchone()[0]
    log.info(f"  NOCODE rows inserted: {inserted}")
    log.info(f"  XML multi-code texts flagged: {xml_multi}")
    log.info(f"  Total limitation_indication: {total}")

    # Verify no limitation is uncovered
    uncovered = conn.execute("""
        SELECT COUNT(*) FROM limitation l
        WHERE l.limitation_id NOT IN (
            SELECT limitation_id FROM limitation_indication
        )
    """).fetchone()[0]
    log.info(f"  Uncovered limitations: {uncovered} (should be 0)")

    conn.close()
    log.info("  Step 02 done.")

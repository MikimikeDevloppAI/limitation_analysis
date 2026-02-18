"""
Step 02: Initialize segments and indications.

- Creates 1 NOCODE indication (code=NULL, name=NULL)
- Creates 1 default segment (index=0, full text) per limitation_text
- For texts with XML codes: segments linked to XML indications (via _text_indication_xml)
- For texts without XML codes: segments linked to NOCODE
- Sets text_complexity = 'XML_MULTI_CODE' for texts with >1 XML code
"""

import logging
import sqlite3

log = logging.getLogger(__name__)


def run(db_path):
    """Run step 02: create segments and link indications."""
    log.info("Step 02: Initialize segments and indications")

    conn = sqlite3.connect(str(db_path))

    # ---- Create NOCODE indication ----
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

    # ---- Build XML code map: text_id -> [(indication_id, code)] ----
    xml_text_codes = {}
    rows = conn.execute(
        "SELECT text_id, indication_id, indication_code "
        "FROM _text_indication_xml"
    ).fetchall()
    for text_id, ind_id, code in rows:
        xml_text_codes.setdefault(text_id, []).append((ind_id, code))
    log.info(f"  XML text-indication mappings: {len(rows)} "
             f"({len(xml_text_codes)} texts)")

    # ---- Create segments ----
    texts = conn.execute(
        "SELECT text_id, description_de, description_fr, description_it "
        "FROM limitation_text"
    ).fetchall()

    xml_single = 0
    xml_multi = 0
    nocode_seg = 0

    for text_id, desc_de, desc_fr, desc_it in texts:
        xml_codes = xml_text_codes.get(text_id)

        if xml_codes and len(xml_codes) == 1:
            # Single XML code: 1 segment linked to that indication
            ind_id, _ = xml_codes[0]
            conn.execute(
                "INSERT INTO limitation_text_segment "
                "(text_id, segment_index, description_de, description_fr, "
                " description_it, indication_id, code_source) "
                "VALUES (?, 0, ?, ?, ?, ?, 'STRUCTURED_XML')",
                (text_id, desc_de, desc_fr, desc_it, ind_id),
            )
            xml_single += 1

        elif xml_codes and len(xml_codes) > 1:
            # Multiple XML codes: 1 segment per code (full text for now)
            for i, (ind_id, _) in enumerate(xml_codes):
                conn.execute(
                    "INSERT INTO limitation_text_segment "
                    "(text_id, segment_index, description_de, description_fr, "
                    " description_it, indication_id, code_source) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'STRUCTURED_XML')",
                    (text_id, i, desc_de, desc_fr, desc_it, ind_id),
                )
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'XML_MULTI_CODE' "
                "WHERE text_id = ?", (text_id,),
            )
            xml_multi += 1

        else:
            # No XML codes: 1 segment with NOCODE
            conn.execute(
                "INSERT INTO limitation_text_segment "
                "(text_id, segment_index, description_de, description_fr, "
                " description_it, indication_id, code_source) "
                "VALUES (?, 0, ?, ?, ?, ?, 'NOCODE')",
                (text_id, desc_de, desc_fr, desc_it, nocode_id),
            )
            nocode_seg += 1

    conn.commit()

    total_segs = conn.execute(
        "SELECT COUNT(*) FROM limitation_text_segment"
    ).fetchone()[0]

    log.info(f"  Segments created: {total_segs}")
    log.info(f"    XML single-code: {xml_single}")
    log.info(f"    XML multi-code:  {xml_multi} texts ({xml_multi} flagged)")
    log.info(f"    NOCODE:          {nocode_seg}")

    conn.close()
    log.info("  Step 02 done.")

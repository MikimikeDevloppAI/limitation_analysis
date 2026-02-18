"""
Step 03c: Propagate indication codes to NOCODE segments.

For pre-2023 texts that have no bold header and no embedded code (code_source='NOCODE'),
we try to propagate an indication_id from other periods using two strategies:

Phase A — SAME_LIM_CODE:
    If the same limitation_code appears on another limitation whose segment already
    has an indication with a real code, propagate that indication_id.

Phase B — SINGLE_SKU_CODE:
    If a SKU (GTIN) linked to the NOCODE segment has exactly one distinct
    indication_code across all its other limitations, propagate that indication_id.

Each propagated segment keeps a distinct code_source so the origin is always traceable.

Depends on: step_01, step_02, step_03, step_03b.
"""

import logging
import sqlite3

log = logging.getLogger(__name__)


def run(db_path):
    """Run step 03c: propagate indication codes to NOCODE segments."""
    log.info("Step 03c: Propagate indication codes to NOCODE segments")

    conn = sqlite3.connect(str(db_path))

    counters = {
        "nocode_before": 0,
        "same_lim_code": 0,
        "single_sku_code": 0,
        "nocode_after": 0,
    }

    # ------------------------------------------------------------------
    # Count NOCODE segments before
    # ------------------------------------------------------------------
    counters["nocode_before"] = conn.execute(
        "SELECT COUNT(*) FROM limitation_text_segment WHERE code_source = 'NOCODE'"
    ).fetchone()[0]
    log.info(f"  NOCODE segments before: {counters['nocode_before']}")

    # ------------------------------------------------------------------
    # Phase A: Propagation by same limitation_code
    # ------------------------------------------------------------------
    # For each NOCODE segment, find the limitation_code(s) pointing to its text_id.
    # Then find other limitations with the SAME limitation_code that have a segment
    # with a real indication (code_source != 'NOCODE', indication has a code).
    # ------------------------------------------------------------------
    log.info("  Phase A: same limitation_code...")

    phase_a_matches = conn.execute("""
        WITH nocode_segs AS (
            SELECT seg.segment_id, seg.text_id
            FROM limitation_text_segment seg
            WHERE seg.code_source = 'NOCODE'
        ),
        -- For each nocode text, get its limitation_codes
        nocode_lim_codes AS (
            SELECT DISTINCT ns.segment_id, ns.text_id, lim.limitation_code
            FROM nocode_segs ns
            JOIN limitation lim ON lim.text_id = ns.text_id
        ),
        -- For each limitation_code, find other limitations with coded segments
        coded_peers AS (
            SELECT nlc.segment_id,
                   nlc.text_id,
                   nlc.limitation_code,
                   seg2.indication_id,
                   ind.indication_code
            FROM nocode_lim_codes nlc
            JOIN limitation lim2 ON lim2.limitation_code = nlc.limitation_code
                AND lim2.text_id != nlc.text_id
            JOIN limitation_text_segment seg2 ON seg2.text_id = lim2.text_id
                AND seg2.code_source != 'NOCODE'
            JOIN indication ind ON ind.indication_id = seg2.indication_id
                AND ind.indication_code IS NOT NULL
        )
        SELECT segment_id, indication_id
        FROM (
            SELECT segment_id, indication_id,
                   ROW_NUMBER() OVER (PARTITION BY segment_id ORDER BY indication_code) as rn
            FROM coded_peers
        )
        WHERE rn = 1
    """).fetchall()

    for segment_id, indication_id in phase_a_matches:
        conn.execute(
            "UPDATE limitation_text_segment "
            "SET indication_id = ?, code_source = 'SAME_LIM_CODE' "
            "WHERE segment_id = ? AND code_source = 'NOCODE'",
            (indication_id, segment_id),
        )
        counters["same_lim_code"] += 1

    conn.commit()
    log.info(f"    Propagated: {counters['same_lim_code']}")

    # ------------------------------------------------------------------
    # Phase B: Propagation by single SKU indication code
    # ------------------------------------------------------------------
    # For remaining NOCODE segments, find GTINs linked to the same limitation.
    # If ALL GTINs for that text agree on exactly 1 indication_code,
    # propagate it.
    # ------------------------------------------------------------------
    log.info("  Phase B: single SKU indication code...")

    phase_b_matches = conn.execute("""
        WITH nocode_segs AS (
            SELECT seg.segment_id, seg.text_id
            FROM limitation_text_segment seg
            WHERE seg.code_source = 'NOCODE'
        ),
        -- Get GTINs for each nocode text
        nocode_gtins AS (
            SELECT DISTINCT ns.segment_id, ns.text_id, sl.gtin
            FROM nocode_segs ns
            JOIN limitation lim ON lim.text_id = ns.text_id
            JOIN sku_limitation sl ON sl.limitation_id = lim.limitation_id
        ),
        -- For each GTIN, find all indication_codes from other limitations
        gtin_codes AS (
            SELECT ng.segment_id, ng.text_id, ng.gtin,
                   ind.indication_id, ind.indication_code
            FROM nocode_gtins ng
            JOIN sku_limitation sl2 ON sl2.gtin = ng.gtin
            JOIN limitation lim2 ON lim2.limitation_id = sl2.limitation_id
            JOIN limitation_text_segment seg2 ON seg2.text_id = lim2.text_id
                AND seg2.code_source != 'NOCODE'
            JOIN indication ind ON ind.indication_id = seg2.indication_id
                AND ind.indication_code IS NOT NULL
        ),
        -- Keep only segments where all GTINs agree on exactly 1 code
        seg_unique_code AS (
            SELECT segment_id, MIN(indication_id) as indication_id
            FROM gtin_codes
            GROUP BY segment_id
            HAVING COUNT(DISTINCT indication_code) = 1
        )
        SELECT segment_id, indication_id
        FROM seg_unique_code
    """).fetchall()

    for segment_id, indication_id in phase_b_matches:
        conn.execute(
            "UPDATE limitation_text_segment "
            "SET indication_id = ?, code_source = 'SINGLE_SKU_CODE' "
            "WHERE segment_id = ? AND code_source = 'NOCODE'",
            (indication_id, segment_id),
        )
        counters["single_sku_code"] += 1

    conn.commit()

    log.info(f"    Propagated: {counters['single_sku_code']}")

    # ------------------------------------------------------------------
    # Final count
    # ------------------------------------------------------------------
    counters["nocode_after"] = conn.execute(
        "SELECT COUNT(*) FROM limitation_text_segment WHERE code_source = 'NOCODE'"
    ).fetchone()[0]

    log.info("  Results:")
    log.info(f"    NOCODE before:     {counters['nocode_before']}")
    log.info(f"    SAME_LIM_CODE:     {counters['same_lim_code']}")
    log.info(f"    SINGLE_SKU_CODE:   {counters['single_sku_code']}")
    log.info(f"    NOCODE after:      {counters['nocode_after']}")
    log.info(f"    Reduction:         {counters['nocode_before'] - counters['nocode_after']}")

    conn.close()
    log.info("  Step 03c done.")

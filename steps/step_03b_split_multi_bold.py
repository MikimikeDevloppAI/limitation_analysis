"""
Step 03b: Split MULTI_BOLD limitation texts into per-indication segments.

Phase A: Reclassify false positives (0-2 non-structural bolds → NONE or SIMPLE)
Phase B: Split genuine multi-bold texts at <b>Name</b> boundaries
Phase C: Replace the default segment with N segments, match each to an indication

Depends on step_03 (uses text_complexity, fusion_cache, indication table).
"""

import logging
import re
import sqlite3

log = logging.getLogger(__name__)

# Import shared utilities from step_03
from steps.step_03_extract_bolds import (
    _is_structural_name,
    _clean_indication_name,
    _normalize_name,
    _extract_bold_names,
    _extract_codes_from_text,
    _extract_qualifier,
    _get_dossier_from_code,
    _get_or_create_indication_with_code,
    _get_or_create_indication_by_name,
    RE_BOLD,
)


# ============================================================
# Splitting functions
# ============================================================

def _find_segment_boundaries(text):
    """Find split positions for non-structural bold headers in a text.

    Returns list of (start_pos, bold_name) where start_pos is the position
    where the segment begins (at the <br> before the bold, or 0).
    """
    if not text:
        return []

    boundaries = []
    for m in RE_BOLD.finditer(text):
        raw_name = m.group(1)
        if _is_structural_name(raw_name):
            continue

        bold_start = m.start()

        # Walk backwards to find the start of the <br> sequence before this bold
        pos = bold_start
        before = text[:pos].rstrip()
        while True:
            stripped = False
            for suffix in ("<br />", "<br/>", "<br>"):
                if before.endswith(suffix):
                    before = before[:-len(suffix)].rstrip()
                    stripped = True
                    break
            if not stripped:
                break
        # pos = end of content before the <br> sequence
        seg_start = len(before)
        if seg_start < 0:
            seg_start = 0

        name = _clean_indication_name(raw_name)
        boundaries.append((seg_start, name))

    return boundaries


def _split_text_at_bolds(text):
    """Split a text at non-structural bold boundaries.

    Returns:
        intro: str (text before first indication bold, may be empty)
        segments: list of {"name": str, "body": str}
    """
    if not text:
        return "", []

    boundaries = _find_segment_boundaries(text)
    if not boundaries:
        return text, []

    intro = text[:boundaries[0][0]]
    segments = []
    for i, (start, name) in enumerate(boundaries):
        end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(text)
        body = text[start:end]
        segments.append({"name": name, "body": body})

    return intro, segments


def _align_segments(segs_fr, segs_de, segs_it):
    """Align segments across languages by ordinal position (FR = canonical).

    Returns list of dicts:
        [{"name_fr": str, "name_de": str|None, "name_it": str|None,
          "body_fr": str, "body_de": str|None, "body_it": str|None}, ...]
    """
    n = len(segs_fr)
    aligned = []
    for i in range(n):
        seg = {
            "name_fr": segs_fr[i]["name"],
            "body_fr": segs_fr[i]["body"],
            "name_de": segs_de[i]["name"] if i < len(segs_de) else None,
            "body_de": segs_de[i]["body"] if i < len(segs_de) else None,
            "name_it": segs_it[i]["name"] if i < len(segs_it) else None,
            "body_it": segs_it[i]["body"] if i < len(segs_it) else None,
        }
        aligned.append(seg)
    return aligned


# ============================================================
# Main logic
# ============================================================

def run(db_path):
    """Run step 03b: split MULTI_BOLD texts into per-indication segments."""
    log.info("Step 03b: Split MULTI_BOLD texts into segments")

    conn = sqlite3.connect(str(db_path))

    # ---- Lookup structures ----

    # text_id -> bag_dossier_no
    text_to_dossier = {}
    rows = conn.execute("""
        SELECT DISTINCT l.text_id, s.bag_dossier_no
        FROM limitation l
        JOIN sku_limitation sl ON sl.limitation_id = l.limitation_id
        JOIN sku s ON s.gtin = sl.gtin
        WHERE s.bag_dossier_no IS NOT NULL
    """).fetchall()
    for text_id, dossier in rows:
        if text_id not in text_to_dossier:
            text_to_dossier[text_id] = dossier

    # Fusion cache from all named indications
    fusion_cache = {}
    for ind_id, dossier, name_fr in conn.execute(
        "SELECT indication_id, bag_dossier_no, indication_name_fr "
        "FROM indication WHERE bag_dossier_no IS NOT NULL "
        "AND indication_name_fr IS NOT NULL"
    ).fetchall():
        norm = _normalize_name(name_fr)
        if norm:
            fusion_cache[(dossier, norm)] = ind_id
    log.info(f"  Fusion cache: {len(fusion_cache)} entries")

    # NOCODE indication_id
    nocode_row = conn.execute(
        "SELECT indication_id FROM indication WHERE indication_code IS NULL "
        "AND indication_name_de IS NULL AND indication_name_fr IS NULL"
    ).fetchone()
    nocode_id = nocode_row[0] if nocode_row else None

    # Load all MULTI_BOLD texts
    texts = conn.execute(
        "SELECT text_id, description_de, description_fr, description_it "
        "FROM limitation_text WHERE text_complexity = 'MULTI_BOLD'"
    ).fetchall()
    log.info(f"  MULTI_BOLD texts to process: {len(texts)}")

    counters = {
        "reclassified_none": 0,
        "reclassified_simple": 0,
        "split": 0,
        "segments_created": 0,
        "fused": 0,
        "created": 0,
    }

    for text_id, desc_de, desc_fr, desc_it in texts:
        # Count non-structural bolds
        bolds_fr = _extract_bold_names(desc_fr)
        n_bolds = len(bolds_fr)

        # ---- Phase A: Reclassify false positives ----
        if n_bolds == 0:
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'NONE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["reclassified_none"] += 1
            continue

        if n_bolds <= 2:
            # Treat as SIMPLE — update the default segment with the indication
            bolds_de = _extract_bold_names(desc_de)
            bolds_it = _extract_bold_names(desc_it)

            if n_bolds == 2:
                name_fr = " - ".join(bolds_fr[:2])
                name_de = " - ".join(bolds_de[:2]) if len(bolds_de) >= 2 else (bolds_de[0] if bolds_de else None)
                name_it = " - ".join(bolds_it[:2]) if len(bolds_it) >= 2 else (bolds_it[0] if bolds_it else None)
            else:
                # Check for qualifier
                qual_fr = _extract_qualifier(desc_fr, "fr")
                qual_de = _extract_qualifier(desc_de, "de")
                qual_it = _extract_qualifier(desc_it, "it")
                if qual_fr or qual_de or qual_it:
                    name_fr = f"{bolds_fr[0]} - {qual_fr}" if qual_fr else bolds_fr[0]
                    name_de = f"{bolds_de[0]} - {qual_de}" if bolds_de and qual_de else (bolds_de[0] if bolds_de else None)
                    name_it = f"{bolds_it[0]} - {qual_it}" if bolds_it and qual_it else (bolds_it[0] if bolds_it else None)
                else:
                    name_fr = bolds_fr[0]
                    name_de = bolds_de[0] if bolds_de else None
                    name_it = bolds_it[0] if bolds_it else None

            # Try to extract embedded code
            codes = _extract_codes_from_text(desc_de, desc_fr, desc_it)
            dossier = text_to_dossier.get(text_id)

            if codes:
                code = codes[0]
                dossier_from_code = _get_dossier_from_code(code)
                ind_id = _get_or_create_indication_with_code(
                    conn, code, dossier_from_code or dossier,
                    name_de, name_fr, name_it, fusion_cache, counters,
                )
                code_source = "TEXT_EMBEDDED"
            else:
                ind_id = _get_or_create_indication_by_name(
                    conn, dossier, name_de, name_fr, name_it,
                    fusion_cache, counters,
                )
                code_source = "BOLD_HEADER"

            # Update the default segment
            conn.execute(
                "UPDATE limitation_text_segment "
                "SET indication_id = ?, code_source = ? "
                "WHERE text_id = ? AND segment_index = 0",
                (ind_id, code_source, text_id),
            )
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'SIMPLE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["reclassified_simple"] += 1
            continue

        # ---- Phase B: Split (3+ bolds) ----
        intro_fr, segs_fr = _split_text_at_bolds(desc_fr)
        intro_de, segs_de = _split_text_at_bolds(desc_de)
        intro_it, segs_it = _split_text_at_bolds(desc_it)

        if not segs_fr:
            # No segments found (shouldn't happen with 3+ bolds)
            log.warning(f"  text_id={text_id}: 3+ bolds but no segments from FR split")
            continue

        aligned = _align_segments(segs_fr, segs_de, segs_it)
        dossier = text_to_dossier.get(text_id)

        # ---- Phase C: Replace default segment with N segments ----

        # Delete the default segment (index=0, full text)
        conn.execute(
            "DELETE FROM limitation_text_segment "
            "WHERE text_id = ? AND segment_index = 0",
            (text_id,),
        )

        for i, seg in enumerate(aligned):
            # Extract codes from segment text
            codes = _extract_codes_from_text(
                seg["body_de"], seg["body_fr"], seg["body_it"]
            )

            # Match to indication
            if codes:
                code = codes[0]
                dossier_from_code = _get_dossier_from_code(code)
                ind_id = _get_or_create_indication_with_code(
                    conn, code, dossier_from_code or dossier,
                    seg["name_de"], seg["name_fr"], seg["name_it"],
                    fusion_cache, counters,
                )
                code_source = "TEXT_EMBEDDED"
            else:
                ind_id = _get_or_create_indication_by_name(
                    conn, dossier,
                    seg["name_de"], seg["name_fr"], seg["name_it"],
                    fusion_cache, counters,
                )
                code_source = "BOLD_HEADER"

            # Insert segment
            conn.execute(
                "INSERT INTO limitation_text_segment "
                "(text_id, segment_index, description_de, description_fr, "
                " description_it, indication_id, code_source) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (text_id, i,
                 seg["body_de"], seg["body_fr"], seg["body_it"],
                 ind_id, code_source),
            )
            counters["segments_created"] += 1

        # Mark text as split
        conn.execute(
            "UPDATE limitation_text SET text_complexity = 'MULTI_BOLD_SPLIT' "
            "WHERE text_id = ?", (text_id,),
        )
        counters["split"] += 1

    conn.commit()

    log.info("  Results:")
    log.info(f"    Reclassified to NONE:   {counters['reclassified_none']}")
    log.info(f"    Reclassified to SIMPLE: {counters['reclassified_simple']}")
    log.info(f"    Split into segments:    {counters['split']}")
    log.info(f"    Segments created:       {counters['segments_created']}")
    log.info(f"    Indications fused:      {counters['fused']}")
    log.info(f"    Indications created:    {counters['created']}")

    # Verify
    remaining = conn.execute(
        "SELECT COUNT(*) FROM limitation_text "
        "WHERE text_complexity = 'MULTI_BOLD'"
    ).fetchone()[0]
    log.info(f"    MULTI_BOLD remaining:   {remaining} (should be 0)")

    no_ind = conn.execute(
        "SELECT COUNT(*) FROM limitation_text_segment WHERE indication_id IS NULL"
    ).fetchone()[0]
    log.info(f"    Segments w/o indication: {no_ind} (should be 0)")

    conn.close()
    log.info("  Step 03b done.")

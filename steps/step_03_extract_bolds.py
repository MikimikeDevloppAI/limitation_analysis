"""
Step 03: Extract bold indication names and embedded codes from limitation texts.

For each limitation_text:
1. Extract non-structural bold headers (<b>Name</b>) from DE/FR/IT texts
2. Extract embedded indication codes (XXXXX.XX) from text via regex
3. Determine text_complexity: SIMPLE, MULTI_BOLD, MULTI_CODE, NONE, XML_MULTI_CODE

For SIMPLE texts (1 bold and/or 1 code):
- Post-2023 (XML code exists): update the indication name from the bold header
- Pre-2023 (no XML code): create or find an indication and link it

Fusion logic: same (bag_dossier_no, normalized_name_fr) -> same indication row.
This ensures that the same indication across multiple texts/years is not duplicated.
"""

import html
import logging
import re
import sqlite3

log = logging.getLogger(__name__)

# ============================================================
# Regex patterns & constants
# ============================================================

RE_NUMERIC = re.compile(r"(\d{5}\.\d{2})\.?\b")
RE_BOLD = re.compile(r"<b>(.+?)</b>")
RE_HEADER_BOLD = re.compile(
    r"(?:^|<br\s*/?>[\s\n]*(?:<br\s*/?>[\s\n]*)*)"
    r"(<b>.+?</b>)",
    re.MULTILINE,
)
RE_TRAILING_CODE = re.compile(r"\s*\d{5}\.\d{2}\s*$")

# Inline qualifier patterns per language (mid-line, not caught by RE_HEADER_BOLD)
# FR: "En <b>monothérapie</b>", "En <b>association avec...</b>"
# DE: "Als <b>Monotherapie</b>", "In <b>Kombination mit...</b>"
# IT: "Come <b>monoterapia</b>", "In <b>combinazione con...</b>"
RE_QUALIFIER = {
    "fr": re.compile(r"(?:^|[.\s])En\s+<b>(.+?)</b>", re.IGNORECASE),
    "de": re.compile(r"(?:^|[.\s])(?:Als|In)\s+<b>(.+?)</b>", re.IGNORECASE),
    "it": re.compile(r"(?:^|[.\s])(?:Come|In)\s+<b>(.+?)</b>", re.IGNORECASE),
}

# Age-range patterns: "4 à 24 mois:", "4-24 Monate:", "À partir de 12 ans:"
RE_AGE_RANGE = re.compile(
    r"^\d+\s*[àa\-]\s*\d+\s*(mois|Monate|ans|Jahre|mesi|anni)",
    re.IGNORECASE,
)
RE_AGE_FROM = re.compile(
    r"^[ÀAa]?\s*partir\s+de\s+\d+|^Ab\s+\d+\s*(Jahr|Monat)",
    re.IGNORECASE,
)

STRUCTURAL_BOLD_NAMES = {
    "UND", "ODER", "AND", "OR", "ET", "OU",
    "und", "oder", "and", "or", "et", "ou",
}

STRUCTURAL_PREFIXES = (
    # DE
    "Vor Therapiebeginn", "Vor Beginn", "Vor der Behandlung",
    "Therapiefortführung", "Therapiefortsetzung",
    "Therapieabbruch", "Behandlungsabbruch",
    "nach AJCC", "Nach AJCC", "Gemäss AJCC",
    "Fr. ", "CHF ",
    "Maximal ",
    "Dosierungsschema",
    "Für alle vergütungspflichtigen",
    "Rückerstattungen",
    "Erwachsene",
    "Kriterien für die Vergütung",
    "Für die", "Gültig für",
    "Weiterführung der Therapie",
    "Limitation gültig",
    "Austausch Referenzpräparat",
    "Indikationsübergreifend",
    # FR
    "Pour les", "Valable pour",
    "Les conditions",
    "Poursuite du traitement",
    "Selon l'AJCC", "Selon la classification",
    "Avant le début", "Avant de commencer",
    "Arrêt du traitement",
    "Limitation valide",
    "Substitution préparation",
    # IT
    "Secondo l'AJCC", "Secondo la classificazione",
    "Prima dell'inizio", "Prima di iniziare",
    "Interruzione del trattamento",
    "Limitazione valida",
    "Sostituzione preparato",
)

# Patterns to extract indication codes from free text (DE/FR/IT)
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
# Utility functions
# ============================================================

def _is_structural_name(name):
    """Return True if the bold name is a structural marker, not an indication."""
    if not name:
        return True
    stripped = name.strip().rstrip(":")
    if stripped in STRUCTURAL_BOLD_NAMES:
        return True
    if stripped.startswith(STRUCTURAL_PREFIXES):
        return True
    if stripped.replace(".", "").replace(",", "").isdigit():
        return True
    if len(stripped) <= 3 and stripped.islower():
        return True
    # Age ranges: "4 à 24 mois:", "4-24 Monate:", "À partir de 12 ans:"
    if RE_AGE_RANGE.match(stripped):
        return True
    if RE_AGE_FROM.match(stripped):
        return True
    return False


def _clean_indication_name(name):
    """Clean an indication name: strip trailing code, HTML tags, extra whitespace."""
    if not name:
        return name
    cleaned = RE_TRAILING_CODE.sub("", name)
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    return cleaned.strip()


def _normalize_name(name):
    """Normalize an indication name for fusion matching: lowercase, strip code/HTML/whitespace."""
    if not name:
        return ""
    n = RE_TRAILING_CODE.sub("", name)
    n = re.sub(r"<[^>]+>", "", n)
    n = re.sub(r"\s+", " ", n).strip().lower().rstrip(":")
    return n


def _extract_bold_names(text):
    """Extract non-structural bold header names from a text.
    Returns list of cleaned names.
    """
    if not text:
        return []
    headers = RE_HEADER_BOLD.findall(text)
    names = []
    for h in headers:
        m = RE_BOLD.search(h)
        if m:
            raw_name = m.group(1)
            if not _is_structural_name(raw_name):
                names.append(_clean_indication_name(raw_name))
    return names


def _compose_name(bold_list, n=2):
    """Compose an indication name from the first N bold names.
    E.g. ['Mélanome', 'monothérapie'] -> 'Mélanome - monothérapie'
    Returns a single composed name, or the first bold if n=1.
    """
    parts = bold_list[:n]
    if len(parts) <= 1:
        return parts[0] if parts else None
    return " - ".join(parts)


def _extract_qualifier(text, lang="fr"):
    """Extract an inline qualifier from therapy-mode bold pattern.
    lang: 'fr', 'de', or 'it'
    Returns cleaned qualifier string or None.
    """
    if not text:
        return None
    pattern = RE_QUALIFIER.get(lang)
    if not pattern:
        return None
    m = pattern.search(text)
    if m:
        raw = m.group(1)
        if not _is_structural_name(raw):
            return _clean_indication_name(raw)
    return None


def _build_name(bolds, qualifier):
    """Build an indication name from bold headers and an optional qualifier.

    - 0 bolds + qualifier -> qualifier alone
    - 1 bold + qualifier  -> 'bold - qualifier'
    - 1 bold, no qualifier -> bold alone
    - 2+ bolds            -> 'bold1 - bold2' (qualifier ignored, already distinct)
    - 0 bolds, no qualifier -> None
    """
    if len(bolds) >= 2:
        return " - ".join(bolds[:2])
    if len(bolds) == 1:
        if qualifier:
            return f"{bolds[0]} - {qualifier}"
        return bolds[0]
    # 0 bolds
    if qualifier:
        return qualifier
    return None


def _extract_codes_from_text(desc_de, desc_fr, desc_it):
    """Extract indication codes (XXXXX.XX) from free text via regex patterns."""
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


def _get_dossier_from_code(code):
    """Extract bag_dossier_no from indication code: '20461.07' -> '20461'."""
    if code and "." in code:
        return code.split(".")[0]
    return None


# ============================================================
# Main logic
# ============================================================

def run(db_path):
    """Run step 03: extract bold names and embedded codes, with fusion.

    Two passes:
    1. XML texts (post-2023): update indication names from bold headers
    2. Pre-2023 texts: create/fuse indications from bolds and embedded codes

    Between passes, the fusion_cache is populated from all named indications
    so pre-2023 texts can fuse with XML indications that just got their names.
    """
    log.info("Step 03: Extract bold names and codes from limitation texts")

    conn = sqlite3.connect(str(db_path))

    # ---- Build lookup structures ----

    # text_ids that have XML codes (from limitation_text_segment)
    xml_text_ids = set()
    rows = conn.execute(
        "SELECT DISTINCT text_id FROM limitation_text_segment "
        "WHERE code_source = 'STRUCTURED_XML'"
    ).fetchall()
    for r in rows:
        xml_text_ids.add(r[0])

    # text_id -> list of XML indication_ids (for single-code texts)
    xml_single_code = {}   # text_id -> indication_id
    xml_multi_code = set()
    text_ind_map = {}
    for text_id, ind_id in conn.execute(
        "SELECT text_id, indication_id FROM limitation_text_segment "
        "WHERE code_source = 'STRUCTURED_XML'"
    ).fetchall():
        text_ind_map.setdefault(text_id, set()).add(ind_id)
    for text_id, ind_ids in text_ind_map.items():
        if len(ind_ids) == 1:
            xml_single_code[text_id] = next(iter(ind_ids))
        else:
            xml_multi_code.add(text_id)

    # text_id -> bag_dossier_no (via limitation -> sku_limitation -> sku)
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

    # Fusion cache: (bag_dossier_no, normalized_name_fr) -> indication_id
    fusion_cache = {}

    # Get all texts to process
    texts = conn.execute(
        "SELECT text_id, description_de, description_fr, description_it "
        "FROM limitation_text WHERE text_complexity IS NULL OR text_complexity = ''"
    ).fetchall()

    counters = {
        "xml_named": 0,
        "xml_2bold": 0,
        "pre_1bold_1code": 0,
        "pre_2bold_1code": 0,
        "pre_1bold_nocode": 0,
        "pre_2bold_nocode": 0,
        "pre_0bold_1code": 0,
        "pre_multi_bold": 0,
        "pre_multi_code": 0,
        "pre_none": 0,
        "fused": 0,
        "created": 0,
    }

    # ================================================================
    # PASS 1: XML texts (post-2023) — update indication names
    # ================================================================
    log.info("  Pass 1: XML texts (naming indications)...")

    for text_id, desc_de, desc_fr, desc_it in texts:
        if text_id not in xml_text_ids:
            continue

        bold_de = _extract_bold_names(desc_de)
        bold_fr = _extract_bold_names(desc_fr)
        bold_it = _extract_bold_names(desc_it)
        n_bolds = max(len(bold_de), len(bold_fr), len(bold_it))

        if text_id in xml_multi_code:
            # Already flagged in step_02
            pass

        elif text_id in xml_single_code and n_bolds >= 1:
            ind_id = xml_single_code[text_id]

            # Compose name: if 2 bolds, concatenate (e.g. "Mélanome - monothérapie")
            if n_bolds == 2:
                name_de = _compose_name(bold_de, 2) if len(bold_de) >= 2 else (bold_de[0] if bold_de else None)
                name_fr = _compose_name(bold_fr, 2) if len(bold_fr) >= 2 else (bold_fr[0] if bold_fr else None)
                name_it = _compose_name(bold_it, 2) if len(bold_it) >= 2 else (bold_it[0] if bold_it else None)
                counters["xml_2bold"] += 1
            elif n_bolds == 1:
                # Check for inline qualifier: "En <b>monothérapie</b>"
                qual_fr = _extract_qualifier(desc_fr, "fr")
                qual_de = _extract_qualifier(desc_de, "de")
                qual_it = _extract_qualifier(desc_it, "it")
                if qual_fr or qual_de or qual_it:
                    name_fr = f"{bold_fr[0]} - {qual_fr}" if bold_fr and qual_fr else (bold_fr[0] if bold_fr else None)
                    name_de = f"{bold_de[0]} - {qual_de}" if bold_de and qual_de else (bold_de[0] if bold_de else None)
                    name_it = f"{bold_it[0]} - {qual_it}" if bold_it and qual_it else (bold_it[0] if bold_it else None)
                    counters["xml_2bold"] += 1
                else:
                    name_de = bold_de[0] if bold_de else None
                    name_fr = bold_fr[0] if bold_fr else None
                    name_it = bold_it[0] if bold_it else None
            else:
                name_de = bold_de[0] if bold_de else None
                name_fr = bold_fr[0] if bold_fr else None
                name_it = bold_it[0] if bold_it else None

            conn.execute(
                "UPDATE indication SET "
                "indication_name_de = COALESCE(indication_name_de, ?), "
                "indication_name_fr = COALESCE(indication_name_fr, ?), "
                "indication_name_it = COALESCE(indication_name_it, ?), "
                "name_source = COALESCE(name_source, 'BOLD_HEADER') "
                "WHERE indication_id = ?",
                (name_de, name_fr, name_it, ind_id),
            )
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'SIMPLE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["xml_named"] += 1

        else:
            # XML code exists but 0 bolds — simple, no name to add
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'SIMPLE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["xml_named"] += 1

    conn.commit()

    # ================================================================
    # Populate fusion_cache from ALL named indications (including just-named XML ones)
    # ================================================================
    for ind_id, dossier, name_fr in conn.execute(
        "SELECT indication_id, bag_dossier_no, indication_name_fr "
        "FROM indication WHERE bag_dossier_no IS NOT NULL AND indication_name_fr IS NOT NULL"
    ).fetchall():
        norm = _normalize_name(name_fr)
        if norm:
            fusion_cache[(dossier, norm)] = ind_id

    log.info(f"  Fusion cache populated: {len(fusion_cache)} entries")

    # ================================================================
    # PASS 2: Pre-2023 texts — create/fuse indications
    # ================================================================
    log.info("  Pass 2: Pre-2023 texts (creating/fusing indications)...")

    for text_id, desc_de, desc_fr, desc_it in texts:
        if text_id in xml_text_ids:
            continue

        bold_de = _extract_bold_names(desc_de)
        bold_fr = _extract_bold_names(desc_fr)
        bold_it = _extract_bold_names(desc_it)
        n_bolds = max(len(bold_de), len(bold_fr), len(bold_it))

        codes_in_text = _extract_codes_from_text(desc_de, desc_fr, desc_it)
        n_codes = len(codes_in_text)

        if n_bolds > 2:
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'MULTI_BOLD' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["pre_multi_bold"] += 1

        elif n_codes > 1:
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'MULTI_CODE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["pre_multi_code"] += 1

        elif n_bolds == 2 and n_codes == 1:
            # 2 bolds + 1 code -> composed name with code
            code = codes_in_text[0]
            name_de = _compose_name(bold_de, 2) if len(bold_de) >= 2 else (bold_de[0] if bold_de else None)
            name_fr = _compose_name(bold_fr, 2) if len(bold_fr) >= 2 else (bold_fr[0] if bold_fr else None)
            name_it = _compose_name(bold_it, 2) if len(bold_it) >= 2 else (bold_it[0] if bold_it else None)
            dossier = _get_dossier_from_code(code)

            ind_id = _get_or_create_indication_with_code(
                conn, code, dossier, name_de, name_fr, name_it, fusion_cache, counters
            )
            _link_indication_to_text(conn, text_id, ind_id, "TEXT_EMBEDDED")
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'SIMPLE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["pre_2bold_1code"] += 1

        elif n_bolds == 2 and n_codes == 0:
            # 2 bolds, no code -> composed name
            name_de = _compose_name(bold_de, 2) if len(bold_de) >= 2 else (bold_de[0] if bold_de else None)
            name_fr = _compose_name(bold_fr, 2) if len(bold_fr) >= 2 else (bold_fr[0] if bold_fr else None)
            name_it = _compose_name(bold_it, 2) if len(bold_it) >= 2 else (bold_it[0] if bold_it else None)
            dossier = text_to_dossier.get(text_id)

            ind_id = _get_or_create_indication_by_name(
                conn, dossier, name_de, name_fr, name_it, fusion_cache, counters
            )
            _link_indication_to_text(conn, text_id, ind_id, "BOLD_HEADER")
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'SIMPLE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["pre_2bold_nocode"] += 1

        elif n_bolds == 1 and n_codes == 1:
            # 1 bold + 1 code -> complete indication (check for inline qualifier)
            code = codes_in_text[0]
            qual_fr = _extract_qualifier(desc_fr, "fr")
            qual_de = _extract_qualifier(desc_de, "de")
            qual_it = _extract_qualifier(desc_it, "it")
            if qual_fr or qual_de or qual_it:
                name_fr = f"{bold_fr[0]} - {qual_fr}" if bold_fr and qual_fr else (bold_fr[0] if bold_fr else None)
                name_de = f"{bold_de[0]} - {qual_de}" if bold_de and qual_de else (bold_de[0] if bold_de else None)
                name_it = f"{bold_it[0]} - {qual_it}" if bold_it and qual_it else (bold_it[0] if bold_it else None)
            else:
                name_de = bold_de[0] if bold_de else None
                name_fr = bold_fr[0] if bold_fr else None
                name_it = bold_it[0] if bold_it else None
            dossier = _get_dossier_from_code(code)

            ind_id = _get_or_create_indication_with_code(
                conn, code, dossier, name_de, name_fr, name_it, fusion_cache, counters
            )
            _link_indication_to_text(conn, text_id, ind_id, "TEXT_EMBEDDED")
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'SIMPLE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["pre_1bold_1code"] += 1

        elif n_bolds == 1 and n_codes == 0:
            # 1 bold, no code -> indication with name only (check for inline qualifier)
            qual_fr = _extract_qualifier(desc_fr, "fr")
            qual_de = _extract_qualifier(desc_de, "de")
            qual_it = _extract_qualifier(desc_it, "it")
            if qual_fr or qual_de or qual_it:
                name_fr = f"{bold_fr[0]} - {qual_fr}" if bold_fr and qual_fr else (bold_fr[0] if bold_fr else None)
                name_de = f"{bold_de[0]} - {qual_de}" if bold_de and qual_de else (bold_de[0] if bold_de else None)
                name_it = f"{bold_it[0]} - {qual_it}" if bold_it and qual_it else (bold_it[0] if bold_it else None)
            else:
                name_de = bold_de[0] if bold_de else None
                name_fr = bold_fr[0] if bold_fr else None
                name_it = bold_it[0] if bold_it else None
            dossier = text_to_dossier.get(text_id)

            ind_id = _get_or_create_indication_by_name(
                conn, dossier, name_de, name_fr, name_it, fusion_cache, counters
            )
            _link_indication_to_text(conn, text_id, ind_id, "BOLD_HEADER")
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'SIMPLE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["pre_1bold_nocode"] += 1

        elif n_bolds == 0 and n_codes == 1:
            # No bold, 1 code -> indication with code only
            code = codes_in_text[0]
            dossier = _get_dossier_from_code(code)

            ind_id = _get_or_create_indication_with_code(
                conn, code, dossier, None, None, None, fusion_cache, counters
            )
            _link_indication_to_text(conn, text_id, ind_id, "TEXT_EMBEDDED")
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'SIMPLE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["pre_0bold_1code"] += 1

        else:
            # No bold, no code -> nothing
            conn.execute(
                "UPDATE limitation_text SET text_complexity = 'NONE' "
                "WHERE text_id = ?", (text_id,),
            )
            counters["pre_none"] += 1

    conn.commit()

    log.info("  Results:")
    log.info(f"    XML texts named:              {counters['xml_named']} ({counters['xml_2bold']} with 2-bold name)")
    log.info(f"    Pre-2023: 1 bold + 1 code:    {counters['pre_1bold_1code']}")
    log.info(f"    Pre-2023: 2 bold + 1 code:    {counters['pre_2bold_1code']}")
    log.info(f"    Pre-2023: 1 bold, no code:    {counters['pre_1bold_nocode']}")
    log.info(f"    Pre-2023: 2 bold, no code:    {counters['pre_2bold_nocode']}")
    log.info(f"    Pre-2023: 0 bold + 1 code:    {counters['pre_0bold_1code']}")
    log.info(f"    Pre-2023: multi-bold (flag):  {counters['pre_multi_bold']}")
    log.info(f"    Pre-2023: multi-code (flag):  {counters['pre_multi_code']}")
    log.info(f"    Pre-2023: none found:         {counters['pre_none']}")
    log.info(f"    Indications fused:            {counters['fused']}")
    log.info(f"    Indications created:          {counters['created']}")

    # indication table stats
    ind_total = conn.execute("SELECT COUNT(*) FROM indication").fetchone()[0]
    ind_with_code = conn.execute(
        "SELECT COUNT(*) FROM indication WHERE indication_code IS NOT NULL"
    ).fetchone()[0]
    ind_with_name = conn.execute(
        "SELECT COUNT(*) FROM indication WHERE indication_name_fr IS NOT NULL"
    ).fetchone()[0]
    log.info(f"    Indications total: {ind_total} "
             f"({ind_with_code} with code, {ind_with_name} with name)")

    conn.close()
    log.info("  Step 03 done.")


# ============================================================
# Helper functions
# ============================================================

def _get_or_create_indication_with_code(conn, code, dossier,
                                         name_de, name_fr, name_it,
                                         fusion_cache, counters):
    """Get or create an indication by code. Updates name if not yet set.
    If a name-only indication exists in fusion_cache (no code), upgrade it with the code.
    Returns indication_id.
    """
    # Check if this code already exists
    existing = conn.execute(
        "SELECT indication_id FROM indication WHERE indication_code = ?",
        (code,),
    ).fetchone()
    if existing:
        ind_id = existing[0]
    else:
        # Check if a name-only match exists in fusion_cache that has no code yet
        merged = False
        if dossier and name_fr:
            norm = _normalize_name(name_fr)
            if norm and (dossier, norm) in fusion_cache:
                cached_id = fusion_cache[(dossier, norm)]
                # Only merge if that indication has no code
                row = conn.execute(
                    "SELECT indication_code FROM indication WHERE indication_id = ?",
                    (cached_id,),
                ).fetchone()
                if row and row[0] is None:
                    # Upgrade the existing name-only indication with this code
                    conn.execute(
                        "UPDATE indication SET indication_code = ?, "
                        "bag_dossier_no = COALESCE(bag_dossier_no, ?) "
                        "WHERE indication_id = ?",
                        (code, dossier, cached_id),
                    )
                    ind_id = cached_id
                    merged = True
                    counters["fused"] += 1

        if not merged:
            conn.execute(
                "INSERT OR IGNORE INTO indication (indication_code, bag_dossier_no) "
                "VALUES (?, ?)",
                (code, dossier),
            )
            row = conn.execute(
                "SELECT indication_id FROM indication WHERE indication_code = ?",
                (code,),
            ).fetchone()
            ind_id = row[0]

    # Update name if not yet set
    if name_de or name_fr or name_it:
        conn.execute(
            "UPDATE indication SET "
            "indication_name_de = COALESCE(indication_name_de, ?), "
            "indication_name_fr = COALESCE(indication_name_fr, ?), "
            "indication_name_it = COALESCE(indication_name_it, ?), "
            "name_source = COALESCE(name_source, 'BOLD_HEADER') "
            "WHERE indication_id = ?",
            (name_de, name_fr, name_it, ind_id),
        )

    # Update fusion cache
    if dossier and name_fr:
        norm = _normalize_name(name_fr)
        if norm:
            fusion_cache[(dossier, norm)] = ind_id

    return ind_id


def _get_or_create_indication_by_name(conn, dossier, name_de, name_fr, name_it,
                                       fusion_cache, counters):
    """Get or create an indication by name (no code).
    Uses fusion logic: same (bag_dossier_no, normalized_name_fr) = same indication.
    Returns indication_id.
    """
    # Try fusion: same dossier + same normalized FR name
    if dossier and name_fr:
        norm = _normalize_name(name_fr)
        if norm:
            cache_key = (dossier, norm)
            if cache_key in fusion_cache:
                counters["fused"] += 1
                return fusion_cache[cache_key]

    # No fusion match -> create new indication
    cur = conn.execute(
        "INSERT INTO indication "
        "(bag_dossier_no, indication_name_de, indication_name_fr, "
        " indication_name_it, name_source) "
        "VALUES (?, ?, ?, ?, 'BOLD_HEADER')",
        (dossier, name_de, name_fr, name_it),
    )
    ind_id = cur.lastrowid
    counters["created"] += 1

    # Cache for future fusion
    if dossier and name_fr:
        norm = _normalize_name(name_fr)
        if norm:
            fusion_cache[(dossier, norm)] = ind_id

    return ind_id


def _link_indication_to_text(conn, text_id, indication_id, code_source):
    """Update the default segment (index=0) for this text_id to point to the indication."""
    conn.execute(
        "UPDATE limitation_text_segment "
        "SET indication_id = ?, code_source = ? "
        "WHERE text_id = ? AND segment_index = 0 AND code_source = 'NOCODE'",
        (indication_id, code_source, text_id),
    )

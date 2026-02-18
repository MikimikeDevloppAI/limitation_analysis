"""
Step 03c: Propagate indication codes to NOCODE segments.

For pre-2023 texts that have no bold header and no embedded code (code_source='NOCODE'),
we try to assign an indication_id using four strategies (in order):

Phase A — SAME_LIM_CODE:
    If the same limitation_code appears on another limitation whose segment already
    has an indication with a real code, propagate that indication_id.

Phase B — SINGLE_SKU_CODE:
    If a SKU (GTIN) linked to the NOCODE segment has exactly one distinct
    indication_code across all its other limitations, propagate that indication_id.

Phase C — DOSSIER_XX:
    If the preparation has only ONE limitation active during the segment's period,
    assign indication_code = dossier.XX (whole-product indication).

Phase D — LIM_CODE_NAME:
    For remaining NOCODE segments, try two strategies in order:
    1. Full limitation_code match in LIM_CODE_DICT → trilingual indication (LIM_CODE_FULL)
    2. Suffix analysis via SUFFIX_DICT → trilingual indication (LIM_CODE_SUFFIX)
    Unrecognized suffixes / PRODUCT_ONLY → reclassify only (no named indication)
    VERSIONED → reclassify only

Each propagated segment keeps a distinct code_source so the origin is always traceable.

Depends on: step_01, step_02, step_03, step_03b.
"""

import logging
import re
import sqlite3

log = logging.getLogger(__name__)

# Structural keywords in limitation_codes — not indications
_STRUCTURAL_KEYWORDS = frozenset([
    "EINF", "ENDE", "UNBEFR", "BEFR", "THERAPIEBEG", "THERBEG",
    "KLEINP", "KLEINPACKUNG",
])

# ---------------------------------------------------------------------------
# Suffix dictionary: medically meaningful suffixes → trilingual names
# Suffixes NOT in this dictionary are treated as PRODUCT_ONLY.
# ---------------------------------------------------------------------------
SUFFIX_DICT = {
    # Anemia
    "RENAN":    ("Renale Anämie", "Anémie rénale", "Anemia renale"),
    "SYMPTAN":  ("Symptomatische Anämie", "Anémie symptomatique", "Anemia sintomatica"),
    "SYMP":     ("Symptomatische Anämie", "Anémie symptomatique", "Anemia sintomatica"),
    "ANAE":     ("Anämie", "Anémie", "Anemia"),
    "WAHL":     ("Wahloperation", "Opération élective", "Operazione elettiva"),
    "DIAL":     ("Dialyse", "Dialyse", "Dialisi"),

    # Therapy phases
    ".Ein":     ("Einleitung", "Induction", "Induzione"),
    ".SA.Ein":  ("Einleitung", "Induction", "Induzione"),
    ".SaEin":   ("Einleitung", "Induction", "Induzione"),
    ".SpEin":   ("Einleitung", "Induction", "Induzione"),
    ".ZeEin":   ("Einleitung", "Induction", "Induzione"),
    "ACCein":   ("Einleitung", "Induction", "Induzione"),
    "ZENein":   ("Einleitung", "Induction", "Induzione"),
    ".Erh":     ("Erhaltungstherapie", "Traitement d'entretien", "Terapia di mantenimento"),
    ".Ind":     ("Induktionstherapie", "Traitement d'induction", "Terapia di induzione"),
    ".Prä":     ("Prävention", "Prévention", "Prevenzione"),
    ".End":     ("Endbehandlung", "Traitement final", "Trattamento finale"),
    ".END":     ("Endbehandlung", "Traitement final", "Trattamento finale"),
    "UNBE":     ("Unbefristet", "Durée illimitée", "Durata illimitata"),
    "mono":     ("Monotherapie", "Monothérapie", "Monoterapia"),

    # Biosimilar exchange
    ".R/B":     ("Austausch Referenzpräparat/Biosimilar", "Échange référence/biosimilaire", "Scambio referenza/biosimilare"),
    ".RB":      ("Austausch Referenzpräparat/Biosimilar", "Échange référence/biosimilaire", "Scambio referenza/biosimilare"),
    "XE.Bio":   ("Austausch Referenzpräparat/Biosimilar", "Échange référence/biosimilaire", "Scambio referenza/biosimilare"),

    # Combination (generic — enriched with drug name from text)
    ".Kom":     ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    ".Kombi":   ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    ".Komb2":   ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    ".KoGu":    ("Kostengutsprache Kombination", "Autorisation combinaison", "Autorizzazione combinazione"),
    "IbRi":     ("Kombination Ibrutinib + Rituximab", "Combinaison ibrutinib + rituximab", "Combinazione ibrutinib + rituximab"),

    # Opdivo melanoma combos
    "MELKO5":   ("Melanom - Kombination", "Mélanome - combinaison", "Melanoma - combinazione"),
    "MELKO6":   ("Melanom - Kombination", "Mélanome - combinaison", "Melanoma - combinazione"),
    "MELKO9":   ("Melanom - Kombination", "Mélanome - combinaison", "Melanoma - combinazione"),

    # Infusion
    ".Inf":     ("Infusion", "Perfusion", "Infusione"),

    # Revlimid combinations (extract drug name from text)
    "4KOM":     ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    "5KOM":     ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    "6KOM":     ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    "7KOM":     ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    "6.1K":     ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),

    # Cresemba forms by indication
    ".Kps.IA":  ("Kapseln - Invasive Aspergillose", "Capsules - aspergillose invasive", "Capsule - aspergillosi invasiva"),
    ".Kps.IM":  ("Kapseln - Invasive Mukormykose", "Capsules - mucormycose invasive", "Capsule - mucormicosi invasiva"),
    ".Lsg.IA":  ("Lösung - Invasive Aspergillose", "Solution - aspergillose invasive", "Soluzione - aspergillosi invasiva"),
    ".Lsg.IM":  ("Lösung - Invasive Mukormykose", "Solution - mucormycose invasive", "Soluzione - mucormicosi invasiva"),

    # Pemetrexed generics: combination
    "ACC.Ko":   ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    "MY.Kom":   ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    "SA.Kom":   ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    "SP.Kom":   ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    "TE.Kom":   ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    "VI.Kom":   ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),
    "ZE.Kom":   ("Kombinationstherapie", "Traitement combiné", "Terapia combinata"),

    # Rivaroxaban vascular
    ".MEP.vas": ("Vaskulär", "Vasculaire", "Vascolare"),
    ".San.vas": ("Vaskulär", "Vasculaire", "Vascolare"),
    ".VIA.vas": ("Vaskulär", "Vasculaire", "Vascolare"),
    ".vas.Spi": ("Vaskulär", "Vasculaire", "Vascolare"),

    # Tadalafil/Sildenafil PAH
    ".P.MEP":   ("Pulmonale arterielle Hypertonie", "Hypertension artérielle pulmonaire", "Ipertensione arteriosa polmonare"),
    ".P.OrP":   ("Pulmonale arterielle Hypertonie", "Hypertension artérielle pulmonaire", "Ipertensione arteriosa polmonare"),
    ".P.SPI":   ("Pulmonale arterielle Hypertonie", "Hypertension artérielle pulmonaire", "Ipertensione arteriosa polmonare"),
    ".P.VIA":   ("Pulmonale arterielle Hypertonie", "Hypertension artérielle pulmonaire", "Ipertensione arteriosa polmonare"),
}

# ---------------------------------------------------------------------------
# Full limitation_code → trilingual indication names
# For NOCODE segments where the code doesn't start with the product name.
# ---------------------------------------------------------------------------
LIM_CODE_DICT = {
    # Herpes
    "HERPES SIMPL":  ("Herpes simplex", "Herpès simple", "Erpete semplice"),
    "HERPES ZENTI":  ("Herpes simplex", "Herpès simple", "Erpete semplice"),
    "HERPESSIMIMM":  ("Herpes simplex (Immunsuppression)", "Herpès simple (immunosuppression)", "Erpete semplice (immunosoppressione)"),
    "HERPESSIMIM2":  ("Herpes simplex (Immunsuppression)", "Herpès simple (immunosuppression)", "Erpete semplice (immunosoppressione)"),
    "HERPESZOSTER":  ("Herpes zoster", "Herpès zoster", "Erpete zoster"),

    # Nausea / emetogenic chemotherapy
    "EMETCHEMOTHE":  ("Stark emetogene Chemotherapie", "Chimiothérapie fortement émétogène", "Chimioterapia fortemente emetogena"),
    "NAUSEMETCHEM":  ("Erbrechen bei emetogener Chemotherapie", "Vomissements - chimiothérapie émétisante", "Trattamento antiemetico in chemioterapia"),
    "KYTRIL":        ("Erbrechen bei emetogener Chemotherapie", "Vomissements - chimiothérapie émétisante", "Trattamento antiemetico in chemioterapia"),

    # Infant formula
    "ALFAMINO":      ("Kuhmilchproteinintoleranz (Zweitlinie)", "Intolérance protéines lait de vache (2e ligne)", "Intolleranza proteine latte vaccino (2a linea)"),
    "MILUPAPREGO":   ("Kuhmilchproteinintoleranz (Zweitlinie)", "Intolérance protéines lait de vache (2e ligne)", "Intolleranza proteine latte vaccino (2a linea)"),
    "NEOCATEINFAN":  ("Kuhmilchproteinintoleranz (Zweitlinie)", "Intolérance protéines lait de vache (2e ligne)", "Intolleranza proteine latte vaccino (2a linea)"),
    "ALFARÉ":        ("Kuhmilchproteinintoleranz (6 Monate)", "Intolérance protéines lait de vache (6 mois)", "Intolleranza proteine latte vaccino (6 mesi)"),

    # Hepatic encephalopathy
    "HEPENZE":       ("Hepatische Enzephalopathie", "Encéphalopathie hépatique", "Encefalopatia epatica"),

    # HIV / PrEP
    "EMT.TEN.M_01":  ("HIV-1-Infektion", "Infection par le VIH-1", "Infezione da HIV-1"),
    "EMT.TEN.M_02":  ("Prä-Expositionsprophylaxe (PrEP)", "Prophylaxie pré-exposition (PrEP)", "Profilassi pre-esposizione (PrEP)"),
    "EMT.TEN.V_01":  ("HIV-1-Infektion", "Infection par le VIH-1", "Infezione da HIV-1"),
    "EMT.TEN.V_02":  ("Prä-Expositionsprophylaxe (PrEP)", "Prophylaxie pré-exposition (PrEP)", "Profilassi pre-esposizione (PrEP)"),

    # Vitamin D deficiency
    "DEKRISTOL":     ("Schwerer Vitamin-D-Mangel", "Carence sévère en vitamine D", "Grave carenza di vitamina D"),
    "DIBASE":        ("Schwerer Vitamin-D-Mangel", "Carence sévère en vitamine D", "Grave carenza di vitamina D"),
    "VITD3.1000":    ("Schwerer Vitamin-D-Mangel / Osteomalazie", "Carence sévère en vitamine D / ostéomalacie", "Grave carenza di vitamina D / osteomalacia"),
    "VITD3SPIRIGK":  ("Schwerer Vitamin-D-Mangel / Rachitis / Osteomalazie", "Carence sévère en vitamine D / rachitisme / ostéomalacie", "Grave carenza di vitamina D / rachitismo / osteomalacia"),
    "VITD3SPIRIGL":  ("Schwerer Vitamin-D-Mangel / Rachitis / Osteomalazie / Prophylaxe", "Carence sévère en vitamine D / rachitisme / ostéomalacie / prophylaxie", "Grave carenza di vitamina D / rachitismo / osteomalacia / profilassi"),
    "VITD3STREULK":  ("Schwerer Vitamin-D-Mangel", "Carence sévère en vitamine D", "Grave carenza di vitamina D"),

    # Immunoglobulins
    "INTRATECT2":    ("Antikörpermangel / ITP / Guillain-Barré / Kawasaki / CIDP / MMN",
                      "Déficit immunitaire / PTI / Guillain-Barré / Kawasaki / CIDP / NMM",
                      "Immunodeficienza / PTI / Guillain-Barré / Kawasaki / CIDP / NMM"),

    # Diabetes
    "RYBELSUS":      ("Diabetes mellitus Typ 2 (Kombination)", "Diabète de type 2 (combinaison)", "Diabete mellito di tipo 2 (combinazione)"),
    "RYBELSUS2":     ("Diabetes mellitus Typ 2", "Diabète de type 2", "Diabete mellito di tipo 2"),

    # Pain / opioid substitution
    "L-POLAMIDON1":  ("Prolongierte Schmerzen", "Douleurs prolongées", "Dolori persistenti"),
    "L-POLAMIDON2":  ("Opioidsubstitution (QTc-Risiko)", "Substitution opioïdes (risque QTc)", "Sostituzione oppioidi (rischio QTc)"),

    # Breast cancer
    "AFINITORBEFR":  ("HER2-negativer Brustkrebs (mit Exemestan)", "Cancer du sein HER2-négatif (avec exémestane)", "Carcinoma mammario HER2-negativo (con exemestane)"),

    # Pneumococcal vaccination
    "PREVENAR13_5":  ("Pneumokokken-Impfung ab 65", "Vaccination pneumococcique dès 65 ans", "Vaccinazione pneumococcica dai 65 anni"),
    "PREVENAR20":    ("Pneumokokken-Impfung ab 65", "Vaccination pneumococcique dès 65 ans", "Vaccinazione pneumococcica dai 65 anni"),
}

# Regex to extract combination drug names from limitation text
RE_KOMBI_DE = re.compile(
    r'[Ii]n Kombination(?:stherapie)?\s+(?:mit\s+)?'
    r'([A-ZÄÖÜ][a-zäöüéèà\-]+(?:\s+und\s+[A-ZÄÖÜ][a-zäöüéèà\-]+)*)'
)
RE_KOMBI_FR = re.compile(
    r'en (?:combinaison|association) avec\s+'
    r"(?:l[ea']?\s*|du\s+|de\s+la\s+|des?\s+)?"
    r'([A-ZÉÈÀa-zéèàôûî][A-ZÉÈÀa-zéèàôûî\-]{2,})'
    r'(?:\s+et\s+(?:l[ea\']?\s*|du\s+|de\s+la\s+|des?\s+)?'
    r'([A-ZÉÈÀa-zéèàôûî][A-ZÉÈÀa-zéèàôûî\-]{2,}))?',
    re.IGNORECASE,
)


def _normalize_name(name):
    """Uppercase, strip spaces/hyphens/accents for prefix matching."""
    return (
        name.upper()
        .replace(" ", "")
        .replace("-", "")
        .replace("É", "E")
        .replace("È", "E")
        .replace("À", "A")
        .replace("Ô", "O")
    )


def extract_lim_code_suffix(lim_code, product_name):
    """Extract indication-meaningful suffix from a limitation_code.

    Returns (suffix, category) where category is one of:
    - 'SUFFIX'       : descriptive abbreviation after product name
    - 'PRODUCT_ONLY' : code matches product name exactly (no suffix)
    - 'VERSIONED'    : only a version number suffix (1, 2, .01, etc.)
    - 'STRUCTURAL'   : EINF/ENDE/THERAPIEBEG etc.
    - 'GRANDFRERE'   : cross-product limitation
    - 'NOMATCH'      : cannot parse
    """
    code_upper = lim_code.upper().replace(" ", "")

    # GRANDFRERE
    if "GRANDFRERE" in code_upper or "GRANDFR" in code_upper:
        return ("", "GRANDFRERE")

    # Structural
    for kw in _STRUCTURAL_KEYWORDS:
        if kw in code_upper:
            return ("", "STRUCTURAL")

    if not product_name:
        return (lim_code, "NOMATCH")

    # Try to match product name as prefix of the limitation_code
    norm_product = _normalize_name(product_name)

    # Try progressively shorter prefixes (at least 4 chars)
    best_len = 0
    for length in range(len(norm_product), 3, -1):
        if code_upper.startswith(norm_product[:length]):
            best_len = length
            break

    if best_len < 4:
        return (lim_code, "NOMATCH")

    suffix = lim_code[best_len:]

    # No suffix → product name only
    if not suffix.strip():
        return ("", "PRODUCT_ONLY")

    # Dot-number or pure number suffix → versioned
    if re.match(r'^\.?\d{1,2}[a-z]?$', suffix, re.IGNORECASE):
        return (suffix, "VERSIONED")

    # Strip leading version digits to get the descriptive part
    # e.g. REVLIMID5KOM → "5KOM", KEYTR1LNSCLC → "1LNSCLC"
    # These contain meaningful abbreviations even if prefixed with a digit
    return (suffix, "SUFFIX")


def _find_or_create_indication(conn, bag_dossier_no, name_de):
    """Find or create an indication by dossier + name_de."""
    row = conn.execute(
        "SELECT indication_id FROM indication "
        "WHERE bag_dossier_no = ? AND indication_name_de = ?",
        (bag_dossier_no, name_de),
    ).fetchone()
    if row:
        return row[0]
    conn.execute(
        "INSERT INTO indication (bag_dossier_no, indication_name_de, name_source) "
        "VALUES (?, ?, 'LIM_CODE')",
        (bag_dossier_no, name_de),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _find_or_create_indication_trilingual(conn, bag_dossier_no, name_de, name_fr, name_it):
    """Find or create an indication with trilingual names."""
    row = conn.execute(
        "SELECT indication_id FROM indication "
        "WHERE bag_dossier_no = ? AND indication_name_de = ?",
        (bag_dossier_no, name_de),
    ).fetchone()
    if row:
        if name_fr:
            conn.execute(
                "UPDATE indication SET indication_name_fr = ? "
                "WHERE indication_id = ? AND indication_name_fr IS NULL",
                (name_fr, row[0]),
            )
        if name_it:
            conn.execute(
                "UPDATE indication SET indication_name_it = ? "
                "WHERE indication_id = ? AND indication_name_it IS NULL",
                (name_it, row[0]),
            )
        return row[0]
    conn.execute(
        "INSERT INTO indication "
        "(bag_dossier_no, indication_name_de, indication_name_fr, indication_name_it, name_source) "
        "VALUES (?, ?, ?, ?, 'LIM_CODE')",
        (bag_dossier_no, name_de, name_fr, name_it),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _extract_combination_drugs(desc_de, desc_fr):
    """Extract drug names from combination therapy texts."""
    drugs_de = None
    drugs_fr = None
    if desc_de:
        m = RE_KOMBI_DE.search(desc_de)
        if m:
            drugs_de = m.group(1)
    if desc_fr:
        m = RE_KOMBI_FR.search(desc_fr)
        if m:
            drug1 = m.group(1)
            drug2 = m.group(2)
            if drug2:
                drugs_fr = f"{drug1} et {drug2}"
            else:
                drugs_fr = drug1
    return drugs_de, drugs_fr


def run(db_path):
    """Run step 03c: propagate indication codes to NOCODE segments."""
    log.info("Step 03c: Propagate indication codes to NOCODE segments")

    conn = sqlite3.connect(str(db_path))

    counters = {
        "nocode_before": 0,
        "same_lim_code": 0,
        "single_sku_code": 0,
        "lim_code_full": 0,
        "lim_code_suffix": 0,
        "lim_code_product": 0,
        "lim_code_versioned": 0,
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
    # Phase C: Assign dossier.XX to single-limitation preparations
    # ------------------------------------------------------------------
    # For NOCODE segments, check if the preparation has only ONE limitation
    # active during the segment's period. If so, the indication covers
    # the entire product → assign code = dossier.XX.
    # This runs BEFORE limitation_code analysis so that segments get a
    # real indication code when possible.
    # ------------------------------------------------------------------
    log.info("  Phase C: assign dossier.XX to single-limitation segments...")

    counters["dossier_xx"] = 0

    candidates = conn.execute("""
        SELECT seg.segment_id, seg.text_id, seg.indication_id,
               s.bag_dossier_no, s.product_name,
               l.first_seen_extract, l.last_seen_extract
        FROM limitation_text_segment seg
        JOIN limitation l ON l.text_id = seg.text_id
        JOIN sku_limitation sl ON sl.limitation_id = l.limitation_id
        JOIN sku s ON s.gtin = sl.gtin
        WHERE seg.code_source = 'NOCODE'
        GROUP BY seg.segment_id
    """).fetchall()

    for seg_id, text_id, ind_id, dossier, product, first, last in candidates:
        # Count other active limitations for this product in the same period
        other = conn.execute("""
            SELECT COUNT(DISTINCT l2.limitation_id)
            FROM sku_limitation sl
            JOIN limitation l2 ON l2.limitation_id = sl.limitation_id
            JOIN sku s ON s.gtin = sl.gtin
            WHERE s.product_name = ?
              AND l2.text_id != ?
              AND l2.first_seen_extract <= ?
              AND l2.last_seen_extract >= ?
        """, (product, text_id, last, first)).fetchone()[0]

        if other == 0:
            code = f"{dossier}.XX"
            row = conn.execute(
                "SELECT indication_id FROM indication "
                "WHERE indication_code = ?", (code,),
            ).fetchone()
            if row:
                new_ind_id = row[0]
            else:
                conn.execute(
                    "INSERT INTO indication "
                    "(indication_code, bag_dossier_no, name_source) "
                    "VALUES (?, ?, 'DOSSIER_XX')",
                    (code, dossier),
                )
                new_ind_id = conn.execute(
                    "SELECT last_insert_rowid()"
                ).fetchone()[0]
            conn.execute(
                "UPDATE limitation_text_segment "
                "SET indication_id = ?, code_source = 'DOSSIER_XX' "
                "WHERE segment_id = ? AND code_source = 'NOCODE'",
                (new_ind_id, seg_id),
            )
            counters["dossier_xx"] += 1

    conn.commit()
    log.info(f"    dossier.XX assigned: {counters['dossier_xx']}")

    # ------------------------------------------------------------------
    # Phase D: Extract indication info from limitation_code names
    # ------------------------------------------------------------------
    # For remaining NOCODE segments, try two strategies in order:
    # 1. Full limitation_code match in LIM_CODE_DICT → LIM_CODE_FULL
    # 2. Suffix analysis via SUFFIX_DICT → LIM_CODE_SUFFIX
    # Unrecognized suffixes / PRODUCT_ONLY → reclassify only.
    # ------------------------------------------------------------------
    log.info("  Phase D: limitation_code name analysis...")

    nocode_data = conn.execute("""
        SELECT seg.segment_id, seg.text_id, l.limitation_code,
               s.product_name, s.bag_dossier_no
        FROM limitation_text_segment seg
        JOIN limitation l ON l.text_id = seg.text_id
        JOIN sku_limitation sl ON sl.limitation_id = l.limitation_id
        JOIN sku s ON s.gtin = sl.gtin
        WHERE seg.code_source = 'NOCODE'
        GROUP BY seg.segment_id
    """).fetchall()

    combo_texts = {}  # text_id → (desc_de, desc_fr)

    for segment_id, _text_id, lim_code, product_name, dossier in nocode_data:
        # 1) Try full limitation_code match first
        if lim_code in LIM_CODE_DICT:
            name_de, name_fr, name_it = LIM_CODE_DICT[lim_code]
            ind_id = _find_or_create_indication_trilingual(
                conn, dossier, name_de, name_fr, name_it,
            )
            conn.execute(
                "UPDATE limitation_text_segment "
                "SET indication_id = ?, code_source = 'LIM_CODE_FULL' "
                "WHERE segment_id = ? AND code_source = 'NOCODE'",
                (ind_id, segment_id),
            )
            counters["lim_code_full"] += 1
            continue

        # 2) Try suffix analysis
        suffix, category = extract_lim_code_suffix(lim_code, product_name)

        if category == "SUFFIX":
            clean_suffix = suffix.strip()
            if clean_suffix in SUFFIX_DICT:
                name_de, name_fr, name_it = SUFFIX_DICT[clean_suffix]

                # For combination suffixes: enrich with drug names from text
                if "Kombination" in (name_de or ""):
                    if _text_id not in combo_texts:
                        row = conn.execute(
                            "SELECT description_de, description_fr "
                            "FROM limitation_text WHERE text_id = ?",
                            (_text_id,),
                        ).fetchone()
                        combo_texts[_text_id] = (row[0], row[1]) if row else (None, None)
                    desc_de, desc_fr = combo_texts[_text_id]
                    drugs_de, drugs_fr = _extract_combination_drugs(desc_de, desc_fr)
                    if drugs_de:
                        name_de = f"Kombinationstherapie - {drugs_de}"
                    if drugs_fr:
                        name_fr = f"Traitement combiné - {drugs_fr}"

                ind_id = _find_or_create_indication_trilingual(
                    conn, dossier, name_de, name_fr, name_it,
                )
                conn.execute(
                    "UPDATE limitation_text_segment "
                    "SET indication_id = ?, code_source = 'LIM_CODE_SUFFIX' "
                    "WHERE segment_id = ? AND code_source = 'NOCODE'",
                    (ind_id, segment_id),
                )
                counters["lim_code_suffix"] += 1
            else:
                # Suffix not in dictionary → just reclassify, keep placeholder
                conn.execute(
                    "UPDATE limitation_text_segment "
                    "SET code_source = 'LIM_CODE_PRODUCT' "
                    "WHERE segment_id = ? AND code_source = 'NOCODE'",
                    (segment_id,),
                )
                counters["lim_code_product"] += 1

        elif category == "PRODUCT_ONLY":
            conn.execute(
                "UPDATE limitation_text_segment "
                "SET code_source = 'LIM_CODE_PRODUCT' "
                "WHERE segment_id = ? AND code_source = 'NOCODE'",
                (segment_id,),
            )
            counters["lim_code_product"] += 1

        elif category == "VERSIONED":
            conn.execute(
                "UPDATE limitation_text_segment "
                "SET code_source = 'LIM_CODE_VERSIONED' "
                "WHERE segment_id = ? AND code_source = 'NOCODE'",
                (segment_id,),
            )
            counters["lim_code_versioned"] += 1

        # STRUCTURAL, GRANDFRERE, NOMATCH → leave as NOCODE

    conn.commit()

    log.info(f"    LIM_CODE_FULL:      {counters['lim_code_full']}")
    log.info(f"    LIM_CODE_SUFFIX:    {counters['lim_code_suffix']}")
    log.info(f"    LIM_CODE_PRODUCT:   {counters['lim_code_product']}")
    log.info(f"    LIM_CODE_VERSIONED: {counters['lim_code_versioned']}")

    # ------------------------------------------------------------------
    # Final count
    # ------------------------------------------------------------------
    counters["nocode_after"] = conn.execute(
        "SELECT COUNT(*) FROM limitation_text_segment WHERE code_source = 'NOCODE'"
    ).fetchone()[0]
    no_ind_code = conn.execute(
        "SELECT COUNT(*) FROM limitation_text_segment seg "
        "LEFT JOIN indication ind ON ind.indication_id = seg.indication_id "
        "WHERE ind.indication_code IS NULL"
    ).fetchone()[0]

    log.info("  Results:")
    log.info(f"    NOCODE before:      {counters['nocode_before']}")
    log.info(f"    A SAME_LIM_CODE:    {counters['same_lim_code']}")
    log.info(f"    B SINGLE_SKU_CODE:  {counters['single_sku_code']}")
    log.info(f"    C DOSSIER_XX:       {counters['dossier_xx']}")
    log.info(f"    D LIM_CODE_FULL:    {counters['lim_code_full']}")
    log.info(f"    D LIM_CODE_SUFFIX:  {counters['lim_code_suffix']}")
    log.info(f"    D LIM_CODE_PRODUCT: {counters['lim_code_product']}")
    log.info(f"    D LIM_CODE_VERSION: {counters['lim_code_versioned']}")
    log.info(f"    NOCODE after:       {counters['nocode_after']}")
    log.info(f"    Reduction:          {counters['nocode_before'] - counters['nocode_after']}")
    log.info(f"    Segments w/o code:  {no_ind_code}")

    conn.close()
    log.info("  Step 03c done.")

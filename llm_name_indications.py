"""
Standalone script: Name unnamed indication codes using LLM (Anthropic Claude).

For each bag_dossier with unnamed non-XX indication codes:
1. Gathers the latest limitation text (DE) + bold headers + limitation_codes
2. Sends a batch prompt to Claude with named codes as style examples
3. Parses the JSON response and updates the DB + saves to JSON file

Usage:
    python llm_name_indications.py
"""

import json
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path

import anthropic

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).parent / "sku_indication.db"
OUTPUT_JSON = Path(__file__).parent / "llm_indication_names.json"
MODEL = "claude-sonnet-4-5-20250929"
MAX_TEXT_CHARS = 3000  # truncate long texts
PAUSE_BETWEEN_CALLS = 0.5  # seconds

RE_BOLD = re.compile(r"<b>(.+?)</b>")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)
sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_unnamed_codes(conn):
    """Load unnamed non-XX indication codes grouped by dossier."""
    rows = conn.execute("""
        SELECT i.indication_id, i.indication_code, i.bag_dossier_no,
               lt.description_de,
               GROUP_CONCAT(DISTINCT l.limitation_code) as lim_codes
        FROM indication i
        JOIN limitation_text_segment seg ON seg.indication_id = i.indication_id
        JOIN limitation_text lt ON lt.text_id = seg.text_id
        JOIN limitation l ON l.text_id = seg.text_id
        WHERE i.indication_code IS NOT NULL
          AND i.indication_name_de IS NULL
        GROUP BY i.indication_id
    """).fetchall()

    by_dossier = {}
    for ind_id, ind_code, dossier, desc_de, lim_codes in rows:
        if dossier not in by_dossier:
            by_dossier[dossier] = []
        bolds = RE_BOLD.findall(desc_de) if desc_de else []
        by_dossier[dossier].append({
            "indication_id": ind_id,
            "indication_code": ind_code,
            "limitation_codes": lim_codes or "",
            "bolds": bolds,
            "text_de": (desc_de or "")[:MAX_TEXT_CHARS],
        })
    return by_dossier


def load_named_codes(conn, dossier):
    """Load named indication codes for a dossier (context for LLM)."""
    rows = conn.execute("""
        SELECT i.indication_code,
               COALESCE(i.indication_name_de, '') as name_de,
               COALESCE(i.indication_name_fr, '') as name_fr,
               COALESCE(i.indication_name_it, '') as name_it,
               GROUP_CONCAT(DISTINCT l.limitation_code) as lim_codes,
               lt.description_de
        FROM indication i
        LEFT JOIN limitation_text_segment seg ON seg.indication_id = i.indication_id
        LEFT JOIN limitation_text lt ON lt.text_id = seg.text_id
        LEFT JOIN limitation l ON l.text_id = seg.text_id
        WHERE i.bag_dossier_no = ?
          AND i.indication_name_de IS NOT NULL
          AND i.indication_code IS NOT NULL
        GROUP BY i.indication_id
        ORDER BY i.indication_code
    """, (dossier,)).fetchall()

    named = []
    for ind_code, name_de, name_fr, name_it, lim_codes, desc_de in rows:
        bolds = RE_BOLD.findall(desc_de) if desc_de else []
        named.append({
            "indication_code": ind_code,
            "name_de": name_de,
            "name_fr": name_fr,
            "name_it": name_it,
            "limitation_codes": lim_codes or "",
            "bolds": bolds,
        })
    return named


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """\
Tu es un expert en pharmacologie suisse (BAG/OFSP).
Tu nommes les indications médicales à partir des textes de limitation.

RÈGLES DE NOMMAGE :
- Nom COURT et PRÉCIS (max ~80 caractères)
- Utilise la terminologie médicale standard
- Si le texte contient des titres en gras (<b>), c'est souvent le nom de l'indication
- Si plusieurs codes partagent le même texte, distingue-les par le contexte \
(ligne de thérapie, combinaison, forme galénique, population, etc.)
- Le limitation_code peut donner des indices \
(ex: .Kom = combinaison, RENAN = renale Anämie, .Ein = Einleitung)
- Si tu ne peux absolument pas distinguer deux codes, retourne "na"
- Les noms doivent être cohérents en DE, FR et IT (même sens médical)

EXEMPLES DE NOMMAGE (tirés de la base) :
- "Kolorektalkarzinom" / "Carcinome colorectal" / "Carcinoma del colon-retto"
- "1L NSCLC (in Kombination mit Lazertinib)" / "1L CBNPC (en combinaison avec lazertinib)" / "1L NSCLC (in combinazione con lazertinib)"
- "Schwere Plaque-Psoriasis" / "Psoriasis en plaques sévère" / "Psoriasi a placche grave"
- "Kombination DARZALEX SC, Pomalidomid und Dexamethason" / "DARZALEX SC en association avec le pomalidomide et la dexaméthasone" / "DARZALEX SC in associazione con pomalidomide e desametasone"
- "Adjuvante Behandlung" / "Traitement adjuvant" / "Trattamento adiuvante"
- "Chronische Rhinosinusitis mit Nasenpolypen (CRSwNP)" / "Rhinosinusite chronique avec polypes nasaux (RSCaPN)" / "Rinosinusite cronica con polipi nasali (CRSwNP)"
- "Schweres eosinophiles Asthma" / "Asthme sévère à éosinophiles" / "Asma eosinofilo severo"
- "Renale Anämie" / "Anémie rénale" / "Anemia renale"
- "Hepatische Enzephalopathie" / "Encéphalopathie hépatique" / "Encefalopatia epatica"
"""


def build_prompt(dossier, named_codes, unnamed_codes):
    """Build the LLM prompt for one dossier."""
    parts = [f"DOSSIER {dossier}\n"]

    # Named codes as context
    if named_codes:
        parts.append("=== Indications déjà nommées (référence de style) ===\n")
        for nc in named_codes:
            parts.append(
                f"indication_code={nc['indication_code']}  "
                f"limitation_codes={nc['limitation_codes']}\n"
                f"  name_de: {nc['name_de']}\n"
                f"  name_fr: {nc['name_fr']}\n"
                f"  name_it: {nc['name_it']}\n"
            )
            if nc["bolds"]:
                parts.append(f"  bolds: {nc['bolds']}\n")
            parts.append("\n")
    else:
        parts.append("(Aucune indication nommée sur ce dossier)\n\n")

    # Unnamed codes to name
    parts.append("=== Indications à nommer ===\n")
    for uc in unnamed_codes:
        parts.append(
            f"indication_code={uc['indication_code']}  "
            f"limitation_codes={uc['limitation_codes']}\n"
        )
        if uc["bolds"]:
            parts.append(f"  bolds: {uc['bolds']}\n")
        parts.append(f"  text_de: {uc['text_de']}\n\n")

    # Response format
    codes_list = [uc["indication_code"] for uc in unnamed_codes]
    example = {c: {"de": "...", "fr": "...", "it": "..."} for c in codes_list[:2]}
    parts.append(
        "Réponds UNIQUEMENT en JSON valide (pas de texte avant/après) :\n"
        f"{json.dumps(example, ensure_ascii=False, indent=2)}\n"
    )

    return "".join(parts)


# ---------------------------------------------------------------------------
# LLM call + JSON parsing
# ---------------------------------------------------------------------------
def call_llm(client, prompt):
    """Call Anthropic API and return parsed JSON."""
    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()

    # Try to parse directly
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try extracting from ```json``` block
    m = re.search(r"```json\s*\n(.*?)\n```", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # Try extracting any { ... } block
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass

    log.warning(f"  Could not parse LLM response: {text[:200]}...")
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Name indication codes via LLM")
    parser.add_argument("--dossier", type=str, help="Process only this dossier (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="Print prompt without calling LLM")
    args = parser.parse_args()

    log.info("=== LLM Indication Naming ===")
    log.info(f"DB: {DB_PATH}")
    log.info(f"Model: {MODEL}")

    conn = sqlite3.connect(str(DB_PATH))

    # Load unnamed codes grouped by dossier
    unnamed_by_dossier = load_unnamed_codes(conn)

    if args.dossier:
        if args.dossier not in unnamed_by_dossier:
            log.error(f"Dossier {args.dossier} not found in unnamed codes")
            return
        unnamed_by_dossier = {args.dossier: unnamed_by_dossier[args.dossier]}

    total_codes = sum(len(v) for v in unnamed_by_dossier.values())
    log.info(f"Unnamed codes: {total_codes} across {len(unnamed_by_dossier)} dossiers")

    # Load existing results (for resume support)
    all_results = {}
    if OUTPUT_JSON.exists():
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            all_results = json.load(f)
        log.info(f"Loaded {len(all_results)} existing results from {OUTPUT_JSON}")

    # Init API client
    client = anthropic.Anthropic()

    stats = {"named": 0, "na": 0, "error": 0, "skipped": 0}

    for i, (dossier, unnamed_codes) in enumerate(sorted(unnamed_by_dossier.items()), 1):
        # Skip if all codes for this dossier already done
        codes_to_do = [
            uc for uc in unnamed_codes
            if uc["indication_code"] not in all_results
        ]
        if not codes_to_do:
            stats["skipped"] += len(unnamed_codes)
            continue

        log.info(f"  [{i}/{len(unnamed_by_dossier)}] Dossier {dossier}: "
                 f"{len(codes_to_do)} codes to name...")

        # Load named codes for context
        named_codes = load_named_codes(conn, dossier)

        # Build prompt
        prompt = build_prompt(dossier, named_codes, codes_to_do)

        if args.dry_run:
            print("=" * 80)
            print(f"PROMPT for dossier {dossier}:")
            print("=" * 80)
            print(prompt)
            continue

        # Call LLM
        try:
            result = call_llm(client, prompt)
        except Exception as e:
            log.warning(f"  API error for dossier {dossier}: {e}")
            stats["error"] += len(codes_to_do)
            time.sleep(PAUSE_BETWEEN_CALLS)
            continue

        if result is None:
            stats["error"] += len(codes_to_do)
            time.sleep(PAUSE_BETWEEN_CALLS)
            continue

        # Process results
        for uc in codes_to_do:
            code = uc["indication_code"]
            if code not in result:
                log.warning(f"    {code}: missing from LLM response")
                stats["error"] += 1
                continue

            names = result[code]
            name_de = names.get("de", "na")
            name_fr = names.get("fr", "na")
            name_it = names.get("it", "na")

            # Save to results dict
            all_results[code] = {
                "de": name_de,
                "fr": name_fr,
                "it": name_it,
                "dossier": dossier,
            }

            if name_de == "na" or name_de == "...":
                stats["na"] += 1
                log.info(f"    {code}: na")
            else:
                stats["named"] += 1
                log.info(f"    {code}: {name_de}")

                # Update DB
                conn.execute(
                    "UPDATE indication "
                    "SET indication_name_de = ?, indication_name_fr = ?, "
                    "indication_name_it = ?, name_source = 'LLM' "
                    "WHERE indication_id = ?",
                    (name_de, name_fr, name_it, uc["indication_id"]),
                )

        conn.commit()

        # Save JSON incrementally
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)

        time.sleep(PAUSE_BETWEEN_CALLS)

    # Final stats
    log.info("")
    log.info("=== Results ===")
    log.info(f"  Named by LLM:  {stats['named']}")
    log.info(f"  Returned 'na': {stats['na']}")
    log.info(f"  Errors:        {stats['error']}")
    log.info(f"  Skipped:       {stats['skipped']}")
    log.info(f"  JSON saved:    {OUTPUT_JSON}")

    # Verify DB
    count = conn.execute(
        "SELECT COUNT(*) FROM indication WHERE name_source = 'LLM'"
    ).fetchone()[0]
    log.info(f"  DB indications with name_source='LLM': {count}")

    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    main()

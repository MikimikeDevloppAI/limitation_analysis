"""
LLM-based segmentation of pre-2023 limitation texts with cashback.

Processes 177 cashback texts that have no regex-based segments,
using Claude to detect multi-indication structure and cashback details per segment.

Uses async parallelism (100 concurrent requests) for fast throughput.
"""

import argparse
import asyncio
import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import os

# Load API key from .env file if not already in environment
_env_file = Path(__file__).parent / ".env"
if _env_file.exists() and "ANTHROPIC_API_KEY" not in os.environ:
    for line in _env_file.read_text().splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

import anthropic

from cashback_extractor import clean_html

# ============================================================
# Configuration
# ============================================================

BASE_DIR = Path(r"c:\Users\micha\OneDrive\Matching_indication_code")
DB_PATH = BASE_DIR / "sku_indication.db"

MODEL_HAIKU = "claude-haiku-4-5-20251001"
MODEL_SONNET = "claude-sonnet-4-5-20250929"
LONG_TEXT_THRESHOLD = 2000  # chars — use Sonnet above this

MAX_CONCURRENCY = 100
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ============================================================
# LLM System Prompt
# ============================================================

SYSTEM_PROMPT = r"""Tu es un spécialiste des textes de limitation BAG suisses (Office fédéral de la santé publique).

## Ta tâche
Analyse un texte de limitation pharmaceutique en français et produis un JSON structuré.

### 1. SEGMENTATION : le texte couvre-t-il UNE ou PLUSIEURS indications médicales distinctes ?

**SPLITTER en plusieurs segments si** :
- Le texte liste des maladies/pathologies DISTINCTES (ex: polyarthrite rhumatoïde ≠ maladie de Crohn ≠ psoriasis)
- Chaque indication a ses propres critères de traitement

**NE PAS splitter si** :
- Différents génotypes/sous-types de la MÊME maladie (ex: hépatite C génotype 1 vs 3 = UNE indication)
- Différentes phases de traitement pour la MÊME pathologie (initiation vs poursuite vs arrêt)
- Différents groupes de patients pour la MÊME pathologie (adultes vs enfants)
- Différentes lignes de traitement pour le MÊME cancer

### 2. DÉTECTION CASHBACK : DISTINCTION CRITIQUE

Il y a DEUX types de "remboursement" dans ces textes. Tu DOIS les distinguer :

**❌ PAS du cashback** = l'assurance-maladie rembourse le patient (flux standard) :
- "Prise en charge par l'assurance maladie après consultation du médecin conseil"
- "Garantie de prise en charge des coûts par l'assurance maladie"
- "Accord préalable sur la prise en charge des frais par l'assureur-maladie"
→ C'est le flux normal assurance → patient. Ce N'EST PAS du cashback.

**✅ OUI cashback** = l'entreprise pharmaceutique rembourse l'assureur (flux inverse) :
- "[Société SA/AG] rembourse [montant/pourcentage]..."
- "Le titulaire de l'autorisation... rembourse..."
- "Sur demande de l'assureur-maladie... l'entreprise rembourse..."
- Mentions de "prix de fabrique", "prix de sortie d'usine", "prix ex-factory"
- Montants spécifiques (CHF, Fr., %) payés PAR l'entreprise À l'assureur
- "La taxe sur la valeur ajoutée ne peut pas être exigée en sus"
→ C'est l'entreprise pharma qui rembourse à l'assurance. C'EST du cashback.

**Attention** : Le cashback s'applique souvent à TOUTES les indications du texte (paragraphe en fin de texte). Dans ce cas, marque is_cashback=true sur CHAQUE segment.

### 3. FORMAT DE SORTIE

Retourne UNIQUEMENT du JSON valide (pas de markdown, pas d'explication) :
```
{
  "is_multi_indication": true/false,
  "comment": "Explication brève de ton analyse",
  "segments": [
    {
      "order": 0,
      "indication_name_fr": "Nom court de l'indication en français",
      "is_cashback": true/false,
      "cashback_company": "Nom Société SA" ou null,
      "cashback_calc_type": "percentage|chf_fixed|chf_per_mg|chf_per_box|full_refund|undisclosed_fixed|unknown" ou null,
      "cashback_calc_value": 26.95 ou null,
      "cashback_unit": "per_box|per_mg|per_cycle|per_month|per_patient|per_year|per_treatment" ou null
    }
  ]
}
```

**Règles** :
- `indication_name_fr` = nom COURT de la maladie/condition (ex: "Polyarthrite rhumatoïde active"), PAS le texte complet
- Pour un texte mono-indication, retourne exactement 1 segment
- `cashback_calc_value` = valeur numérique (26.95 pour 26.95%, 3581.40 pour CHF 3'581.40)
- Si le cashback mentionne "un montant fixe" sans préciser la valeur → calc_type="undisclosed_fixed"

## EXEMPLES

### Exemple 1 : Texte court, mono-indication, sans cashback
INPUT: "Herpès zoster."
OUTPUT:
{"is_multi_indication": false, "comment": "Indication unique pour le traitement du zona", "segments": [{"order": 0, "indication_name_fr": "Herpès zoster", "is_cashback": false, "cashback_company": null, "cashback_calc_type": null, "cashback_calc_value": null, "cashback_unit": null}]}

### Exemple 2 : Texte multi-indication sans balises bold
INPUT: "Polyarthrite rhumatoïde active, l'arthrite juvénile idiopathique polyarticulaire, arthrite psoriasique : traitement par HUMIRA en cas de réponse inadéquate aux traitements de fond classique. Prise en charge par l'assurance maladie après consultation préalable du médecin conseil.<br>\nSpondylarthrite ankylosante (maladie de Bechterew) : traitement par HUMIRA lorsque le traitement de fond classique a été insuffisant ou n'a pas été toléré. Prise en charge par l'assurance maladie après consultation préalable du médecin conseil.<br>\nMaladie de Crohn active : traitement des patients adultes et pédiatriques par HUMIRA lorsque le traitement de fond classique a été insuffisant. Prise en charge par l'assurance maladie.<br>\nColite ulcéreuse modérée à grave chez les patients adultes : traitement par HUMIRA. Prise en charge par l'assurance maladie.<br>\nPsoriasis en plaques grave : traitement des patients adultes. Prise en charge par l'assurance maladie.<br>\nHidradénite suppurée : traitement des patients adultes. Prise en charge par l'assurance maladie.<br>"
OUTPUT:
{"is_multi_indication": true, "comment": "6 indications distinctes pour adalimumab, chacune avec ses propres critères", "segments": [{"order": 0, "indication_name_fr": "Polyarthrite rhumatoïde, arthrite juvénile, arthrite psoriasique", "is_cashback": false, "cashback_company": null, "cashback_calc_type": null, "cashback_calc_value": null, "cashback_unit": null}, {"order": 1, "indication_name_fr": "Spondylarthrite ankylosante", "is_cashback": false, "cashback_company": null, "cashback_calc_type": null, "cashback_calc_value": null, "cashback_unit": null}, {"order": 2, "indication_name_fr": "Maladie de Crohn active", "is_cashback": false, "cashback_company": null, "cashback_calc_type": null, "cashback_calc_value": null, "cashback_unit": null}, {"order": 3, "indication_name_fr": "Colite ulcéreuse", "is_cashback": false, "cashback_company": null, "cashback_calc_type": null, "cashback_calc_value": null, "cashback_unit": null}, {"order": 4, "indication_name_fr": "Psoriasis en plaques", "is_cashback": false, "cashback_company": null, "cashback_calc_type": null, "cashback_calc_value": null, "cashback_unit": null}, {"order": 5, "indication_name_fr": "Hidradénite suppurée", "is_cashback": false, "cashback_company": null, "cashback_calc_type": null, "cashback_calc_value": null, "cashback_unit": null}]}

### Exemple 3 : Mono-indication avec cashback pharma
INPUT: "En association avec Zelboraf pour le traitement de patients atteints d'un mélanome non résécable ou métastatique porteurs d'une mutation BRAF V600. Uniquement jusqu'à la progression de la maladie.<br>\nLe traitement exige une garantie préalable de prise en charge des frais par l'assureur-maladie après consultation préalable du médecin-conseil.<br>\nSur demande de l'assureur-maladie auprès duquel la personne était assurée au moment de l'achat, l'entreprise Roche Pharma (Suisse) SA rembourse pour l'association Zelboraf et Cotellic la somme de CHF 3'581.40 à l'assureur-maladie pour chaque boîte de Cotellic achetée (=indicateur d'un cycle de traitement). La taxe sur la valeur ajoutée ne peut pas être exigée en sus de ce montant. La demande de remboursement doit intervenir en règle générale dans les 3 mois qui suivent l'administration.<br>"
OUTPUT:
{"is_multi_indication": false, "comment": "Indication unique (mélanome BRAF V600) avec cashback Roche de CHF 3581.40 par boîte", "segments": [{"order": 0, "indication_name_fr": "Mélanome non résécable ou métastatique BRAF V600", "is_cashback": true, "cashback_company": "Roche Pharma (Suisse) SA", "cashback_calc_type": "chf_fixed", "cashback_calc_value": 3581.40, "cashback_unit": "per_box"}]}
"""


# ============================================================
# Validation
# ============================================================

VALID_CALC_TYPES = {
    "percentage", "chf_fixed", "chf_per_mg", "chf_per_box",
    "full_refund", "undisclosed_fixed", "unknown", None,
}
VALID_UNITS = {
    "per_box", "per_mg", "per_cycle", "per_month", "per_patient",
    "per_year", "per_treatment", "per_dose", "per_week",
    "per_flacon", "per_syringe", "per_pen", "unknown", None,
}


def validate_response(data):
    """Validate LLM JSON response structure. Raises ValueError if invalid."""
    if not isinstance(data, dict):
        raise ValueError("Response must be a dict")
    if "segments" not in data:
        raise ValueError("Missing 'segments' key")
    if not isinstance(data["segments"], list) or len(data["segments"]) == 0:
        raise ValueError("segments must be a non-empty list")
    for seg in data["segments"]:
        if "indication_name_fr" not in seg:
            raise ValueError("segment missing indication_name_fr")
        if "is_cashback" not in seg:
            raise ValueError("segment missing is_cashback")
        if seg.get("cashback_calc_type") not in VALID_CALC_TYPES:
            seg["cashback_calc_type"] = "unknown"
        if seg.get("cashback_unit") not in VALID_UNITS:
            seg["cashback_unit"] = "unknown"


def extract_json(text):
    """Extract JSON from LLM response, handling markdown code blocks."""
    text = text.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
    return json.loads(text)


# ============================================================
# Database setup
# ============================================================

def ensure_schema(conn):
    """Create text_segment_llm table and add columns to limitation_text."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS text_segment_llm (
            segment_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            text_id             INTEGER NOT NULL REFERENCES limitation_text(text_id),
            segment_order       INTEGER NOT NULL,
            indication_name_fr  TEXT,
            segment_text_fr     TEXT,
            is_cashback         INTEGER DEFAULT 0,
            cashback_company    TEXT,
            cashback_calc_type  TEXT,
            cashback_calc_value REAL,
            cashback_unit       TEXT,
            llm_model           TEXT,
            llm_raw_response    TEXT,
            processed_at        TEXT,
            UNIQUE(text_id, segment_order)
        );
        CREATE INDEX IF NOT EXISTS idx_tsllm_text ON text_segment_llm(text_id);
    """)
    # Add columns to limitation_text (ignore if already exist)
    for col, dtype in [
        ("llm_comment", "TEXT"),
        ("llm_is_multi_indication", "INTEGER"),
        ("llm_processed_at", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE limitation_text ADD COLUMN {col} {dtype}")
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()


def get_target_texts(conn, force=False, limit=None):
    """Get pre-2023 cashback texts without regex segments."""
    where_processed = "" if force else "AND lt.llm_processed_at IS NULL"
    query = f"""
        SELECT lt.text_id, lt.description_fr, lt.limitation_code
        FROM limitation_text lt
        JOIN extract_info e ON e.extract_id = lt.first_seen_extract
        WHERE e.file_year < 2023
          AND lt.is_cashback = 1
          AND lt.text_id NOT IN (SELECT text_id FROM text_segment)
          {where_processed}
        ORDER BY lt.text_id
    """
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query).fetchall()


def save_result(conn, text_id, parsed, raw_json, model_name):
    """Write LLM results to database."""
    now = datetime.now().isoformat()

    # Clear previous segments for this text (for --force reruns)
    conn.execute("DELETE FROM text_segment_llm WHERE text_id = ?", (text_id,))

    conn.execute(
        "UPDATE limitation_text SET llm_comment = ?, llm_is_multi_indication = ?, "
        "llm_processed_at = ? WHERE text_id = ?",
        (parsed.get("comment"),
         int(parsed.get("is_multi_indication", False)),
         now, text_id),
    )

    for seg in parsed["segments"]:
        conn.execute(
            "INSERT OR REPLACE INTO text_segment_llm "
            "(text_id, segment_order, indication_name_fr, segment_text_fr, "
            " is_cashback, cashback_company, cashback_calc_type, "
            " cashback_calc_value, cashback_unit, "
            " llm_model, llm_raw_response, processed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (text_id, seg.get("order", 0),
             seg.get("indication_name_fr"),
             seg.get("segment_text_fr"),
             int(seg.get("is_cashback", False)),
             seg.get("cashback_company"),
             seg.get("cashback_calc_type"),
             seg.get("cashback_calc_value"),
             seg.get("cashback_unit"),
             model_name, raw_json, now),
        )
    conn.commit()


def save_error(conn, text_id, raw_text, model_name):
    """Mark text as processed even on error, store raw response."""
    now = datetime.now().isoformat()
    conn.execute(
        "UPDATE limitation_text SET llm_comment = ?, llm_processed_at = ? "
        "WHERE text_id = ?",
        (f"LLM_ERROR: {(raw_text or '')[:200]}", now, text_id),
    )
    conn.commit()


# ============================================================
# Async LLM processing
# ============================================================

async def process_one_text(client, semaphore, text_id, description_fr, lim_code,
                           conn, stats, lock):
    """Process a single text with the LLM, with retry logic."""
    text_len = len(description_fr or "")
    model = MODEL_SONNET if text_len >= LONG_TEXT_THRESHOLD else MODEL_HAIKU

    user_message = f"Analyse ce texte de limitation (text_id={text_id}, code={lim_code}) :\n\n{description_fr}"

    raw_text = None
    for attempt in range(MAX_RETRIES):
        async with semaphore:
            try:
                response = await client.messages.create(
                    model=model,
                    max_tokens=4096,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": user_message}],
                )
                raw_text = response.content[0].text
                parsed = extract_json(raw_text)
                validate_response(parsed)

                async with lock:
                    save_result(conn, text_id, parsed, raw_text, model)
                    stats["processed"] += 1
                    n_segs = len(parsed["segments"])
                    stats["segments"] += n_segs
                    if parsed.get("is_multi_indication"):
                        stats["multi"] += 1
                    stats["cb_segments"] += sum(
                        1 for s in parsed["segments"] if s.get("is_cashback")
                    )
                    if model == MODEL_SONNET:
                        stats["sonnet"] += 1
                    else:
                        stats["haiku"] += 1
                return  # success

            except anthropic.RateLimitError:
                wait = RETRY_BASE_DELAY * (2 ** attempt)
                log.warning(f"Rate limited on text_id={text_id}, waiting {wait}s")
                await asyncio.sleep(wait)
                async with lock:
                    stats["retries"] += 1

            except anthropic.APIError as e:
                log.error(f"API error text_id={text_id}: {e}")
                await asyncio.sleep(RETRY_BASE_DELAY)
                async with lock:
                    stats["retries"] += 1

            except (json.JSONDecodeError, ValueError) as e:
                log.warning(f"Invalid JSON text_id={text_id} attempt {attempt+1}: {e}")
                async with lock:
                    stats["retries"] += 1
                if attempt == MAX_RETRIES - 1:
                    async with lock:
                        save_error(conn, text_id, raw_text, model)
                        stats["errors"] += 1
                    return

    # All retries exhausted
    async with lock:
        save_error(conn, text_id, raw_text, model)
        stats["errors"] += 1


async def run_pipeline(conn, texts, dry_run=False, concurrency=100):
    """Run the async LLM pipeline on all texts."""
    if dry_run:
        log.info("DRY RUN — showing first text prompt:")
        if texts:
            t = texts[0]
            print(f"\n{'='*60}")
            print(f"Model: {MODEL_SONNET if len(t[1] or '') >= LONG_TEXT_THRESHOLD else MODEL_HAIKU}")
            print(f"text_id={t[0]}, code={t[2]}")
            print(f"{'='*60}")
            print(f"\n[SYSTEM PROMPT]\n{SYSTEM_PROMPT[:500]}...\n")
            print(f"[USER MESSAGE]\nAnalyse ce texte de limitation (text_id={t[0]}, code={t[2]}) :\n\n{t[1][:500]}...")
        return

    client = anthropic.AsyncAnthropic()
    semaphore = asyncio.Semaphore(concurrency)
    lock = asyncio.Lock()
    stats = {
        "processed": 0, "errors": 0, "retries": 0,
        "segments": 0, "multi": 0, "cb_segments": 0,
        "haiku": 0, "sonnet": 0,
    }

    log.info(f"Launching {len(texts)} tasks with concurrency={concurrency}")
    start = time.time()

    # Create progress reporting task
    async def report_progress():
        while True:
            await asyncio.sleep(5)
            done = stats["processed"] + stats["errors"]
            if done > 0:
                elapsed = time.time() - start
                rate = done / elapsed
                remaining = (len(texts) - done) / rate if rate > 0 else 0
                log.info(
                    f"  Progress: {done}/{len(texts)} "
                    f"({stats['processed']} ok, {stats['errors']} err, "
                    f"{stats['retries']} retries) "
                    f"~{remaining:.0f}s remaining"
                )
            if done >= len(texts):
                break

    progress_task = asyncio.create_task(report_progress())

    tasks = [
        process_one_text(
            client, semaphore, text_id, desc_fr, lim_code,
            conn, stats, lock,
        )
        for text_id, desc_fr, lim_code in texts
    ]
    await asyncio.gather(*tasks)

    progress_task.cancel()
    try:
        await progress_task
    except asyncio.CancelledError:
        pass

    elapsed = time.time() - start
    log.info(f"Pipeline completed in {elapsed:.1f}s")
    log.info(f"  Processed: {stats['processed']}, Errors: {stats['errors']}, "
             f"Retries: {stats['retries']}")
    log.info(f"  Haiku: {stats['haiku']}, Sonnet: {stats['sonnet']}")
    log.info(f"  Segments: {stats['segments']} ({stats['multi']} multi-indication)")
    log.info(f"  Cashback segments: {stats['cb_segments']}")


# ============================================================
# Validation report
# ============================================================

def generate_report(conn):
    """Compare LLM cashback detection vs regex-based detection."""
    log.info("")
    log.info("=" * 60)
    log.info("VALIDATION REPORT")
    log.info("=" * 60)

    # Total processed
    total = conn.execute(
        "SELECT COUNT(*) FROM limitation_text WHERE llm_processed_at IS NOT NULL"
    ).fetchone()[0]
    errors = conn.execute(
        "SELECT COUNT(*) FROM limitation_text "
        "WHERE llm_processed_at IS NOT NULL AND llm_comment LIKE 'LLM_ERROR%'"
    ).fetchone()[0]
    log.info(f"Total processed: {total} ({errors} errors)")

    # Multi-indication
    multi = conn.execute(
        "SELECT COUNT(*) FROM limitation_text WHERE llm_is_multi_indication = 1"
    ).fetchone()[0]
    log.info(f"Multi-indication detected: {multi}")

    # Segments
    seg_total = conn.execute("SELECT COUNT(*) FROM text_segment_llm").fetchone()[0]
    seg_cb = conn.execute(
        "SELECT COUNT(*) FROM text_segment_llm WHERE is_cashback = 1"
    ).fetchone()[0]
    log.info(f"LLM segments: {seg_total} ({seg_cb} with cashback)")

    # Cashback concordance
    rows = conn.execute("""
        SELECT lt.text_id, lt.is_cashback,
               MAX(ts.is_cashback) as llm_cb
        FROM limitation_text lt
        JOIN text_segment_llm ts ON ts.text_id = lt.text_id
        WHERE lt.llm_processed_at IS NOT NULL
          AND lt.llm_comment NOT LIKE 'LLM_ERROR%'
        GROUP BY lt.text_id
    """).fetchall()

    agree = sum(1 for _, rcb, lcb in rows if rcb == lcb)
    disagree_list = [(tid, rcb, lcb) for tid, rcb, lcb in rows if rcb != lcb]
    log.info(f"\nCashback concordance: {agree}/{len(rows)} agree "
             f"({100*agree/len(rows):.1f}%)" if rows else "No data")

    if disagree_list:
        log.info(f"Disagreements ({len(disagree_list)}):")
        for tid, rcb, lcb in disagree_list[:20]:
            code = conn.execute(
                "SELECT limitation_code FROM limitation_text WHERE text_id = ?",
                (tid,)
            ).fetchone()[0]
            log.info(f"  text_id={tid} ({code}): regex={rcb}, llm={lcb}")

    # Calc type distribution
    log.info("\nLLM cashback_calc_type distribution:")
    rows = conn.execute(
        "SELECT cashback_calc_type, COUNT(*) FROM text_segment_llm "
        "WHERE is_cashback = 1 GROUP BY cashback_calc_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    for ct, cnt in rows:
        log.info(f"  {ct or 'NULL'}: {cnt}")

    # Sample multi-indication results
    log.info("\nSample multi-indication texts:")
    rows = conn.execute("""
        SELECT lt.limitation_code, COUNT(*) as n_segs,
               GROUP_CONCAT(ts.indication_name_fr, ' | ') as names
        FROM text_segment_llm ts
        JOIN limitation_text lt ON lt.text_id = ts.text_id
        WHERE lt.llm_is_multi_indication = 1
        GROUP BY ts.text_id
        ORDER BY n_segs DESC
        LIMIT 10
    """).fetchall()
    for code, n, names in rows:
        log.info(f"  {code} ({n} segments): {names[:120]}")


# ============================================================
# Main
# ============================================================

def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="LLM segmentation of cashback texts")
    parser.add_argument("--limit", type=int, help="Process only N texts")
    parser.add_argument("--dry-run", action="store_true", help="Show prompt without calling API")
    parser.add_argument("--force", action="store_true", help="Reprocess already-processed texts")
    parser.add_argument("--concurrency", type=int, default=100,
                        help="Max concurrent API calls (default: 100)")
    args = parser.parse_args()

    concurrency = args.concurrency

    log.info("=" * 60)
    log.info("LLM Segmentation of Pre-2023 Cashback Texts")
    log.info("=" * 60)

    conn = sqlite3.connect(str(DB_PATH))
    ensure_schema(conn)

    texts = get_target_texts(conn, force=args.force, limit=args.limit)
    log.info(f"Found {len(texts)} texts to process")

    if not texts:
        log.info("Nothing to process.")
        conn.close()
        return

    # Show model distribution
    n_haiku = sum(1 for _, desc, _ in texts if len(desc or "") < LONG_TEXT_THRESHOLD)
    n_sonnet = len(texts) - n_haiku
    log.info(f"  Haiku (<{LONG_TEXT_THRESHOLD} chars): {n_haiku}")
    log.info(f"  Sonnet (>={LONG_TEXT_THRESHOLD} chars): {n_sonnet}")

    asyncio.run(run_pipeline(conn, texts, dry_run=args.dry_run, concurrency=concurrency))

    if not args.dry_run:
        generate_report(conn)

    conn.close()
    log.info("Done!")


if __name__ == "__main__":
    main()

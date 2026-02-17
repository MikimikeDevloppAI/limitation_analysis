"""
Pipeline orchestrator for sku_indication.db.

Runs all steps sequentially to build and enrich the database from BAG XML files.

Usage:
    python run_pipeline.py              # Run all steps
    python run_pipeline.py --step 3     # Run step 03 only
    python run_pipeline.py --from 2     # Run from step 02 onwards
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# ============================================================
# Configuration
# ============================================================

BASE_DIR = Path(r"c:\Users\micha\OneDrive\Matching_indication_code")
EXTRACTED_DIR = BASE_DIR / "extracted"
DB_PATH = BASE_DIR / "sku_indication.db"

# Step registry: (step_number, module_name, description)
STEPS = [
    (1,  "steps.step_01_parse_xml",        "Parse XML files and create DB"),
    (2,  "steps.step_02_init_indications",  "Initialize NOCODE indications"),
    (3,  "steps.step_03_extract_bolds",     "Extract bold names and codes"),
    (4,  "steps.step_04_cashback",          "Detect cashback"),
    (99, "steps.step_99_stats",             "Statistics"),
]


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Run sku_indication.db pipeline")
    parser.add_argument("--step", type=int, help="Run a single step (e.g. --step 3)")
    parser.add_argument("--from", type=int, dest="from_step",
                        help="Run from step N onwards (e.g. --from 2)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger(__name__)

    log.info("=" * 60)
    log.info("BAG SKU Indication Pipeline")
    log.info(f"DB: {DB_PATH}")
    log.info(f"XML: {EXTRACTED_DIR}")
    log.info("=" * 60)

    # Determine which steps to run
    if args.step:
        steps_to_run = [(n, m, d) for n, m, d in STEPS if n == args.step]
        if not steps_to_run:
            log.error(f"Step {args.step} not found. Available: {[n for n,_,_ in STEPS]}")
            sys.exit(1)
    elif args.from_step:
        steps_to_run = [(n, m, d) for n, m, d in STEPS if n >= args.from_step]
    else:
        steps_to_run = STEPS

    log.info(f"Steps to run: {[n for n,_,_ in steps_to_run]}")
    log.info("")

    total_start = time.time()

    for step_num, module_name, description in steps_to_run:
        log.info(f"{'='*60}")
        log.info(f"Step {step_num:02d}: {description}")
        log.info(f"{'='*60}")

        step_start = time.time()

        # Import the step module
        import importlib
        mod = importlib.import_module(module_name)

        # Run the step
        if step_num == 1:
            mod.run(DB_PATH, EXTRACTED_DIR)
        else:
            mod.run(DB_PATH)

        elapsed = time.time() - step_start
        log.info(f"  Completed in {elapsed:.1f}s")
        log.info("")

    total_elapsed = time.time() - total_start
    log.info("=" * 60)
    log.info(f"Pipeline complete in {total_elapsed:.1f}s")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

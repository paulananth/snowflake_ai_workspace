"""
Local/dev sequential pipeline runner for SEC EDGAR Bronze ingest.

Runs all three ingest scripts in the same order that ADF enforces on Azure,
ensuring SEC rate-limit compliance (never parallel, always sequential).

Usage:
  python pipeline.py [--date YYYY-MM-DD] [--limit N]

Examples:
  # Full run (today's date, all NYSE+Nasdaq CIKs):
  python pipeline.py

  # Smoke test (10 CIKs, specific date):
  python pipeline.py --date 2026-04-08 --limit 10

Environment:
  SEC_USER_AGENT   required   e.g. "MyOrg Pipeline you@yourorg.com"
  CLOUD_PROVIDER   optional   "local" (default) | "azure"
  AZURE_STORAGE_ACCOUNT  required if CLOUD_PROVIDER=azure
"""
import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

SCRIPTS = [
    "scripts/ingest/01_ingest_tickers_exchange.py",
    "scripts/ingest/02_ingest_submissions.py",
    "scripts/ingest/03_ingest_companyfacts.py",
]

# Script 01 does not accept --limit (single HTTP call, always fetches all tickers)
_NO_LIMIT = {"01_ingest_tickers_exchange.py"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Ingestion date partition passed to all scripts (default: today)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Max CIKs for scripts 02 and 03 (for dev/smoke testing)",
    )
    args = parser.parse_args()

    root = Path(__file__).parent
    total = len(SCRIPTS)

    for idx, script in enumerate(SCRIPTS, 1):
        script_name = Path(script).name
        cmd = [sys.executable, str(root / script), "--date", args.date]
        if args.limit and script_name not in _NO_LIMIT:
            cmd += ["--limit", str(args.limit)]

        print(f"\n{'=' * 60}")
        print(f"  Step {idx}/{total}: {script_name}")
        print(f"  Command: {' '.join(cmd)}")
        print("=" * 60)

        subprocess.run(cmd, check=True)

    print(f"\n{'=' * 60}")
    print("  Pipeline complete")
    print("=" * 60)


if __name__ == "__main__":
    main()

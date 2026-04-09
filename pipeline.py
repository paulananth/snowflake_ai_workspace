"""
Local/dev sequential pipeline runner for SEC EDGAR Bronze ingest.

Runs all four ingest scripts in the same order that Step Functions enforces on AWS
(and ADF on Azure), ensuring SEC rate-limit compliance (never parallel, always sequential).

Usage:
  python pipeline.py [--date YYYY-MM-DD] [--limit N] [--full-refresh]

Examples:
  # Incremental run (today's date, only CIKs that filed today):
  python pipeline.py

  # Full-refresh (all ~5k NYSE/Nasdaq CIKs — use for initial bootstrap / weekly re-sync):
  python pipeline.py --full-refresh

  # Smoke test (10 CIKs, specific date):
  python pipeline.py --date 2026-04-08 --limit 10

Environment:
  SEC_USER_AGENT   required   e.g. "MyOrg Pipeline you@yourorg.com"
  CLOUD_PROVIDER   optional   "local" (default) | "azure" | "aws"
  AZURE_STORAGE_ACCOUNT  required if CLOUD_PROVIDER=azure
  AWS_BUCKET             required if CLOUD_PROVIDER=aws
"""
import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

SCRIPTS = [
    "scripts/ingest/01_ingest_tickers_exchange.py",
    "scripts/ingest/02_ingest_daily_index.py",
    "scripts/ingest/03_ingest_submissions.py",
    "scripts/ingest/04_ingest_companyfacts.py",
]

# Scripts that do NOT accept --limit (single HTTP call or no CIK loop)
_NO_LIMIT = {"01_ingest_tickers_exchange.py", "02_ingest_daily_index.py"}

# Scripts that accept --full-refresh
_FULL_REFRESH = {
    "02_ingest_daily_index.py",
    "03_ingest_submissions.py",
    "04_ingest_companyfacts.py",
}


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
        help="Max CIKs for scripts 03 and 04 (for dev/smoke testing)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help=(
            "Process all NYSE/Nasdaq CIKs instead of only today's filers. "
            "Use for initial bootstrap and weekly Sunday re-syncs."
        ),
    )
    args = parser.parse_args()

    root = Path(__file__).parent
    total = len(SCRIPTS)

    for idx, script in enumerate(SCRIPTS, 1):
        script_name = Path(script).name
        cmd = [sys.executable, str(root / script), "--date", args.date]
        if args.limit and script_name not in _NO_LIMIT:
            cmd += ["--limit", str(args.limit)]
        if args.full_refresh and script_name in _FULL_REFRESH:
            cmd += ["--full-refresh"]

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


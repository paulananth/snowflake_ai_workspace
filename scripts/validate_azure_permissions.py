"""
Pre-flight permission validator for the SEC EDGAR Bronze ingest pipeline.

Checks every permission the pipeline needs before committing to a full run.
Exits 0 if all checks pass. Exits 1 with a failure summary if any check fails.

On Azure, ADF runs this as the first activity (ValidatePermissions) before the
ingest activities. On AWS, deploy_aws.sh runs it after docker push.

Usage:
  # Local (filesystem only):
  python scripts/validate_azure_permissions.py --cloud local

  # Azure (uses Managed Identity on Batch node):
  python scripts/validate_azure_permissions.py --cloud azure

  # AWS (uses boto3 credential chain — env vars, ~/.aws/credentials, or ECS task role):
  python scripts/validate_azure_permissions.py --cloud aws

Environment (for --cloud azure):
  AZURE_STORAGE_ACCOUNT   required
  AZURE_CONTAINER         optional (default: sec-edgar)

Environment (for --cloud aws):
  AWS_BUCKET              required
  AWS_DEFAULT_REGION      required (or set in ~/.aws/config)

Common:
  SEC_USER_AGENT          required (for EDGAR reachability check)
"""
import argparse
import os
import pathlib
import sys
import time

_ROOT = pathlib.Path(__file__).parent.parent
_INGEST = _ROOT / "scripts" / "ingest"
for _p in (_ROOT, _INGEST):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

# Set a minimal SEC_USER_AGENT if not set, solely for the reachability check.
# The ingest scripts enforce the real value via KeyError; this script only tests connectivity.
if "SEC_USER_AGENT" not in os.environ:
    os.environ["SEC_USER_AGENT"] = "sec-edgar-pipeline permission-check@validate"

from config import settings

AAPL_CIK = "0000320193"   # Apple — reliable, always present in SEC EDGAR


# ── Individual checks ─────────────────────────────────────────────────────────

def _adlfs(cloud: str):
    """
    Return an authenticated AzureBlobFileSystem.

    local  -> AzureCliCredential  (picks up active az login session)
    azure  -> DefaultAzureCredential (Managed Identity on Batch, CLI in dev)
    """
    import adlfs
    if cloud == "local":
        from azure.identity import AzureCliCredential
        credential = AzureCliCredential()
    else:
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()
    return adlfs.AzureBlobFileSystem(
        account_name=settings.AZURE_ACCOUNT,
        credential=credential,
    )


def check_adls_write(cloud: str) -> tuple[bool, str]:
    """Write a small test blob to ADLS, then delete it."""
    if cloud == "local" and not settings.AZURE_ACCOUNT:
        # Running fully local (no Azure) — check filesystem write instead
        test_path = pathlib.Path(settings.STORAGE_ROOT) / ".permission_check"
        try:
            test_path.parent.mkdir(parents=True, exist_ok=True)
            test_path.write_text("ok")
            test_path.unlink()
            return True, f"local write/delete OK ({test_path.parent})"
        except Exception as exc:
            return False, str(exc)

    try:
        fs = _adlfs(cloud)
        blob = f"{settings.AZURE_CONTAINER}/{settings.STORAGE_PREFIX}/.permission_check"
        with fs.open(blob, "wb") as f:
            f.write(b"ok")
        fs.rm(blob)
        return True, f"ADLS write/delete OK (container={settings.AZURE_CONTAINER})"
    except Exception as exc:
        return False, str(exc)


def check_adls_read(cloud: str) -> tuple[bool, str]:
    """List files in the storage root."""
    if cloud == "local" and not settings.AZURE_ACCOUNT:
        root = pathlib.Path(settings.STORAGE_ROOT)
        try:
            root.mkdir(parents=True, exist_ok=True)
            list(root.iterdir())
            return True, f"local filesystem list OK ({root.resolve()})"
        except Exception as exc:
            return False, str(exc)

    try:
        fs = _adlfs(cloud)
        fs.ls(f"{settings.AZURE_CONTAINER}/")
        return True, f"ADLS list OK (account={settings.AZURE_ACCOUNT})"
    except Exception as exc:
        return False, str(exc)


def check_edgar_reachability() -> tuple[bool, str]:
    """Fetch Apple's submission to verify SEC EDGAR is reachable with correct User-Agent."""
    try:
        from _http import edgar_get
        t0 = time.monotonic()
        data = edgar_get(f"https://data.sec.gov/submissions/CIK{AAPL_CIK}.json")
        elapsed = time.monotonic() - t0
        if data is None:
            return False, "Got 404 for Apple CIK — unexpected"
        name = data.get("name", "?")
        return True, f"SEC EDGAR OK — fetched '{name}' in {elapsed:.1f}s"
    except Exception as exc:
        return False, str(exc)


def check_credential_identity(cloud: str) -> tuple[bool, str]:
    """Verify the active credential resolves and print the identity."""
    if cloud == "local" and not settings.AZURE_ACCOUNT:
        return True, "no cloud account configured — skipped"

    if cloud == "aws":
        try:
            import boto3
            identity = boto3.client("sts").get_caller_identity()
            return True, f"STS identity OK — ARN: {identity['Arn']}"
        except Exception as exc:
            return False, f"AWS credential failed: {exc}"

    try:
        if cloud == "local":
            from azure.identity import AzureCliCredential
            cred = AzureCliCredential()
            label = "AzureCliCredential"
        else:
            from azure.identity import DefaultAzureCredential
            cred = DefaultAzureCredential()
            label = "DefaultAzureCredential"
        token = cred.get_token("https://storage.azure.com/.default")
        expires_in = max(0, token.expires_on - int(time.time()))
        return True, f"{label} OK (token expires in {expires_in}s)"
    except Exception as exc:
        return False, f"Credential failed: {exc}"


def check_s3_write(cloud: str) -> tuple[bool, str]:
    """Write a small test object to S3, then delete it."""
    if cloud != "aws":
        return True, "not AWS — skipped"
    try:
        import s3fs
        fs = s3fs.S3FileSystem()
        bucket = settings.AWS_BUCKET
        test_key = f"{bucket}/sec-edgar/bronze/.permission_check"
        with fs.open(test_key, "wb") as f:
            f.write(b"ok")
        fs.rm(test_key)
        return True, f"S3 write/delete OK (s3://{bucket}/sec-edgar/bronze/)"
    except Exception as exc:
        return False, str(exc)


def check_s3_read(cloud: str) -> tuple[bool, str]:
    """List the bucket root to verify GetObject + ListBucket."""
    if cloud != "aws":
        return True, "not AWS — skipped"
    try:
        import s3fs
        fs = s3fs.S3FileSystem()
        bucket = settings.AWS_BUCKET
        fs.ls(f"{bucket}/")
        return True, f"S3 list OK (s3://{bucket}/)"
    except Exception as exc:
        return False, str(exc)


# ── Runner ────────────────────────────────────────────────────────────────────

CHECKS = [
    ("Credential / identity",    lambda cloud: check_credential_identity(cloud)),
    ("Storage read (list)",      lambda cloud: check_s3_read(cloud) if cloud == "aws" else check_adls_read(cloud)),
    ("Storage write + delete",   lambda cloud: check_s3_write(cloud) if cloud == "aws" else check_adls_write(cloud)),
    ("SEC EDGAR reachability",   lambda cloud: check_edgar_reachability()),
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cloud",
        choices=["local", "azure", "aws"],
        default=os.environ.get("CLOUD_PROVIDER", "local"),
        help="Cloud provider to validate (default: CLOUD_PROVIDER env or 'local')",
    )
    args = parser.parse_args()
    cloud = args.cloud

    print(f"{'=' * 60}")
    print(f"  SEC EDGAR Pipeline — Permission Validation")
    print(f"  Cloud: {cloud}")
    if cloud == "azure":
        print(f"  Storage account: {settings.AZURE_ACCOUNT or '(not set)'}")
        print(f"  Container:       {settings.AZURE_CONTAINER}")
    elif cloud == "aws":
        print(f"  S3 bucket:       {settings.AWS_BUCKET or '(not set)'}")
        print(f"  Region:          {settings.AWS_DEFAULT_REGION or '(not set)'}")
    print(f"{'=' * 60}")

    results: list[tuple[str, bool, str]] = []
    for label, check_fn in CHECKS:
        print(f"\n  Checking: {label} ...", end=" ", flush=True)
        try:
            passed, detail = check_fn(cloud)
        except Exception as exc:
            passed, detail = False, f"Unhandled error: {exc}"
        status = "PASS" if passed else "FAIL"
        print(status)
        print(f"    {detail}")
        results.append((label, passed, detail))

    # Summary
    passed_count = sum(1 for _, p, _ in results if p)
    total = len(results)
    print(f"\n{'=' * 60}")
    print(f"  Results: {passed_count}/{total} checks passed")
    print(f"{'=' * 60}")

    failures = [(label, detail) for label, passed, detail in results if not passed]
    if failures:
        print("\n  FAILURES:")
        for label, detail in failures:
            print(f"    ✗ {label}: {detail}")
        print()
        sys.exit(1)
    else:
        print("\n  All checks passed. Pipeline is ready to run.")
        sys.exit(0)


if __name__ == "__main__":
    main()

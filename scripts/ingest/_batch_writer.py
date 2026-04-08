"""
Write (and read) PyArrow tables as Parquet — local filesystem or Azure Blob Storage.

Cloud provider is determined by config.settings.CLOUD_PROVIDER at call time.
Azure auth uses DefaultAzureCredential:
  - Local dev:  az login (picks up your interactive session)
  - Azure Batch: Managed Identity assigned to the pool (no secrets in code)
"""
import io
import pathlib
import re
import sys

import pyarrow as pa
import pyarrow.parquet as pq

_ROOT = pathlib.Path(__file__).parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import settings


def _abfss_to_blob_path(abfss_path: str) -> str:
    """
    Convert abfss://container@account.dfs.core.windows.net/rest  →  container/rest

    adlfs.AzureBlobFileSystem.open() expects paths in the form
    {container}/{blob_path}, not the full abfss:// URI.
    """
    m = re.match(r"abfss://([^@]+)@[^/]+/(.+)", abfss_path)
    if not m:
        raise ValueError(f"Invalid abfss path: {abfss_path!r}")
    return f"{m.group(1)}/{m.group(2)}"


def write_parquet(table: pa.Table, path: str) -> None:
    """
    Write table to path with Snappy compression.

    path format:
      local: "output/bronze/submissions_meta/..."
      azure: "abfss://sec-edgar@account.dfs.core.windows.net/sec-edgar/bronze/..."
    """
    if settings.CLOUD_PROVIDER == "local":
        local = pathlib.Path(path)
        local.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(local), compression="snappy")
        return

    if settings.CLOUD_PROVIDER == "azure":
        import adlfs
        fs = adlfs.AzureBlobFileSystem(account_name=settings.AZURE_ACCOUNT)
        blob_path = _abfss_to_blob_path(path)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        with fs.open(blob_path, "wb") as f:
            f.write(buf.getvalue())
        return

    raise ValueError(f"Unknown CLOUD_PROVIDER: {settings.CLOUD_PROVIDER!r}")


def read_parquet(path: str) -> pa.Table:
    """
    Read a Parquet file from local or Azure Blob Storage.

    Used by scripts 02 and 03 to load the tickers table produced by script 01.
    """
    if settings.CLOUD_PROVIDER == "local":
        return pq.read_table(path)

    if settings.CLOUD_PROVIDER == "azure":
        import adlfs
        fs = adlfs.AzureBlobFileSystem(account_name=settings.AZURE_ACCOUNT)
        blob_path = _abfss_to_blob_path(path)
        return pq.read_table(blob_path, filesystem=fs)

    raise ValueError(f"Unknown CLOUD_PROVIDER: {settings.CLOUD_PROVIDER!r}")

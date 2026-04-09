"""
Write and read PyArrow tables as Parquet on local filesystem, Azure Blob Storage, or AWS S3.

Cloud provider is determined by config.settings.CLOUD_PROVIDER at call time.

Auth:
  local  - plain filesystem, no credentials
  azure  - DefaultAzureCredential (az login locally; Managed Identity on Batch)
  aws    - boto3 credential chain (env vars -> ~/.aws/credentials -> ECS task role)
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
    Convert abfss://container@account.dfs.core.windows.net/rest -> container/rest

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
      aws:   "s3://bucket/sec-edgar/bronze/..."
    """
    if settings.CLOUD_PROVIDER == "local":
        local = pathlib.Path(path)
        local.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(local), compression="snappy")
        return

    if settings.CLOUD_PROVIDER == "azure":
        import adlfs
        from azure.identity import DefaultAzureCredential
        fs = adlfs.AzureBlobFileSystem(
            account_name=settings.AZURE_ACCOUNT,
            credential=DefaultAzureCredential(),
        )
        blob_path = _abfss_to_blob_path(path)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        with fs.open(blob_path, "wb") as f:
            f.write(buf.getvalue())
        return

    if settings.CLOUD_PROVIDER == "aws":
        import s3fs
        fs = s3fs.S3FileSystem()  # boto3 chain: env -> profile -> ECS task role
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        with fs.open(path, "wb") as f:
            f.write(buf.getvalue())
        return

    raise ValueError(f"Unknown CLOUD_PROVIDER: {settings.CLOUD_PROVIDER!r}")


def read_parquet(path: str) -> pa.Table:
    """
    Read a Parquet file from local, Azure Blob Storage, or AWS S3.

    Used by downstream scripts to load tables produced by the preceding script.
    """
    if settings.CLOUD_PROVIDER == "local":
        with open(path, "rb") as handle:
            return pq.read_table(handle)

    if settings.CLOUD_PROVIDER == "azure":
        import adlfs
        from azure.identity import DefaultAzureCredential
        fs = adlfs.AzureBlobFileSystem(
            account_name=settings.AZURE_ACCOUNT,
            credential=DefaultAzureCredential(),
        )
        blob_path = _abfss_to_blob_path(path)
        with fs.open(blob_path, "rb") as handle:
            return pq.read_table(handle)

    if settings.CLOUD_PROVIDER == "aws":
        import s3fs
        fs = s3fs.S3FileSystem()
        with fs.open(path, "rb") as handle:
            return pq.read_table(handle)

    raise ValueError(f"Unknown CLOUD_PROVIDER: {settings.CLOUD_PROVIDER!r}")


def parquet_exists(path: str) -> bool:
    """
    Return True if the Parquet file at path already exists.

    Used for idempotency checks and skip-existing-batches resume logic.
    """
    if settings.CLOUD_PROVIDER == "local":
        return pathlib.Path(path).exists()

    if settings.CLOUD_PROVIDER == "azure":
        import adlfs
        from azure.identity import DefaultAzureCredential
        fs = adlfs.AzureBlobFileSystem(
            account_name=settings.AZURE_ACCOUNT,
            credential=DefaultAzureCredential(),
        )
        try:
            return fs.exists(_abfss_to_blob_path(path))
        except Exception:
            return False

    if settings.CLOUD_PROVIDER == "aws":
        import s3fs
        try:
            return s3fs.S3FileSystem().exists(path)
        except Exception:
            return False

    raise ValueError(f"Unknown CLOUD_PROVIDER: {settings.CLOUD_PROVIDER!r}")

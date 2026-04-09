FROM python:3.11-slim

WORKDIR /app

# Install uv (fast Python package manager)
RUN pip install --no-cache-dir uv

# Copy dependency files first for better layer caching.
# When only source code changes, this layer is reused.
COPY pyproject.toml .python-version ./

# Install production dependencies only (excludes dev group: jupyter, ipykernel).
# --group aws pulls in s3fs for AWS deployments; adlfs/azure-identity are already
# in the default dependency set so both Azure and AWS are covered by one image.
RUN uv sync --no-dev --group aws

# Add the uv-managed venv to PATH so 'python' resolves to the venv's interpreter
ENV PATH=/app/.venv/bin:$PATH

# Add repo root to PYTHONPATH so 'from config import settings' works
# when scripts are invoked as  python scripts/ingest/0N_....py
ENV PYTHONPATH=/app

# Verify venv works and key deps are importable before shipping
RUN python -c "import pyarrow, s3fs, requests; print('deps OK')"

# Copy source - config, scripts, and pipeline runner
COPY config/ config/
COPY scripts/ scripts/
COPY pipeline.py .

# -u: unbuffered stdout/stderr so CloudWatch captures all output even on fast crashes
ENTRYPOINT ["python", "-u"]
CMD ["pipeline.py"]


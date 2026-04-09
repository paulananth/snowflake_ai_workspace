"""
SEC EDGAR HTTP client.

Mandatory compliance (SEC Developer Resources):
  - User-Agent header: "ApplicationName ContactEmail" on EVERY request
  - Raises KeyError at import time if SEC_USER_AGENT env var is not set,
    preventing any requests without a valid User-Agent.

Reference: https://www.sec.gov/developer
"""
import os
import requests
from requests.adapters import HTTPAdapter, Retry

# REQUIRED env var -- KeyError at startup if missing (intentional fail-fast)
_USER_AGENT: str = os.environ["SEC_USER_AGENT"]

_RETRY = Retry(
    total=3,
    backoff_factor=1.0,
    status_forcelist=[429, 500, 502, 503],
    allowed_methods=["GET"],
)

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": _USER_AGENT})
_SESSION.mount("https://", HTTPAdapter(max_retries=_RETRY))


def edgar_get(url: str, timeout: int = 20) -> dict | None:
    """
    Fetch a JSON endpoint from SEC EDGAR.

    Returns:
        Parsed JSON dict on success.
        None on HTTP 404 (company not found / no filing).

    Raises:
        requests.HTTPError for non-404 HTTP errors (after retries).
        requests.ConnectionError / Timeout on network failures.
    """
    try:
        resp = _SESSION.get(url, timeout=timeout)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        raise RuntimeError(f"Timeout fetching {url}") from None


def edgar_get_text(url: str, timeout: int = 30) -> str | None:
    """
    Fetch a plain-text endpoint from SEC EDGAR (e.g. daily-index .idx files).

    Returns:
        Response body as a string on success.
        None on HTTP 404.

    Raises:
        requests.HTTPError for non-404 HTTP errors (after retries).
        requests.ConnectionError / Timeout on network failures.
    """
    try:
        resp = _SESSION.get(url, timeout=timeout)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.Timeout:
        raise RuntimeError(f"Timeout fetching {url}") from None

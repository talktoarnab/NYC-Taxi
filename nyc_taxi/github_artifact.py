"""
Load ETL `output/` from a GitHub Actions workflow artifact (zip).

Requires a personal access token with **read** access to **Actions** for the repository
(fine-grained: "Actions" read, or classic **repo** scope for private repos).

API: https://docs.github.com/en/rest/actions/artifacts
"""
from __future__ import annotations

import io
import os
import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

GITHUB_API = "https://api.github.com"


def _headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def parse_repo(value: str) -> tuple[str, str]:
    """``owner/name`` or ``owner/name`` with optional ``https://github.com/`` prefix."""
    s = value.strip()
    for prefix in ("https://github.com/", "github.com/"):
        if s.startswith(prefix):
            s = s[len(prefix) :]
    s = s.strip("/")
    parts = s.split("/")
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"Invalid GITHUB_REPO (expected owner/repo): {value!r}")
    return parts[0], parts[1]


def list_artifacts_paginated(
    token: str, owner: str, repo: str, per_page: int = 100
) -> list[dict[str, Any]]:
    """Return all artifacts for a repo (paginated GET)."""
    out: list[dict[str, Any]] = []
    url: str | None = f"{GITHUB_API}/repos/{owner}/{repo}/actions/artifacts"
    params: dict[str, int | str] = {"per_page": per_page}
    session = requests.Session()
    while url:
        r = session.get(url, headers=_headers(token), params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("artifacts", []))
        # Follow Link: rel=next
        link = r.headers.get("Link", "")
        url = None
        params = {}  # only first request uses per_page; next is full URL
        for part in link.split(","):
            if 'rel="next"' in part:
                url = part[part.find("<") + 1 : part.find(">")]
                break
    return out


def _artifact_recency_key(a: dict[str, Any]) -> tuple[float, int]:
    """Sort key: most recent first (UTC ``created_at``, then higher ``id`` as tie-breaker)."""
    s = a.get("created_at") or ""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        ts = datetime.fromisoformat(s)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        t = ts.timestamp()
    except (ValueError, OSError, TypeError):
        t = 0.0
    return (t, int(a.get("id", 0) or 0))


def get_latest_artifact(
    token: str,
    owner: str,
    repo: str,
    artifact_name: str,
) -> dict[str, Any] | None:
    """Newest non-expired artifact with the given `name` (by ``created_at``, then ``id``)."""
    arts = list_artifacts_paginated(token, owner, repo)
    candidates = [
        a
        for a in arts
        if a.get("name") == artifact_name and not a.get("expired", False)
    ]
    if not candidates:
        return None
    candidates.sort(key=_artifact_recency_key, reverse=True)
    return candidates[0]


def download_artifact_zip(
    token: str,
    owner: str,
    repo: str,
    artifact_id: int,
) -> bytes:
    url = f"{GITHUB_API}/repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip"
    r = requests.get(url, headers=_headers(token), stream=True, timeout=600)
    r.raise_for_status()
    buf = io.BytesIO()
    for chunk in r.iter_content(chunk_size=1 << 20):
        if chunk:
            buf.write(chunk)
    return buf.getvalue()


def resolve_artifact_layout(extract_root: Path) -> tuple[Path, Path | None]:
    """
    Return ``(config_base_dir, artifact_output_root | None)`` for :class:`nyc_taxi.config.Config`.

    GitHub ``upload-artifact`` with ``path: output/`` zips paths **relative to** that
    folder, so the zip root is often ``gold/`` + ``kpi/`` (no ``output/``). This repo’s
    workflow stages ``output`` under ``_artifact_staging/`` so the artifact includes
    ``output/gold/`` like a local run. Both flat and nested layouts are supported here.
    """
    p_nested = extract_root / "output" / "gold" / "nyc_taxi_gold.parquet"
    if p_nested.is_file():
        return extract_root, None

    p_flat = extract_root / "gold" / "nyc_taxi_gold.parquet"
    if p_flat.is_file():
        return extract_root, extract_root  # gold + kpi at zip root (common GHA layout)

    found = [
        p
        for p in extract_root.rglob("nyc_taxi_gold.parquet")
        if p.is_file() and p.parent.name == "gold"
    ]
    if not found:
        raise FileNotFoundError(
            f"Could not find nyc_taxi_gold.parquet under artifact path {extract_root!s}"
        )
    p = min(found, key=lambda x: len(x.parts))
    if p.parents[1].name == "output":
        return p.parents[2], None
    data_root = p.parents[1]
    return extract_root, data_root


def download_and_extract_latest_artifact(
    token: str,
    owner: str,
    repo: str,
    artifact_name: str,
    cache_root: Path | None = None,
) -> tuple[Path, Path | None, dict[str, Any]]:
    """
    Download the latest named artifact, unzip, return ``(config_base_dir, artifact_output_root, meta)``.

    ``artifact_output_root`` is set when the zip uses a *flat* ``gold/``/``kpi/`` layout
    (no ``output/`` directory); pass it to :class:`nyc_taxi.config.Config` as
    ``artifact_output_root=``.

    Caches by artifact `id` under ``cache_root`` (default: ``~/.cache/nyc_taxi_streamlit``).
    Reuses cache if the directory already contains a valid layout for the same id.
    """
    if cache_root is None:
        env = os.environ.get("NYC_TAXI_ARTIFACT_CACHE")
        cache_root = Path(env) if env else (Path.home() / ".cache" / "nyc_taxi_streamlit")
    meta = get_latest_artifact(token, owner, repo, artifact_name)
    if meta is None:
        raise RuntimeError(
            f'No non-expired artifact named {artifact_name!r} in {owner}/{repo}. '
            "Check the workflow `upload-artifact` name and retention."
        )
    aid = int(meta["id"])
    target = cache_root / f"artifact_{aid}"
    try:
        base_cached, aor = resolve_artifact_layout(target)
        g = (
            (base_cached / "output" / "gold" / "nyc_taxi_gold.parquet")
            if aor is None
            else (aor / "gold" / "nyc_taxi_gold.parquet")
        )
        if g.is_file():
            return base_cached, aor, meta
    except FileNotFoundError:
        pass

    if target.exists():
        shutil.rmtree(target, ignore_errors=True)
    target.mkdir(parents=True, exist_ok=True)
    data = download_artifact_zip(token, owner, repo, aid)
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        zf.extractall(target)

    base, aor = resolve_artifact_layout(target)
    return base, aor, meta

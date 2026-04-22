"""
Load ETL `output/` from a GitHub Actions workflow artifact (zip).

Requires a personal access token with **read** access to **Actions** for the repository
(fine-grained: "Actions" read, or classic **repo** scope for private repos).

API: https://docs.github.com/en/rest/actions/artifacts
"""
from __future__ import annotations

import io
import os
import zipfile
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


def get_latest_artifact(
    token: str,
    owner: str,
    repo: str,
    artifact_name: str,
) -> dict[str, Any] | None:
    """Newest non-expired artifact with the given `name` (by `created_at`)."""
    arts = list_artifacts_paginated(token, owner, repo)
    candidates = [
        a
        for a in arts
        if a.get("name") == artifact_name and not a.get("expired", False)
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda x: x.get("created_at", ""), reverse=True)
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


def find_project_base_dir(extract_root: Path) -> Path:
    """
    After unzipping a workflow artifact, locate the directory that should be
    :attr:`Config.base_dir` (must contain ``output/gold/nyc_taxi_gold.parquet``).
    """
    direct = extract_root / "output" / "gold" / "nyc_taxi_gold.parquet"
    if direct.is_file():
        return extract_root

    for p in extract_root.rglob("nyc_taxi_gold.parquet"):
        # expect .../output/gold/nyc_taxi_gold.parquet
        if p.parent.name == "gold" and p.parents[1].name == "output":
            return p.parents[2]
    raise FileNotFoundError(
        f"Could not find nyc_taxi_gold.parquet under artifact extract path {extract_root!s}"
    )


def download_and_extract_latest_artifact(
    token: str,
    owner: str,
    repo: str,
    artifact_name: str,
    cache_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    """
    Download the latest named artifact, unzip, return ``(config_base_dir, artifact_meta)``.

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
        base_cached = find_project_base_dir(target)
        gold = base_cached / "output" / "gold" / "nyc_taxi_gold.parquet"
        if gold.is_file():
            return base_cached, meta
    except FileNotFoundError:
        pass

    target.mkdir(parents=True, exist_ok=True)
    data = download_artifact_zip(token, owner, repo, aid)
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        zf.extractall(target)

    base = find_project_base_dir(target)
    return base, meta

"""Tests for the layers publisher: the combined DuckDB goes into the day's release.

There is one release per data date; the bronze CSV and the cumulative DuckDB share the tag. The
publisher adds the DuckDB to that release (reused if the bronze job already created it).
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from lib.workspace.publish_layers import publish_layers

REPO = "dataengineergrav/rental-market-dynamics-dubai"
API = "https://api.github.com"
UPLOADS = "https://uploads.github.com"
TAG = "release-2026-10-05"


def _release_json(tag=TAG):
    return {
        "id": 1,
        "name": tag.replace("release-", "Release ", 1),
        "upload_url": f"{UPLOADS}/repos/{REPO}/releases/1/assets{{?name,label}}",
        "assets_url": f"{API}/repos/{REPO}/releases/1/assets",
    }


def _seed(requests_mock, tag=TAG):
    requests_mock.get(f"{API}/repos/{REPO}/releases/tags/{tag}", status_code=404)
    requests_mock.post(f"{API}/repos/{REPO}/releases", status_code=201, json=_release_json(tag))
    requests_mock.get(f"{API}/repos/{REPO}/releases/1/assets", status_code=200, json=[])
    requests_mock.post(f"{UPLOADS}/repos/{REPO}/releases/1/assets?name=rents_layers.duckdb", status_code=201)


def test_derives_the_dated_tag_and_uploads_one_asset(requests_mock, tmp_path):
    artifact = tmp_path / "rents_layers.duckdb"
    artifact.write_bytes(b"x" * 32)
    _seed(requests_mock)

    with patch.dict(os.environ, {"GH_TOKEN": "t"}):
        tag = publish_layers(artifact, repo=REPO, data_through="2026-10-05")

    assert tag == TAG
    uploaded = [
        r.url.split("name=")[-1]
        for r in requests_mock.request_history
        if r.method == "POST" and r.url.startswith(UPLOADS)
    ]
    assert uploaded == ["rents_layers.duckdb"], "one combined artifact, one asset"


def test_explicit_tag_is_respected(requests_mock, tmp_path):
    artifact = tmp_path / "rents_layers.duckdb"
    artifact.write_bytes(b"x" * 32)
    _seed(requests_mock, tag="release-2026-01-01")

    with patch.dict(os.environ, {"GH_TOKEN": "t"}):
        tag = publish_layers(artifact, repo=REPO, tag="release-2026-01-01")

    assert tag == "release-2026-01-01"


def test_missing_artifact_fails_before_any_upload(requests_mock, tmp_path):
    _seed(requests_mock)

    with pytest.raises(FileNotFoundError, match="layers artifact not found"):
        publish_layers(tmp_path / "missing.duckdb", repo=REPO, data_through="2026-10-05")

    created = [
        r for r in requests_mock.request_history if r.method == "POST" and r.url == f"{API}/repos/{REPO}/releases"
    ]
    assert created == [], "no release may be created when the artifact is missing"


def test_no_tag_or_date_raises(tmp_path):
    artifact = tmp_path / "rents_layers.duckdb"
    artifact.write_bytes(b"x" * 32)
    with pytest.raises(ValueError, match="needs --tag or --data-through"):
        publish_layers(artifact, repo=REPO)

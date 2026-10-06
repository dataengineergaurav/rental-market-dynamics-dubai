"""Tests for the layers publisher.

The combined DuckDB is published to one stable tag (`release-layers-latest`), clobbered on each
run; this pins the tag, the single asset, and the validate-before-upload guarantee.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from lib.workspace.publish_layers import publish_layers

REPO = "dataengineergrav/rental-market-dynamics-dubai"
API = "https://api.github.com"
UPLOADS = "https://uploads.github.com"
TAG = "release-layers-latest"


def _release_json():
    return {
        "id": 1,
        "name": "layers latest",
        "upload_url": f"{UPLOADS}/repos/{REPO}/releases/1/assets{{?name,label}}",
        "assets_url": f"{API}/repos/{REPO}/releases/1/assets",
    }


def _seed(requests_mock):
    requests_mock.get(f"{API}/repos/{REPO}/releases/tags/{TAG}", status_code=404)
    requests_mock.post(f"{API}/repos/{REPO}/releases", status_code=201, json=_release_json())
    requests_mock.get(f"{API}/repos/{REPO}/releases/1/assets", status_code=200, json=[])
    requests_mock.post(f"{UPLOADS}/repos/{REPO}/releases/1/assets?name=rents_layers.duckdb", status_code=201)


def test_publishes_artifact_to_the_stable_tag(requests_mock, tmp_path):
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


def test_missing_artifact_fails_before_any_upload(requests_mock, tmp_path):
    _seed(requests_mock)

    with pytest.raises(FileNotFoundError, match="layers artifact not found"):
        publish_layers(tmp_path / "missing.duckdb", repo=REPO)

    created = [
        r
        for r in requests_mock.request_history
        if r.method == "POST" and r.url == f"{API}/repos/{REPO}/releases"
    ]
    assert created == [], "no release may be created when the artifact is missing"

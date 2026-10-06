"""Tests for the unified layer publisher.

Both the daily bronze publish and the weekly Silver/Gold publish go through `GitHubRelease`; this
pins the weekly path's tags, assets and its validate-before-upload guarantee.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from lib.workspace.publish_layers import publish_weekly_layers

REPO = "dataengineergrav/rental-market-dynamics-dubai"
API = "https://api.github.com"
UPLOADS = "https://uploads.github.com"


def _release_json():
    return {
        "id": 1,
        "name": "x",
        "upload_url": f"{UPLOADS}/repos/{REPO}/releases/1/assets{{?name,label}}",
        "assets_url": f"{API}/repos/{REPO}/releases/1/assets",
    }


def _seed(requests_mock, silver: bool = True, gold: bool = True):
    for tag in ("release-silver-2026W39", "release-gold-2026W39"):
        requests_mock.get(f"{API}/repos/{REPO}/releases/tags/{tag}", status_code=404)
    requests_mock.post(f"{API}/repos/{REPO}/releases", status_code=201, json=_release_json())
    requests_mock.get(f"{API}/repos/{REPO}/releases/1/assets", status_code=200, json=[])
    for name in ("silver_2026W39.duckdb", "gold_2026W39.duckdb"):
        requests_mock.post(f"{UPLOADS}/repos/{REPO}/releases/1/assets?name={name}", status_code=201)


def test_publishes_each_layer_to_its_own_tag(requests_mock, tmp_path):
    silver = tmp_path / "silver_2026W39.duckdb"
    gold = tmp_path / "gold_2026W39.duckdb"
    silver.write_bytes(b"x" * 32)
    gold.write_bytes(b"y" * 32)
    _seed(requests_mock)

    with patch.dict(os.environ, {"GH_TOKEN": "t"}):
        tags = publish_weekly_layers(silver, gold, "2026W39", repo=REPO)

    assert tags == ["release-silver-2026W39", "release-gold-2026W39"]
    uploaded = [
        r.url.split("name=")[-1]
        for r in requests_mock.request_history
        if r.method == "POST" and r.url.startswith(UPLOADS)
    ]
    assert sorted(uploaded) == ["gold_2026W39.duckdb", "silver_2026W39.duckdb"]


def test_missing_artifact_fails_before_any_upload(requests_mock, tmp_path):
    silver = tmp_path / "silver_2026W39.duckdb"
    silver.write_bytes(b"x" * 32)  # gold intentionally missing
    _seed(requests_mock)

    with patch.dict(os.environ, {"GH_TOKEN": "t"}):
        with pytest.raises(FileNotFoundError, match="gold artifact not found"):
            publish_weekly_layers(silver, tmp_path / "gold_2026W39.duckdb", "2026W39", repo=REPO)

    created = [r for r in requests_mock.request_history if r.method == "POST" and r.url == f"{API}/repos/{REPO}/releases"]
    assert created == [], "no release may be created when an artifact is missing"

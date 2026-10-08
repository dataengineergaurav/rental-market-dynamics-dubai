"""EJARI_URL must never reach a log line: the endpoint can carry a token in its query string.

The download log used to interpolate the raw URL, so a token in `?token=...` landed in
`etl.log` and the CI logs. These pin the redaction at the call site, not just the helper.
"""

from __future__ import annotations

import logging

import run_etl_pipeline
from run_etl_pipeline import _redact_url, download_rents


def test_redact_url_strips_query_string():
    assert _redact_url("https://gateway.example.com/rents?token=SECRET123") == ("https://gateway.example.com/rents")


def test_redact_url_keeps_host_and_port():
    assert _redact_url("http://host:8080/a/b?x=1&y=2") == "http://host:8080/a/b"


def test_redact_url_placeholder_for_unparseable():
    assert _redact_url("not a url") == "<configured EJARI_URL endpoint>"
    assert _redact_url("") == "<configured EJARI_URL endpoint>"


def test_download_log_does_not_leak_the_url_token(tmp_path, monkeypatch, caplog):
    token = "SUPERSECRETTOKEN"
    url = f"https://gateway.example.com/rents?token={token}"
    target = tmp_path / "rent_contracts_20260101.csv"

    class _StubDownloader:
        def __init__(self, _url):
            pass

        def run(self, filename):
            from pathlib import Path

            Path(filename).write_text("a,b\n1,2\n", encoding="utf-8")
            return True

    # example.com in the URL routes to the direct downloader; stub it so no network is hit.
    monkeypatch.setattr(run_etl_pipeline, "EjariRentsDownloader", _StubDownloader)

    with caplog.at_level(logging.INFO, logger="READ.ETL"):
        download_rents(url, str(target))

    assert token not in caplog.text
    assert "token=" not in caplog.text
    assert "https://gateway.example.com/rents" in caplog.text

"""
Ejari rents downloader — POSTs the configured EJARI_URL endpoint
(Rent Transaction Details). Params: P_DATE_TYPE, P_VERSION, P_IS_FREE_HOLD etc.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests

from lib.config import API_CONFIG

logger = logging.getLogger(__name__)


class EjariRentsDownloader:
    """Paginated rents fetcher: POST EJARI_URL -> CSV/JSON."""

    def __init__(self, url: str | None = None):
        self.url = url or os.getenv("EJARI_URL") or API_CONFIG["gateway_rents_url"]
        if not self.url:
            raise ValueError("EJARI_URL environment variable not set")
        self.timeout = 40  # rents can be slow vs default 30
        self.max_retries = API_CONFIG["max_retries"]
        self.backoff_factor = API_CONFIG["retry_backoff_factor"]

    def _body(
        self, take: int, skip: int, from_date: str = "01/01/2020", to_date: str | None = None, date_type: str = "0"
    ) -> dict:
        if to_date is None:
            now = datetime.now(timezone.utc)
            to_date = f"{now.month:02d}/{now.day:02d}/{now.year}"
        # rents params from publicData.js: rents -> ["P_FROM_DATE","P_TO_DATE","P_DATE_TYPE","P_IS_FREE_HOLD","P_VERSION","P_AREA_ID","P_USAGE_ID","P_PROP_TYPE_ID","P_TAKE","P_SKIP","P_SORT"]
        return {
            "P_FROM_DATE": from_date,
            "P_TO_DATE": to_date,
            "P_DATE_TYPE": str(date_type),  # 0=Registration, 1=Start, 2=End
            "P_IS_FREE_HOLD": "",
            "P_VERSION": "",
            "P_AREA_ID": "",
            "P_USAGE_ID": "",
            "P_PROP_TYPE_ID": "",
            "P_TAKE": str(take),
            "P_SKIP": str(skip),
            "P_SORT": "REGISTRATION_DATE_ASC",
        }

    def run(self, filename: str, from_date: str = "01/01/2020", to_date: str | None = None) -> bool:
        take, skip = 1000, 0
        all_rows: list[dict] = []
        is_json = filename.lower().endswith(".json")
        while True:
            body = self._body(take, skip, from_date, to_date)
            for attempt in range(self.max_retries):
                try:
                    resp = requests.post(self.url, json=body, timeout=self.timeout)
                    resp.raise_for_status()
                    payload = resp.json()
                    if payload.get("responseCode") != 200:
                        raise RuntimeError(
                            f"Gateway {payload.get('responseCode')}: {payload.get('validationErrorsList')}"
                        )
                    rows = payload.get("response", {}).get("result", []) or []
                    break
                except Exception as e:
                    if attempt == self.max_retries - 1:
                        logger.error(f"Rents download failed: {e}")
                        return False
                    wait = self.backoff_factor**attempt
                    logger.warning(f"Rents retry {attempt + 1}/{self.max_retries}: {e} (wait {wait}s)")
                    time.sleep(wait)
            if not rows:
                break
            all_rows.extend(rows)
            logger.info(f"Fetched {len(rows)} rents (skip={skip}, total={rows[0].get('TOTAL')})")
            if len(rows) < take:
                break
            skip += take
        if not all_rows:
            logger.error("Rents returned no rows")
            return False
        if is_json:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(all_rows, f, ensure_ascii=False, indent=2)
        else:
            with open(filename, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
                w.writeheader()
                w.writerows(all_rows)
        logger.info(f"Rents complete: {len(all_rows)} rows -> {filename}")
        return True

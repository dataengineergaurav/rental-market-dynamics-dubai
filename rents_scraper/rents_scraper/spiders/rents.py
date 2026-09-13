import os
import scrapy
from scrapy.http import JsonRequest
from datetime import datetime, timezone

RENTS_URL = os.getenv("EJARI_URL")

def _fmt(d): return f"{d.month:02d}/{d.day:02d}/{d.year}"

class RentsSpider(scrapy.Spider):
    """POST the configured EJARI_URL endpoint (Rent Transaction Details).

    Params: ["P_FROM_DATE","P_TO_DATE","P_DATE_TYPE","P_IS_FREE_HOLD","P_VERSION","P_AREA_ID","P_USAGE_ID","P_PROP_TYPE_ID","P_TAKE","P_SKIP","P_SORT"]

    Usage:
      scrapy crawl rents -O output/rents.jsonl
      scrapy crawl rents -a from_date=09/10/2026 -a to_date=09/13/2026 -a date_type=0 -O /tmp/rents.csv
    """
    name = "rents"
    # ponytail: allowed_domains derived from EJARI_URL at runtime; hardcoding leaks data source
    allowed_domains = []
    custom_settings = {"FEED_EXPORT_ENCODING": "utf-8", "CONCURRENT_REQUESTS_PER_DOMAIN": 1}

    def __init__(self, from_date="01/01/2020", to_date=None, date_type="0", take="1000", url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url or RENTS_URL
        if not self.url:
            raise ValueError("EJARI_URL environment variable not set (or pass -a url=...)")
        self.from_date = from_date
        self.to_date = to_date or _fmt(datetime.now(timezone.utc))
        self.date_type = str(date_type)
        self.take = int(take)

    def _body(self, skip):
        return {
            "P_FROM_DATE": self.from_date,
            "P_TO_DATE": self.to_date,
            "P_DATE_TYPE": self.date_type,
            "P_IS_FREE_HOLD": "",
            "P_VERSION": "",
            "P_AREA_ID": "",
            "P_USAGE_ID": "",
            "P_PROP_TYPE_ID": "",
            "P_TAKE": str(self.take),
            "P_SKIP": str(skip),
            "P_SORT": "REGISTRATION_DATE_ASC",
        }

    async def start(self):
        yield JsonRequest(url=self.url, data=self._body(0), callback=self.parse, cb_kwargs={"skip": 0})
    def start_requests(self):
        yield JsonRequest(url=self.url, data=self._body(0), callback=self.parse, cb_kwargs={"skip": 0})
    def parse(self, response, skip):
        rows = (response.json().get("response") or {}).get("result") or []
        for r in rows:
            yield r
        if len(rows) == self.take:
            nxt = skip + self.take
            yield JsonRequest(url=self.url, data=self._body(nxt), callback=self.parse, cb_kwargs={"skip": nxt})

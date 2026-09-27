"""Silver contract layer for Dubai Ejari rent registrations.

Validates each registration, derives contract facts, and produces stable
keys. Never raises on a bad row: violations are collected on the instance so
the pipeline stays fail-open (ADR-03) and the 1-registration grain (ADR-02)
is preserved.

Units: ACTUAL_AREA is square FEET, confirmed by residential flats in the
400-1200 band landing at ~80.6 AED/sqft. Never name an area field *_sqm.

The model is frozen, so derived fields must be set with
object.__setattr__(self, ...) inside mode="after" validators, never self.x = ...,
which raises under frozen=True.
"""
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import hashlib
from typing import Optional

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from lib.config import VALIDATION_THRESHOLDS

SHORT_TERM_DAYS = 300
RECONCILE_TOLERANCE = Decimal("0.05")
# 200 sqft equals VALIDATION_THRESHOLDS["min_property_size"], deliberately not
# read from it: that is the validity range, this is the reporting floor below
# which a per-sqft figure is meaningless.
PSF_MIN_AREA_SQFT = 200
DAYS_PER_YEAR = Decimal("365.25")

# Upstream DLD emits word-length '?' masks for enum-lookup Arabic columns, not
# mojibake. The original bytes do not exist, so these are dropped, not repaired.
ARABIC_DROPPED: tuple[str, ...] = ("VERSION_AR", "IS_FREE_HOLD_AR", "MASTER_PROJECT_AR")

dropped_columns: dict[str, str] = {
    "TOTAL": "constant batch metadata (single distinct value), not per-row",
    "DEFAULT_SORT": "query echo, not data",
    "AREA_ID": "single distinct value 0 across all rows",
    "EJARI_PROPERTY_TYPE_ID": "single distinct value 0 across all rows",
    "EJARI_PROPERTY_SUB_TYPE_ID": "single distinct value 0 across all rows",
    "PROPERTY_USAGE_ID": "single distinct value 0 across all rows",
    "CONTRACT_NUMBER": "100% null in every daily file",
    "PROPERTY_ID": "single distinct value 0 across all rows",
    "PARCEL_ID": "100% null",
    "LAND_PROPERTY_ID": "constant 0",
    "VERSION_NUMBER": "constant 0",
    "VERSION_AR": "100% upstream '?' mask, unrecoverable",
    "IS_FREE_HOLD_AR": "100% upstream '?' mask, unrecoverable",
    "IS_FREE_HOLD_EN": "is_free_hold bool is authoritative (1:1 verified)",
    # MASTER_PROJECT_EN: the CSV column is 100% null, but the snake_case field
    # is still declared on the model because dim_project.sql:10 reads it.
    "MASTER_PROJECT_EN": "100% null upstream",
    "MASTER_PROJECT_AR": "100% null upstream",
}


class SilverRentContract(BaseModel):
    """One Ejari rent registration. Field names are the pipeline's canonical
    snake_case names so this drops into the existing pipeline with no rename."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    # location
    area_name_en: str
    area_name_ar: Optional[str] = None
    nearest_metro_en: Optional[str] = None
    nearest_mall_en: Optional[str] = None
    nearest_landmark_en: Optional[str] = None
    nearest_metro_ar: Optional[str] = None
    nearest_mall_ar: Optional[str] = None
    nearest_landmark_ar: Optional[str] = None

    # property
    property_usage_en: Optional[str] = None
    ejari_property_type_en: Optional[str] = None
    ejari_property_sub_type_en: Optional[str] = None
    property_type_ar: Optional[str] = None
    property_sub_type_ar: Optional[str] = None
    property_usage_ar: Optional[str] = None
    rooms: Optional[int] = None
    actual_area: Decimal
    has_parking: bool = False

    # project
    project_name_en: Optional[str] = None
    master_project_en: Optional[str] = None
    project_ar: Optional[str] = None

    # contract facts
    contract_registration_date: Optional[date] = None
    contract_start_date: Optional[date] = None
    contract_end_date: Optional[date] = None
    version_en: Optional[str] = None
    is_free_hold: bool = False
    # No pydantic bounds on the numeric fields. A bound like Field(ge=0) rejects
    # the value BEFORE mode="after" runs, so _derive_and_collect never sees it
    # and the precise violation name is lost — a -1 rent would be reported as a
    # generic schema error instead of annual_amount_not_positive. All range
    # checking lives in one place: _derive_and_collect.
    annual_amount: Decimal
    contract_amount: Optional[Decimal] = None
    total_properties: int = 1

    # derived — every one defaults None so construction precedes derivation
    duration_days: Optional[int] = None
    is_short_term: Optional[bool] = None
    monthly_rent: Optional[Decimal] = None
    rent_per_sqft: Optional[Decimal] = None
    implied_years: Optional[Decimal] = None
    psf_eligible: bool = False
    violations: list[str] = Field(default_factory=list)

    # keys — set by to_silver before construction, so None is never needed in
    # practice, but it keeps a direct construction from failing. The endpoint
    # exposes no contract number, so no candidate key is both stable and unique
    # and both are needed:
    #   row_hash  — STABLE. Built without RN, so it survives re-fetch and is
    #               safe for cross-day dedup. Not unique: genuinely identical
    #               registrations collapse, 4306 rows -> 3378 values.
    #   record_id — UNIQUE within a file. It appends RN, the gateway's
    #               per-response ordinal, so it reaches full cardinality but
    #               breaks if a re-fetch RENUMBERS RN (verified: 928 of 4306
    #               rows change id). Reordering rows alone is harmless, since
    #               RN travels with its row. Cross-day use must key on
    #               row_hash.
    row_hash: Optional[str] = None
    record_id: Optional[str] = None

    @model_validator(mode="after")
    def _derive_and_collect(self):
        """Derive contract facts and record every rule that fires. Never raises:
        a bad cell keeps its value and is named in `violations` so the row
        survives to Silver (ADR-03 fail-open, ADR-02 grain)."""
        v = self.violations
        set_ = object.__setattr__

        max_rent = Decimal(VALIDATION_THRESHOLDS["max_annual_rent"])
        min_rent = Decimal(VALIDATION_THRESHOLDS["min_annual_rent"])
        max_area = Decimal(VALIDATION_THRESHOLDS["max_property_size"])

        if self.annual_amount <= 0:
            v.append("annual_amount_not_positive")
        elif self.annual_amount < min_rent:
            v.append("annual_amount_below_min")
        if self.annual_amount > max_rent:
            v.append("annual_amount_above_max")

        if self.actual_area < 0:
            v.append("actual_area_negative")
        elif self.actual_area > max_area:
            v.append("actual_area_above_max")
        elif self.actual_area < PSF_MIN_AREA_SQFT:
            v.append("actual_area_below_psf_floor")
        else:
            set_(self, "psf_eligible", True)
            set_(
                self,
                "rent_per_sqft",
                (self.annual_amount / self.actual_area).quantize(Decimal("0.01")),
            )

        if self.annual_amount > 0:
            set_(
                self,
                "monthly_rent",
                (self.annual_amount / 12).quantize(Decimal("0.01")),
            )

        if self.contract_start_date and self.contract_end_date:
            if self.contract_end_date <= self.contract_start_date:
                v.append("end_before_start")
            else:
                days = (self.contract_end_date - self.contract_start_date).days
                set_(self, "duration_days", days)
                set_(self, "is_short_term", days < SHORT_TERM_DAYS)

        if self.contract_amount and self.annual_amount > 0:
            implied = self.contract_amount / self.annual_amount
            set_(self, "implied_years", implied.quantize(Decimal("0.0001")))
            if self.duration_days:
                declared = Decimal(self.duration_days) / DAYS_PER_YEAR
                if declared > 0 and abs(implied - declared) / declared > RECONCILE_TOLERANCE:
                    v.append("amount_duration_mismatch")

        for col in ("area_name_en", "ejari_property_type_en", "ejari_property_sub_type_en"):
            value = getattr(self, col)
            if isinstance(value, str) and value != value.strip():
                set_(self, col, value.strip())

        return self


ARABIC_PARTIAL = (
    "nearest_metro_ar",
    "nearest_mall_ar",
    "project_ar",
    "nearest_landmark_ar",
)


@dataclass
class SilverContractResult:
    frame: pl.DataFrame
    quarantined: pl.DataFrame
    groups: pl.DataFrame
    violation_counts: dict[str, int]

    def __len__(self) -> int:
        return self.frame.height


# model field -> source column. Canonical snake_case first (added by
# RentsTransformer's alias step), then the original UPPERCASE column.
_SOURCE_COLUMNS = {
    "total_properties": ("total_properties", "TOTAL_PROPERTIES"),
    "is_free_hold": ("is_free_hold", "IS_FREE_HOLD"),
    "version_en": ("version_en", "VERSION_EN"),
    "rooms": ("rooms", "ROOMS"),
    "has_parking": ("has_parking", "PARKING"),
    "area_name_ar": ("area_name_ar", "AREA_AR"),
    "property_usage_ar": ("property_usage_ar", "USAGE_AR"),
    "property_type_ar": ("property_type_ar", "PROP_TYPE_AR"),
    "property_sub_type_ar": ("property_sub_type_ar", "PROP_SUB_TYPE_AR"),
    "project_ar": ("project_ar", "PROJECT_AR"),
    "nearest_metro_en": ("nearest_metro_en", "NEAREST_METRO_EN"),
    "nearest_mall_en": ("nearest_mall_en", "NEAREST_MALL_EN"),
    "nearest_landmark_en": ("nearest_landmark_en", "NEAREST_LANDMARK_EN"),
    "nearest_metro_ar": ("nearest_metro_ar", "NEAREST_METRO_AR"),
    "nearest_mall_ar": ("nearest_mall_ar", "NEAREST_MALL_AR"),
    "nearest_landmark_ar": ("nearest_landmark_ar", "NEAREST_LANDMARK_AR"),
    "row_hash": ("row_hash",),
    "record_id": ("record_id",),
}
# canonical snake_case names that need no mapping
_MODEL_FIELDS = frozenset(SilverRentContract.model_fields)


def _project(source: dict) -> dict:
    """Map one raw row onto the model's field names."""
    out = {}
    for field in _MODEL_FIELDS:
        for candidate in _SOURCE_COLUMNS.get(field, (field,)):
            if candidate in source:
                out[field] = source[candidate]
                break
    return out


# Two columns need a polars-side cast before pydantic will accept them. The
# no-raise guarantee covers bad VALUES, not bad TYPES: pydantic rejects these
# outright, and the row would be quarantined rather than reach Silver.
#
# Each key is the SOURCE column, because the guard below tests `name in
# df.columns`. RentsTransformer renames only 13 columns, so keying by model
# field name silently skips any cast whose source kept its UPPERCASE name.
#
#   contract_registration_date: RentsTransformer emits Datetime and ALL 4306
#     stamps carry a non-midnight time (3337 distinct values, 00:03:07 to
#     23:57:06, none at 00:00:00), which Optional[date] rejects with
#     date_from_datetime_inexact. START_DATE/END_DATE need no cast: all 4306
#     are exactly midnight, which pydantic accepts as a date.
#   PARKING: Int64, null in 4220 of 4306 rows, and bool rejects None. Aliased
#     to has_parking so _project finds it under the model's field name.
_CASTS = {
    "contract_registration_date": pl.col("contract_registration_date").dt.date(),
    "PARKING": pl.col("PARKING").fill_null(False).cast(pl.Boolean).alias("has_parking"),
}


_ROW_HASH_FIELDS = (
    "area_name_en",
    "ejari_property_sub_type_en",
    "contract_start_date",
    "annual_amount",
    "actual_area",
)


def _row_hash(payload: dict) -> str:
    """Stable fingerprint for cross-day dedup, and the only key safe to use
    across days. Deliberately excludes RN: RN is a column that travels with its
    row, so reordering the payload cannot change this hash — but a renumbered RN
    (different P_SKIP/P_TAKE pagination) does change record_id, and this hash is
    what stays put when that happens. Not unique by design: it resolves 3378 of
    4306 rows, so it deduplicates, it does not identify."""
    parts = [str(payload.get(f) or "").strip() for f in _ROW_HASH_FIELDS]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]


def _is_masked(value) -> bool:
    """True for the upstream DLD word-mask: '??', '??????', '??? ??'.
    The original bytes do not exist, so these are unrecoverable."""
    return isinstance(value, str) and bool(value) and set(value) <= {"?", " "}


def to_silver(df: pl.DataFrame) -> SilverContractResult:
    """Validate every row. Never raises. Every input row lands in exactly one
    of two disjoint buckets, so `len(frame) + len(quarantined) == df.height`:

      frame       — constructed successfully. Keeps every value and carries its
                    violations in the per-row column, so it stays fail-open
                    (ADR-03).
      quarantined — the raw source row pydantic could not build. Retained, not
                    dropped, so a schema failure is auditable rather than a
                    silent hole in the grain (ADR-02).
    """
    casts = [expr for name, expr in _CASTS.items() if name in df.columns]
    if casts:
        df = df.with_columns(casts)

    records: list[dict] = []
    failed: list[dict] = []
    violations: dict[str, int] = {}

    for source in df.iter_rows(named=True):
        payload = _project(source)
        for col in ARABIC_PARTIAL:
            if _is_masked(payload.get(col)):
                payload[col] = None
                violations["masked_arabic_cell"] = violations.get("masked_arabic_cell", 0) + 1
        payload["row_hash"] = _row_hash(payload)
        payload["record_id"] = f"{payload['row_hash']}:{source.get('RN')}"
        try:
            contract = SilverRentContract(**payload)
        except ValidationError as exc:
            for err in exc.errors():
                rule = f"schema:{err['loc'][0] if err['loc'] else 'row'}"
                violations[rule] = violations.get(rule, 0) + 1
            failed.append(source)
            continue
        for rule in contract.violations:
            violations[rule] = violations.get(rule, 0) + 1
        records.append(contract.model_dump())

    return SilverContractResult(
        frame=pl.DataFrame(records) if records else df.clear(),
        quarantined=pl.DataFrame(failed) if failed else df.clear(),
        groups=pl.DataFrame(),
        violation_counts=violations,
    )


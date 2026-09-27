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
from datetime import date, datetime
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

# These three are dropped for TWO DIFFERENT REASONS. Do not read this as one.
#
# VERSION_AR and IS_FREE_HOLD_AR are 100% '?'-masked upstream: 16075 of 16075
# rows non-empty across the five daily files, every non-space character a '?'.
# The original bytes were destroyed by DLD before the payload was written, so
# the text is unrecoverable and no repair could recover it.
#
# MASTER_PROJECT_AR is NOT masked. It is near-empty, not corrupted: 4 of 16075
# rows carry real, unmasked Arabic ('جنات ' x2, 'هيلز بارك', 'رمرام - الرمث'),
# split 1 on 20260913 / 3 on 20260916 — the same split as MASTER_PROJECT_EN.
# Dropping it is therefore a DECISION, not a forced loss: 4 genuine values are
# discarded on purpose because nothing downstream reads an Arabic master-project
# name. If that ever changes, this drop is what to challenge first — the data
# was never missing, only unwanted. It stays in the tuple so the tuple and
# dropped_columns keep agreeing (asserted in tests), and the design spec lists
# all three as deliberately dropped.
ARABIC_DROPPED: tuple[str, ...] = ("VERSION_AR", "IS_FREE_HOLD_AR", "MASTER_PROJECT_AR")

dropped_columns: dict[str, str] = {
    # TOTAL is constant WITHIN a daily file and differs BETWEEN files: 5
    # distinct values over the 5 files (1714/886/4589/4580/4306), each equal
    # to that file's row count. "Single distinct value" is false across files.
    "TOTAL": "per-response batch count, constant within a file, not per-row",
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
    # MASTER_PROJECT_EN is NOT in this dict, and that is the point. It is
    # near-total null upstream but NOT 100% — 3 of 4580 rows on 20260916 and 1 of
    # 1714 on 20260913 carry a value — so it is a carried column, not a dropped
    # one, and listing it here would tell a reader of the Silver parquet it had
    # been removed when it is present in every frame. See the model field.
    #
    # MASTER_PROJECT_AR: near-total null upstream, but NOT 100% — 4 non-empty
    # of 16075 rows across the five daily files: 'جنات ' (x2), 'هيلز بارك',
    # 'رمرام - الرمث'. Same 1 on 20260913 / 3 on 20260916 split as EN. Unlike
    # VERSION_AR and IS_FREE_HOLD_AR this is NOT a '?' mask, so it is listed in
    # ARABIC_DROPPED as near-empty, not as unrecoverable text.
    "MASTER_PROJECT_AR": "near-total null upstream, never 100%",
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
    # Carried despite being near-total null upstream (3 of 4580 rows on
    # 20260916, 1 of 1714 on 20260913): dim_project.sql:10 reads it, and those
    # late non-null values are exactly what broke pl.DataFrame's 100-row schema
    # inference — see to_silver. Declared Optional, which is what makes the
    # leading null rows survivable instead of quarantined.
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
    #               breaks if a re-fetch RENUMBERS RN (verified: all 4306 of
    #               4306 rows change id). Reordering rows alone is harmless,
    #               since RN travels with its row. With no RN column at all
    #               every id ends in ':None' and collides by design, which is
    #               correct: row_hash is the fallback for those. Cross-day use
    #               must key on row_hash.
    row_hash: Optional[str] = None
    record_id: Optional[str] = None

    @model_validator(mode="after")
    def _derive_and_collect(self):
        """Derive contract facts and record every rule that fires. Never raises:
        a bad cell keeps its value and is named in `violations` so the row
        survives to Silver (ADR-03 fail-open, ADR-02 grain)."""
        # Copy, append into the copy, then write the copy back. Appending
        # straight into self.violations works today only because pydantic hands
        # the model a fresh list; relying on that is how a future change to
        # _project turns every violation count into a silent zero, because the
        # appends would land in a local and never reach the field.
        v = list(self.violations)
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

        # `is not None`, never truthiness: Decimal("0") is falsy, and a zero
        # contract amount against a non-zero annual amount is exactly the
        # disagreement this rule exists to catch — it is the case most likely
        # to be real (a rent paid as an upfront nil-consideration registration).
        if self.contract_amount is not None and self.annual_amount > 0:
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

        set_(self, "violations", v)
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

# `violations` is a model field with no _SOURCE_COLUMNS entry, so the fallback in
# _project would copy it out of ANY frame that carries the column — including the
# frame to_silver itself just produced. Every rule then re-fires and re-appends
# its own name, so a second pass double-counts (4122 -> 8244 on the real
# rent_contracts_20260916.csv). to_silver is the documented public API and
# transform_rents overwrites the parquet with its output, so the second pass is
# reachable, not hypothetical. The other derived fields are harmless to re-project
# because set_ overwrites them; violations is the only one that ACCUMULATES.
_NEVER_SOURCED = frozenset({"violations"})


def _project(source: dict) -> dict:
    """Map one raw row onto the model's field names."""
    out = {}
    for field in _MODEL_FIELDS - _NEVER_SOURCED:
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


def _hash_part(value) -> str:
    """Canonical string for one hash field.

    None is absent. Numbers go through Decimal so Decimal('225000'), 225000.0
    and 225000 all produce '225000' — production supplies Float64, so without
    this the same contract hashes differently depending on the caller's numeric
    type, which would break cross-day dedup. 0 and 0.0 are distinct from absent:
    `or ''` would fold them together.

    bool is tested before the numeric branch because it subclasses int, and
    Decimal(str(True)) raises InvalidOperation. That would escape to_silver's
    try block, which catches only ValidationError, and break the never-raises
    contract. datetime is tested before date because it subclasses date: the
    model coerces both to Optional[date], so one contract must not hash two ways.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (int, float, Decimal)):
        return format(Decimal(str(value)).normalize(), "f")
    return str(value).strip()


def _row_hash(payload: dict) -> str:
    """Stable fingerprint for cross-day dedup, and the only key safe to use
    across days. Deliberately excludes RN: RN is a column that travels with its
    row, so reordering the payload cannot change this hash — but a renumbered RN
    (different P_SKIP/P_TAKE pagination) does change record_id, and this hash is
    what stays put when that happens. Not unique by design: it resolves 3378 of
    4306 rows, so it deduplicates, it does not identify."""
    parts = [_hash_part(payload.get(f)) for f in _ROW_HASH_FIELDS]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]


def _is_masked(value) -> bool:
    """True for the upstream DLD word-mask: '??', '??????', '??? ??'.
    The original bytes do not exist, so these are unrecoverable."""
    return isinstance(value, str) and bool(value) and set(value) <= {"?", " "}


@dataclass
class SilverContractGroup:
    """One reconstructed multi-property contract.

    `annual_amount` is CONTRACT-LEVEL and repeated on every member row, so it
    is deduplicated — never summed. `total_area_sqft` IS per-property, so it is
    summed. Getting this backwards overstates an 87-property block 87x.
    """

    group_id: str
    start_date: Optional[date]
    end_date: Optional[date]
    version_en: Optional[str]
    # DEDUPLICATED, NEVER SUMMED. This is the contract's total rent for the
    # whole term, repeated verbatim on each of its property rows. It is NOT a
    # per-property figure, so summing it over n properties overstates the money
    # n-fold: the real 87-property block is AED 3,053,700, and sum() reports
    # AED 265,671,900. `total_area_sqft` below is the opposite case and IS
    # summed. If you are editing the aggregation, this is the line that must not
    # become `block["annual_amount"].sum()`.
    annual_amount: Decimal
    total_properties: int
    observed_property_count: int
    # SUMMED. Per-property, like any other row-level measure.
    total_area_sqft: Decimal
    record_ids: list[str]
    usages: list[str]
    is_complete: bool


# annual_amount is a key component, so a group cannot span two amounts: dedup is
# structural here, not a runtime check that could be forgotten.
_GROUP_KEY = ("contract_start_date", "contract_end_date", "annual_amount", "version_en")


def _rollup(frame: pl.DataFrame, out_violations: dict[str, int]) -> pl.DataFrame:
    """Reconstruct contract blocks for rows declaring more than one property.
    CONTRACT_NUMBER is 100% null upstream, so the key is the best available
    proxy: it reconstructs 82 of 84 declared blocks exactly on the real
    payload. The 2 failures are two contracts sharing dates and amount, which
    is irreducibly ambiguous without a contract number.

    Adds to `out_violations` in place, so both ways a block can fail to
    reconstruct are visible in violation_counts and not only as is_complete=False
    on the group. They are DIFFERENT signals and are tallied separately, because
    conflating them makes the merged-key problem unsizable:

      observed > declared — the key merged two genuinely distinct contracts.
                            Irreducible without a contract number. Counted under
                            `merged_contract_group`.
      observed < declared — the window captured only part of the contract. A
                            capture problem, not a key problem. Counted under
                            `partial_contract_capture`.

    Rows in either case are flagged, never dropped."""
    if frame.height == 0 or "total_properties" not in frame.columns:
        return pl.DataFrame()

    multi = frame.filter(pl.col("total_properties") > 1)
    if multi.height == 0:
        return pl.DataFrame()

    rows = []
    for key, block in multi.group_by(list(_GROUP_KEY), maintain_order=True):
        # `[0]` is a CHOICE, not an invariant. Members of a block can disagree
        # about total_properties: 2 of the 259 real groups do — 18 rows split
        # {4, 14} and 11 rows split {3, 5} — so "they never disagree" is false.
        # Both still classify the same under any read (observed is below every
        # declared value, hence merged_contract_group either way), which is why
        # swapping [0] for [-1] changes no count today. Do not harden this into
        # an assumption without a tie-break rule; if a block is ever observed
        # ABOVE one member's declaration, this silently misclassifies it.
        declared = block["total_properties"][0]
        observed = block.height
        # annual_amount is a _GROUP_KEY member, so it CANNOT disagree inside a
        # group. Do not add a len(amounts) != 1 branch: it is unreachable, and
        # the two ways a block can mismatch are counted separately below.
        if observed > declared:
            # the key merged two distinct contracts: irreducibly ambiguous
            # without a contract number
            out_violations["merged_contract_group"] = out_violations.get(
                "merged_contract_group", 0
            ) + observed
        elif observed < declared:
            # the window captured only part of the contract
            out_violations["partial_contract_capture"] = out_violations.get(
                "partial_contract_capture", 0
            ) + observed
        rows.append(
            {
                "group_id": hashlib.sha256(
                    "|".join(str(k) for k in key).encode("utf-8")
                ).hexdigest()[:32],
                "start_date": key[0],
                "end_date": key[1],
                # CONTRACT-LEVEL, repeated per member row. Deduplicated, never
                # summed: the 87-property block carries one value, and summing
                # it would report AED 265,671,900 instead of AED 3,053,700.
                "annual_amount": block["annual_amount"][0],
                "version_en": key[3],
                "total_properties": declared,
                "observed_property_count": observed,
                "total_area_sqft": block["actual_area"].sum(),
                "record_ids": block["record_id"].to_list(),
                # a set of categories, not one entry per property row
                "usages": sorted(
                    {u for u in block["property_usage_en"].to_list() if u is not None}
                ),
                "is_complete": observed == declared,
            }
        )
    return pl.DataFrame(rows)


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

    # infer_schema_length=None is REQUIRED on both constructions below. polars
    # infers the schema from only the first 100 rows by default, so a field that
    # is null there and a string later raises ComputeError. Not hypothetical: it
    # fires on output/rent_contracts_20260916.csv at row 101 ("Hills Park"),
    # while the other four daily files pass, so the ETL would crash on that day.
    # The df.clear() fallbacks need nothing: an empty frame already carries the
    # source schema and infers nothing.
    frame = pl.DataFrame(records, infer_schema_length=None) if records else df.clear()
    return SilverContractResult(
        frame=frame,
        # same reason as above: `failed` is raw source rows, and a raw source
        # field is exactly the kind of late-populating column that trips this.
        quarantined=pl.DataFrame(failed, infer_schema_length=None)
        if failed
        else df.clear(),
        groups=_rollup(frame, violations),  # mutates violations in place
        violation_counts=violations,
    )


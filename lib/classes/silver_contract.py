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
from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

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


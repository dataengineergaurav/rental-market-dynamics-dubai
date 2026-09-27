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

from pydantic import BaseModel, ConfigDict

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

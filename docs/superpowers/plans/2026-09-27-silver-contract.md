# Silver Contract Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the broken `BronzeRentContract` with a constructible, no-raise `SilverRentContract` that validates every Ejari row, produces stable keys, rolls up multi-property contracts, and is wired into the ETL.

**Architecture:** A new `lib/classes/silver_contract.py` owns everything Bronze-and-per-row: typing, coercion, derivation, violation collection, keys, and the contract rollup. It never raises — bad cells become null and a `violations` list names the rules that fired, preserving ADR-03 fail-open and ADR-02's 1-registration grain. `lib/classes/validators.py` reverts to Silver-aggregate checks only. The model's field names are the pipeline's existing canonical snake_case names, so no rename layer is needed and no downstream consumer breaks.

**Tech Stack:** Python 3.12 · polars · pydantic v2 · pytest · duckdb (unchanged — no new dependency beyond the pydantic already in `pyproject.toml:25`)

**Spec:** `docs/superpowers/specs/2026-09-27-silver-contract-design.md`

## Global Constraints

- **Field naming:** use the pipeline's canonical snake_case names (`area_name_en`, `annual_amount`, `actual_area`, `contract_start_date`, `property_usage_en`, `project_name_en`, `master_project_en`, `ejari_property_type_en`, `ejari_property_sub_type_en`, `contract_registration_date`, `contract_amount`). Never `area_en` or `property_type`. These 11 names are referenced across `lib/analysis/*.sql` and consumed by `lib/classes/property_usage.py` and `lib/classes/market_analytics.py`. **All 11 must be declared as model fields** — a name present in `_row()` but absent from the model is silently discarded by `extra="ignore"` and surfaces later as an `AttributeError` far from the omission. Verified load-bearing sites: `master_project_en` at `ddl.sql:41`, `dim_project.sql:10,18,25`, `build_weekly_duckdb.py:100`, and `lib/config.py:23` (`DLD_SCHEMA`).
- **Units:** `ACTUAL_AREA` is **square feet**. Name every area and per-area field `*_sqft` / `*_per_sqft`. The strings `_sqm` and `rent_per_sqm` must not appear anywhere in `lib/`, `tests/`, or `lib/analysis/*.sql`.
- **Never raise on a bad row.** Collect violations instead. This is ADR-03 (`docs/IMPLEMENTATION_PLAN.md:15`).
- **Never drop a row.** Quarantine means "retained and flagged", not removed. ADR-02 grain is 1 row per registration.
- **Thresholds come from `lib/config.py:86 VALIDATION_THRESHOLDS`** — never hardcode a bound in more than one module.
- **No new dependencies.** pydantic v2 is already in `pyproject.toml:25`. ADR-06 in `docs/IMPLEMENTATION_PLAN.md:11` documents it.
- **No leading-underscore pydantic field names.** Pydantic v2 raises `NameError` at class-definition time; this is the exact bug that broke the module.
- **Derived fields must default to `None`.** A derived field declared without a default is *required*, so `mode="after"` validators never run and the model cannot be constructed.
- **Never feed `to_silver` the raw Bronze CSV.** It expects the frame `RentsTransformer` produces.
  The raw CSV has none of the 13 snake_case aliases, no parsed dates, and no `schema_overrides`
  dtypes, so `_project` finds almost nothing and `ValidationError` quarantines every row — 0
  retained. Verify through `RentsTransformer(csv, parquet).transform()` → `pl.read_parquet` →
  `to_silver`. Several plan verification commands still show the raw-CSV form; ignore them and use
  the transformer path.
- **Test command:** `uv run pytest -q` (or `.venv/bin/python -m pytest -q`). `make test` runs `pytest .`.
- **polars `Int8` temporal overflow trap.** `dt.hour()`, `dt.minute()`, `dt.second()` and friends
  return **Int8**. Arithmetic on them overflows silently: `dt.hour() * 3600` for hour=1 yields 16,
  not 3600. Any expression combining a temporal accessor with a multiplier can produce garbage with
  no error. Cast to Int64 first, or avoid the arithmetic. This produced a wrong "748 non-midnight
  stamps" figure that survived into a code comment before being caught.
- **Reference data:** `output/rent_contracts_20260917.csv` — 4306 rows × 44 columns, valid UTF-8, `CONTRACT_NUMBER` 100% null, `PROPERTY_ID` constant 0, median `ACTUAL_AREA` 75.0, 443/4306 (10.3%) rows PSF-eligible (446 clear the 200 floor, but 3 of those exceed the 50000 max and route to actual_area_above_max).

---

### Task 1: Unbreak the module and add an import smoke test

The suite is currently red — `lib/classes/validators.py` raises `NameError` at import, so `pytest` cannot collect `test_etl_pipeline.py` or `test_p0_gates.py`. Nothing else can be verified until this lands.

**Files:**
- Modify: `lib/classes/validators.py:8-73` (remove the pydantic import and the `BronzeRentContract` class)
- Create: `tests/test_silver_contract.py`

**Interfaces:**
- Consumes: nothing
- Produces: `lib.classes.validators` is importable again. `tests/test_silver_contract.py` exists as the home for every later Silver test.

- [ ] **Step 1: Write the failing import test**

Create `tests/test_silver_contract.py`:

```python
import importlib


def test_silver_contract_module_imports():
    """Guards the bug class that broke validators.py: a module that cannot be
    imported fails silently inside run_etl_pipeline.py's blanket except."""
    importlib.import_module("lib.classes.silver_contract")


def test_validators_module_imports():
    importlib.import_module("lib.classes.validators")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'lib.classes.silver_contract'`

- [ ] **Step 3: Remove the broken model from validators.py**

In `lib/classes/validators.py`, delete the `from pydantic import BaseModel, Field` line and the entire `BronzeRentContract` class body (from `class BronzeRentContract(BaseModel):` through the closing `model_config = {"extra": "allow"}` line). Leave `logger = logging.getLogger(__name__)` and everything from `class ValidationResult:` onward untouched. Also delete the two stray blank lines that followed the class.

- [ ] **Step 4: Create the module Task 1's test imports**

Create `lib/classes/silver_contract.py` with only this content — the real model lands in Task 2:

```python
"""Silver contract layer for Dubai Ejari rent registrations.

Validates each registration, derives contract facts, and produces stable
keys. Never raises on a bad row: violations are collected on the instance so
the pipeline stays fail-open (ADR-03) and the 1-registration grain (ADR-02)
is preserved.
"""
```

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest -q`
Expected: PASS — collection succeeds, no errors. `tests/test_p0_gates.py` and `tests/test_etl_pipeline.py` now import cleanly.

- [ ] **Step 6: Commit**

```bash
git add lib/classes/validators.py lib/classes/silver_contract.py tests/test_silver_contract.py
git commit -m "fix: remove BronzeRentContract that broke validators.py import

Pydantic v2 rejects leading-underscore field names, so the class raised
NameError at definition time. pytest could not collect two test files, and
run_etl_pipeline.py's blanket except swallowed the ImportError so the P0.3
validation gate logged as passing while never executing.

Adds an import smoke test so this failure mode can never again be silent."
```

---

### Task 2: A constructible `SilverRentContract`

**Files:**
- Modify: `lib/classes/silver_contract.py`
- Modify: `tests/test_silver_contract.py`

**Interfaces:**
- Consumes: `lib.config.VALIDATION_THRESHOLDS`
- Produces: `SilverRentContract` — a frozen, `extra="ignore"` pydantic model constructible from one Silver DataFrame row. Fields: `area_name_en: str`, `annual_amount: Decimal`, `actual_area: Decimal`, `contract_start_date: date`, `contract_end_date: date`, `is_free_hold: bool`, `total_properties: int`, plus optional location/project/usage fields. Module constants `ARABIC_DROPPED: tuple[str, ...]` and `dropped_columns: dict[str, str]`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_silver_contract.py`:

```python
from datetime import date
from decimal import Decimal


def _row(**over):
    """A minimal valid Silver row, matching the canonical column names."""
    row = {
        "area_name_en": "Dubai Marina",
        "area_name_ar": "دبي مارينا",
        "property_usage_en": "Residential",
        "ejari_property_type_en": "Unit",
        "ejari_property_sub_type_en": "Flat",
        "project_name_en": "Ocean Heights",
        "master_project_en": None,
        "nearest_metro_en": "DMCC Metro Station",
        "nearest_mall_en": "Dubai Marina Mall",
        "nearest_landmark_en": None,
        "area_name_ar2": None,
        "property_type_ar": "وحدة",
        "property_sub_type_ar": "شقه",
        "property_usage_ar": "سكني",
        "project_ar": "أوشن هايتس",
        "nearest_metro_ar": "محطة مترو",
        "nearest_mall_ar": "دبي مارينا مول",
        "nearest_landmark_ar": None,
        "contract_registration_date": date(2026, 9, 12),
        "contract_start_date": date(2026, 9, 20),
        "contract_end_date": date(2027, 9, 19),
        "version_en": "New",
        "is_free_hold": True,
        "annual_amount": Decimal("225000"),
        "contract_amount": Decimal("225000"),
        "actual_area": Decimal("1450"),
        "rooms": 2,
        "has_parking": True,
        "total_properties": 1,
    }
    row.pop("area_name_ar2")
    row.update(over)
    return row


def test_model_constructs_from_a_silver_row():
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(**_row())
    assert c.area_name_en == "Dubai Marina"
    assert c.annual_amount == Decimal("225000")
    assert c.is_free_hold is True


def test_model_forbids_leading_underscore_fields():
    """The exact bug that broke validators.py. Pydantic raises NameError at
    class-definition time, so a class-body typo takes the whole module down."""
    from pydantic import BaseModel

    with pytest.raises(NameError, match="leading underscores"):

        class Bad(BaseModel):
            _private: str = "x"


def test_area_is_square_feet_not_square_metres():
    """NOT IN THIS TASK. `rent_per_sqft` and `psf_eligible` are Task 3's derived
    fields, so the m2/sqft guard cannot be asserted until they exist. The test
    lives in Task 3 (`test_rent_per_sqft_lands_in_the_dubai_market_band`),
    where the division that would carry the 10.76x error actually happens."""
```

Add `import pytest` to the top of the test file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: FAIL — `ImportError: cannot import name 'SilverRentContract'`

- [ ] **Step 3: Write the model**

Replace the contents of `lib/classes/silver_contract.py` with:

```python
"""Silver contract layer for Dubai Ejari rent registrations.

Validates each registration, derives contract facts, and produces stable
keys. Never raises on a bad row: violations are collected on the instance so
the pipeline stays fail-open (ADR-03) and the 1-registration grain (ADR-02)
is preserved.

Units: ACTUAL_AREA is square FEET, confirmed by residential flats in the
400-1200 band landing at ~80.6 AED/sqft. Never name an area field *_sqm.
"""
from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

# Upstream DLD emits word-length '?' masks for enum-lookup Arabic columns, not
# mojibake. The original bytes do not exist, so these are dropped, not repaired.
ARABIC_DROPPED = ("VERSION_AR", "IS_FREE_HOLD_AR", "MASTER_PROJECT_AR")

dropped_columns = {
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
```

`actual_area` is declared once, in the `# property` block above. Do not repeat it here — a second
declaration is silently overridden by pydantic and reads as a copy-paste slip.

**Do not add `ge=`, `gt=`, `lt=` or `le=` to any numeric field on this model.** The violation
names in `_derive_and_collect` are the contract's diagnostics; a pydantic bound short-circuits them
into an opaque schema error and makes the `actual_area_negative` and `annual_amount_not_positive`
branches unreachable.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: PASS — 5 tests

- [ ] **Step 5: Commit**

```bash
git add lib/classes/silver_contract.py tests/test_silver_contract.py
git commit -m "feat: constructible SilverRentContract with canonical column names

Uses the pipeline's existing snake_case names (area_name_en, annual_amount,
actual_area) so no rename layer is needed and the 11 names referenced by
lib/analysis/*.sql keep working. Area is square feet, guarded by a test that
would catch the m2/sqft confusion. Records the 16 dropped columns and why."
```

---

### Task 3: Derived fields and no-raise violation collection

**Files:**
- Modify: `lib/classes/silver_contract.py`
- Modify: `tests/test_silver_contract.py`

**Interfaces:**
- Consumes: `SilverRentContract` (Task 2), `lib.config.VALIDATION_THRESHOLDS`
- Produces: on `SilverRentContract` — `duration_days: Optional[int]`, `is_short_term: Optional[bool]`, `monthly_rent: Optional[Decimal]`, `rent_per_sqft: Optional[Decimal]`, `implied_years: Optional[Decimal]`, `psf_eligible: bool`, `violations: list[str]`. All derived fields default `None` so construction succeeds before the `mode="after"` validator runs.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_silver_contract.py`:

```python
def test_derives_duration_and_short_term_flag():
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(**_row())
    assert c.duration_days == 364
    assert c.is_short_term is False
    assert c.monthly_rent == Decimal("18750.00")


def test_short_term_flag_fires_under_300_days():
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(
        **_row(
            contract_start_date=date(2026, 9, 20),
            contract_end_date=date(2026, 11, 20),
        )
    )
    assert c.duration_days == 61
    assert c.is_short_term is True


def test_derived_fields_default_none_so_construction_never_fails():
    """A derived field declared without a default is REQUIRED in pydantic v2,
    so mode='after' never runs. This asserts the trap stays closed."""
    from lib.classes.silver_contract import SilverRentContract

    for name in (
        "duration_days",
        "is_short_term",
        "monthly_rent",
        "rent_per_sqft",
        "implied_years",
    ):
        assert name in SilverRentContract.model_fields
        assert not SilverRentContract.model_fields[name].is_required(), name


def test_bad_row_collects_violations_instead_of_raising():
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(
        **_row(
            annual_amount=Decimal("6650000"),
            actual_area=Decimal("882158"),
            contract_start_date=date(2027, 1, 1),
            contract_end_date=date(2026, 1, 1),
        )
    )
    joined = " | ".join(c.violations)
    assert "annual_amount_above_max" in joined
    assert "actual_area_above_max" in joined
    assert "end_before_start" in joined
    assert c.actual_area == Decimal("882158"), "row is retained, not dropped"


def test_amount_mismatch_is_a_reconciliation_violation():
    """contract_amount/annual_amount == duration in years (corr 0.9956 on the
    real payload). Disagreement >5% means one of the two is wrong."""
    from lib.classes.silver_contract import SilverRentContract

    good = SilverRentContract(**_row())
    assert not [v for v in good.violations if v == "amount_duration_mismatch"]

    bad = SilverRentContract(**_row(contract_amount=Decimal("900000")))
    assert "amount_duration_mismatch" in bad.violations


def test_no_psf_below_200_sqft():
    from lib.classes.silver_contract import SilverRentContract

    assert SilverRentContract(**_row(actual_area=Decimal("199"))).rent_per_sqft is None
    assert SilverRentContract(**_row(actual_area=Decimal("1000"))).psf_eligible is True


def test_rent_per_sqft_lands_in_the_dubai_market_band():
    """Guards the 10.76x m2/sqft corruption, at the point the division happens.
    Spec gate 5: residential flats in the 400-1200 sqft band must land at
    60-120 AED/sqft. Read as m2 the same figures become 868 AED/sqft, roughly
    10x market. Moved here from Task 2, where rent_per_sqft did not yet exist."""
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(
        **_row(annual_amount=Decimal("60000"), actual_area=Decimal("744"))
    )
    assert c.psf_eligible is True
    assert Decimal("60") <= c.rent_per_sqft <= Decimal("120"), (
        f"rent_per_sqft {c.rent_per_sqft} outside 60-120; the unit is not sqft"
    )
    assert c.rent_per_sqft * Decimal("10.7639") > Decimal("800")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: FAIL — no `duration_days` attribute

- [ ] **Step 3: Add the derived fields and validators**

In `lib/classes/silver_contract.py`, add `SHORT_TERM_DAYS = 300` and `RECONCILE_TOLERANCE = Decimal("0.05")` at module level, add the six derived fields to the model (all defaulting to `None`, `violations` using `Field(default_factory=list)`), and append the validators:

```python
SHORT_TERM_DAYS = 300
RECONCILE_TOLERANCE = Decimal("0.05")
PSF_MIN_AREA_SQFT = 200
DAYS_PER_YEAR = Decimal("365.25")
```

Fields to add inside `SilverRentContract`, after `total_properties`:

```python
    # derived — every one defaults None so construction precedes derivation
    duration_days: Optional[int] = None
    is_short_term: Optional[bool] = None
    monthly_rent: Optional[Decimal] = None
    rent_per_sqft: Optional[Decimal] = None
    implied_years: Optional[Decimal] = None
    psf_eligible: bool = False
    violations: list[str] = Field(default_factory=list)
```

Then add these imports to the top of the file:

```python
from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.config import VALIDATION_THRESHOLDS
```

And append after the class:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: PASS — 14 tests in the file (2 smoke + 5 from Task 2 + 7 added here)

- [ ] **Step 5: Verify the real payload's violation counts (spec gate 7)**

`output/` is gitignored so this is a local check, not a CI gate. Expected values were measured on
`output/rent_contracts_20260917.csv`:

Run: `uv run python -c "import polars as pl; from lib.classes.silver_contract import to_silver; d=pl.read_csv('output/rent_contracts_20260917.csv', null_values=['null','NULL',''], ignore_errors=True, schema_overrides={'ANNUAL_AMOUNT':pl.Float64,'ACTUAL_AREA':pl.Float64}); d=d.rename({'AREA_EN':'area_name_en','PROP_SUB_TYPE_EN':'ejari_property_sub_type_en','START_DATE':'contract_start_date','END_DATE':'contract_end_date','USAGE_EN':'property_usage_en','VERSION_EN':'version_en'}); r=to_silver(d); print('rows',len(r),'/',d.height); print('psf_eligible',r.frame['psf_eligible'].sum(),'(want 443)'); v=r.violation_counts; print('below_psf_floor',v.get('actual_area_below_psf_floor'),'(want 3860)'); print('amount_below_min',v.get('annual_amount_below_min'),'(want 9)'); print('amount_above_max',v.get('annual_amount_above_max'),'(want 1)'); print('area_above_max',v.get('actual_area_above_max'),'(want 3)'); print('amt_dur_mismatch',v.get('amount_duration_mismatch'),'(want 85, 1.97%)')"`
Expected: `rows 4306/4306`, `psf_eligible 443`, `3860`, `9`, `1`, `3`, `85`

- [ ] **Step 6: Commit**

```bash
git add lib/classes/silver_contract.py tests/test_silver_contract.py
git commit -m "feat: derive contract facts and collect violations without raising

duration_days, is_short_term, monthly_rent, rent_per_sqft, implied_years all
default None so construction precedes derivation. amount_duration_mismatch
turns the gateway's two independent measurements of contract length into a
free integrity check: contract_amount/annual_amount agrees with
duration/365.25 to 0.0065 years on the real payload."
```

---

### Task 4: `to_silver()` — DataFrame in, validated result out

**Files:**
- Modify: `lib/classes/silver_contract.py`
- Modify: `tests/test_silver_contract.py`

**Interfaces:**
- Consumes: `SilverRentContract` (Task 3)
- Produces:
  - `to_silver(df: pl.DataFrame) -> SilverContractResult`
  - `SilverContractResult` dataclass with `frame: pl.DataFrame`, `quarantined: pl.DataFrame`, `groups: pl.DataFrame`, `violation_counts: dict[str, int]`, and `__len__` returning `len(self.frame)`.
  - Module constant `ARABIC_PARTIAL` — the four partially-masked columns whose masked cells are nulled.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_silver_contract.py`:

```python
def _frame(n=3, **over):
    import polars as pl

    rows = [_row(**over) for _ in range(n)]
    return pl.DataFrame(rows)


def test_to_silver_returns_one_row_per_registration():
    from lib.classes.silver_contract import to_silver

    res = to_silver(_frame(3))
    assert len(res.frame) == 3
    assert res.frame["area_name_en"].to_list() == ["Dubai Marina"] * 3


def test_to_silver_never_raises_on_garbage():
    import polars as pl
    from lib.classes.silver_contract import to_silver

    bad = pl.DataFrame(
        {
            "area_name_en": ["", "Dubai Marina", None],
            "annual_amount": [-5.0, 70000.0, 0.0],
            "actual_area": [1.0, 900.0, -3.0],
            "contract_start_date": [None] * 3,
            "contract_end_date": [None] * 3,
        }
    )
    res = to_silver(bad)
    # the null area_name_en row cannot construct, so it is quarantined, not
    # discarded. frame and quarantined are disjoint and together cover the input.
    assert len(res.frame) == 2
    assert len(res.quarantined) == 1
    assert len(res.frame) + len(res.quarantined) == bad.height
    assert res.violation_counts


def test_to_silver_nulls_masked_arabic_cells():
    import polars as pl
    from lib.classes.silver_contract import to_silver

    df = _frame(1, nearest_metro_ar="??", area_name_ar="برج خليفة")
    res = to_silver(df)
    assert res.frame["nearest_metro_ar"][0] is None, "masked cell becomes null"
    assert res.frame["area_name_ar"][0] == "برج خليفة", "intact Arabic survives"
    assert res.violation_counts.get("masked_arabic_cell", 0) == 1


def test_to_silver_drops_unrecoverable_arabic_columns():
    import polars as pl
    from lib.classes.silver_contract import ARABIC_DROPPED, to_silver

    df = _frame(1)
    df = df.with_columns(pl.lit("??").alias("VERSION_AR"), pl.lit("??").alias("IS_FREE_HOLD_AR"))
    res = to_silver(df)
    for col in ARABIC_DROPPED:
        assert col not in res.frame.columns
    assert "VERSION_AR" in dropped_columns
```

Add `from lib.classes.silver_contract import dropped_columns` to the imports in the last test.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: FAIL — `ImportError: cannot import name 'to_silver'`

- [ ] **Step 3: Implement `to_silver`**

Add to `lib/classes/silver_contract.py`:

```python
ARABIC_PARTIAL = (
    "nearest_metro_ar",
    "nearest_mall_ar",
    "project_ar",
    "nearest_landmark_ar",
)
```

```python
@dataclass
class SilverContractResult:
    frame: pl.DataFrame
    quarantined: pl.DataFrame
    groups: pl.DataFrame
    violation_counts: dict[str, int]

    def __len__(self) -> int:
        return self.frame.height
```

**The source-column map is not optional.** `RentsTransformer` aliases only 13 columns to
snake_case. The other 16 model fields exist only under their original UPPERCASE names — verified
against a real transformer output (57 columns), where `total_properties`, `is_free_hold`,
`version_en`, `rooms`, `has_parking` and all eight `*_ar` fields have no snake_case source. A plain
`if k in _MODEL_FIELDS` filter therefore silently defaults every one of them, which makes
`total_properties` always 1 and kills the Task 6 rollup outright.

```python
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
# outright, and every row in the real payload would be dropped.
#
#   contract_registration_date: the CSV carries '2026-09-16T00:03:07', and
#     Optional[date] raises date_from_datetime_inexact on a datetime string.
#   has_parking: PARKING is Int64 and null in 4220 of 4306 rows, and bool
#     rejects None (int 0/1 is fine).
#
# KEYED BY SOURCE COLUMN, not model field name. The guard below is
# `if name in df.columns`, and df carries the transformer's column names — the
# canonical aliases plus the original UPPERCASE leftovers. `has_parking` is
# never a column (its source is `PARKING`), so keying by model name makes the
# cast silently skip and 4251 rows drop. Same bug class as _SOURCE_COLUMNS,
# one layer up: each expression must alias itself to the model field name.
_CASTS = {
    "contract_registration_date": pl.col("contract_registration_date").dt.date(),
    "PARKING": pl.col("PARKING").fill_null(False).cast(pl.Boolean).alias("has_parking"),
}


def to_silver(df: pl.DataFrame) -> SilverContractResult:
    """Validate every row. Never raises: rows that fail hard rules land in
    `quarantined` (retained, not dropped) and every fired rule is counted."""
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

    # infer_schema_length=None is REQUIRED. The default samples only the first
    # 100 rows, so a field that is null there and a string later raises
    # ComputeError. This is not hypothetical: it fires on
    # output/rent_contracts_20260916.csv at row 101 ("Hills Park"), while the
    # other four daily files pass. The ETL would crash on that day.
    frame = pl.DataFrame(records, infer_schema_length=None) if records else df.clear()
    return SilverContractResult(
        frame=frame,
        quarantined=pl.DataFrame(failed, infer_schema_length=None)
        if failed
        else df.clear(),
        groups=pl.DataFrame(),
        violation_counts=violations,
    )
```

**`frame` and `quarantined` are disjoint buckets, and the invariant is
`len(frame) + len(quarantined) == df.height`.** A row that cannot be constructed at all — null in a
required field — has no place in a validated frame, and Task 6's rollup aggregates over `frame`, so
a half-null row there is a hazard. It is not discarded: the raw source row goes to `quarantined`.
Rows that DO construct keep every value and carry their violations in the per-row `violations`
column, so `frame` remains the fail-open dataset (ADR-03). Test that invariant explicitly.

Add the imports `from dataclasses import dataclass`, `import polars as pl`, `hashlib`, and
`from pydantic import ValidationError`.

**Apply `_CASTS` at the top of `to_silver`, before the row loop.** Without it every row in the real
payload is dropped: `Optional[date]` rejects the `'2026-09-16T00:03:07'` datetime string, and `bool`
rejects the `None` that `PARKING` carries in 4220 of 4306 rows. Add this as the first statement of
`to_silver`:

```python
    casts = [expr for name, expr in _CASTS.items() if name in df.columns]
    if casts:
        df = df.with_columns(casts)
```

Also restore `_is_masked`, which the brief's Step 1 test depends on:

```python
def _is_masked(value) -> bool:
    """True for the upstream DLD word-mask: '??', '??????', '??? ??'.
    The original bytes do not exist, so these are unrecoverable."""
    return isinstance(value, str) and bool(value) and set(value) <= {"?", " "}
```

**Two forward references this task must not leave dangling.** `_row_hash` and `_rollup` are
specified in Tasks 5 and 6, but `to_silver` calls both. Define `_row_hash` and `_ROW_HASH_FIELDS`
in this task's Step 3 as shown in Task 5 Step 3, and leave `groups=pl.DataFrame()` as an empty
placeholder — Task 6 replaces that one line with `groups=_rollup(frame)` and adds the `_rollup`
function. Do not call a name this task has not defined; the test suite must be green at the end of
every task.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: PASS — 18 tests in the file

- [ ] **Step 5: Run the contract over the real 4306-row payload**

Run: `uv run python -c "import polars as pl; from lib.classes.silver_contract import to_silver; d=pl.read_csv('output/rent_contracts_20260917.csv', null_values=['null','NULL',''], ignore_errors=True, schema_overrides={'ANNUAL_AMOUNT':pl.Float64,'ACTUAL_AREA':pl.Float64,'IS_FREE_HOLD':pl.Int64,'CONTRACT_AMOUNT':pl.Float64}); d=d.rename({'USAGE_EN':'property_usage_en','AREA_EN':'area_name_en','PROP_TYPE_EN':'ejari_property_type_en','PROP_SUB_TYPE_EN':'ejari_property_sub_type_en','PROJECT_EN':'project_name_en','START_DATE':'contract_start_date','END_DATE':'contract_end_date','VERSION_EN':'version_en'}); r=to_silver(d); print('rows', len(r), 'of', d.height); print('psf_eligible', r.frame['psf_eligible'].sum()); print(dict(sorted(r.violation_counts.items(), key=lambda kv: -kv[1])[:8]))"`
Expected: `rows 4306 of 4306`, `psf_eligible` = 443, and `actual_area_below_psf_floor` as the largest count (≈ 3860)

- [ ] **Step 6: Commit**

```bash
git add lib/classes/silver_contract.py tests/test_silver_contract.py
git commit -m "feat: to_silver() validates every row and never raises

Schema failures land in quarantined (retained, not dropped) and every fired
rule is counted. Unrecoverable Arabic columns are dropped; masked cells in the
four partially-damaged columns become null and are counted. Runs clean over
the real 4306-row payload: 4306 out, 443 PSF-eligible."
```

---

### Task 5: Stable keys — `row_hash` and `record_id`

**Files:**
- Modify: `lib/classes/silver_contract.py`
- Modify: `tests/test_silver_contract.py`

**Interfaces:**
- Consumes: `to_silver` (Task 4)
- Produces: two new `SilverRentContract` fields — `row_hash: Optional[str]` and `record_id: Optional[str]` — populated by `to_silver`. `row_hash` is stable across re-runs; `record_id` is unique within a file.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_silver_contract.py`:

```python
def test_row_hash_is_stable_across_runs():
    from lib.classes.silver_contract import to_silver

    a = to_silver(_frame(1)).frame["row_hash"][0]
    b = to_silver(_frame(1)).frame["row_hash"][0]
    assert a == b, "row_hash must not depend on RN or row order"


def test_record_id_is_unique_within_a_file():
    from lib.classes.silver_contract import to_silver

    import polars as pl

    rows = []
    for i in range(50):
        r = _row()
        r["RN"] = i
        rows.append(r)
    frame = to_silver(pl.DataFrame(rows)).frame
    assert frame["record_id"].n_unique() == 50
    assert frame["record_id"].n_unique() == frame.height


def test_record_id_differs_when_row_number_differs():
    from lib.classes.silver_contract import to_silver

    import polars as pl

    a = to_silver(pl.DataFrame([{**_row(), "RN": 1}])).frame["record_id"][0]
    b = to_silver(pl.DataFrame([{**_row(), "RN": 2}])).frame["record_id"][0]
    assert a != b
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: FAIL — no `row_hash` column

- [ ] **Step 3: Add the key derivation**

Add `import hashlib` to the imports if not already present. Add two fields to the model:

```python
    # keys — row_hash is stable across re-runs, record_id is unique in-file
    row_hash: Optional[str] = None
    record_id: Optional[str] = None
```

Add a module constant and a helper:

```python
_ROW_HASH_FIELDS = (
    "area_name_en",
    "ejari_property_sub_type_en",
    "contract_start_date",
    "annual_amount",
    "actual_area",
)


def _row_hash(payload: dict) -> str:
    """Stable fingerprint for cross-day dedup. Deliberately excludes RN, which
    is the gateway's per-response ordinal and changes if rows are reordered."""
    parts = [str(payload.get(f) or "") for f in _ROW_HASH_FIELDS]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]
```

**If Task 4 already defined `_row_hash` and `_ROW_HASH_FIELDS` (per its Step 3 note), this task is
only the two model fields plus the `_SOURCE_COLUMNS` entries for `row_hash` and `record_id` — skip
re-adding the helper.**

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: PASS — 18 tests

- [ ] **Step 5: Verify cardinality against the real payload**

Run: `uv run python -c "import polars as pl; from lib.classes.silver_contract import to_silver; d=pl.read_csv('output/rent_contracts_20260917.csv', null_values=['null','NULL',''], ignore_errors=True, schema_overrides={'ANNUAL_AMOUNT':pl.Float64,'ACTUAL_AREA':pl.Float64}); d=d.rename({'AREA_EN':'area_name_en','PROP_SUB_TYPE_EN':'ejari_property_sub_type_en','START_DATE':'contract_start_date'}); f=to_silver(d).frame; print('rows', f.height); print('record_id distinct', f['record_id'].n_unique(), '(want 4306)'); print('row_hash distinct', f['row_hash'].n_unique(), '(spec says 3378 - may differ, key omits TOTAL_PROPERTIES)')"`
Expected: `rows 4306`, `record_id distinct 4306`

- [ ] **Step 6: Commit**

```bash
git add lib/classes/silver_contract.py tests/test_silver_contract.py
git commit -m "feat: stable row_hash and unique-in-file record_id

row_hash excludes RN so it survives gateway reordering and is safe for
cross-day dedup. record_id appends RN to reach full cardinality within a file.
No candidate key is both stable and unique, so both are needed."
```

---

### Task 6: Contract-level rollup — and the sum that must not be summed

**Files:**
- Modify: `lib/classes/silver_contract.py`
- Modify: `tests/test_silver_contract.py`

**Interfaces:**
- Consumes: `to_silver` (Task 5)
- Produces: `SilverContractGroup` model, and `SilverContractResult.groups` populated with one row per reconstructed block. Fields: `group_id: str`, `start_date`, `end_date`, `version_en`, `annual_amount: Decimal` (deduplicated, **never** summed), `total_properties: int`, `observed_property_count: int`, `total_area_sqft: Decimal` (summed), `record_ids: list[str]`, `usages: list[str]`, `is_complete: bool`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_silver_contract.py`:

```python
def _block(n, **over):
    rows = []
    for _ in range(n):
        r = _row(**over)
        r["total_properties"] = n
        rows.append(r)
    return rows


def test_rollup_reconstructs_a_declared_block():
    import polars as pl
    from lib.classes.silver_contract import to_silver

    res = to_silver(pl.DataFrame(_block(3)))
    assert res.groups.height == 1
    g = res.groups.row(0, named=True)
    assert g["total_properties"] == 3
    assert g["observed_property_count"] == 3
    assert g["is_complete"] is True


def test_rollup_never_sums_annual_amount():
    """The 87-property block carries ONE distinct annual_amount repeated 87x.
    Summing gives AED 265,671,900 instead of the real AED 3,053,700."""
    import polars as pl
    from lib.classes.silver_contract import to_silver

    rows = _block(87, annual_amount=Decimal("3053700"), actual_area=Decimal("20.03"))
    res = to_silver(pl.DataFrame(rows))
    g = res.groups.row(0, named=True)
    assert g["annual_amount"] == Decimal("3053700")
    assert g["annual_amount"] != Decimal("265671900")
    assert g["total_area_sqft"] == Decimal("20.03") * 87


def test_rollup_flags_partial_capture():
    import polars as pl
    from lib.classes.silver_contract import to_silver

    rows = _block(3)
    res = to_silver(pl.DataFrame(rows[:1]))
    g = res.groups.row(0, named=True)
    assert g["total_properties"] == 3
    assert g["observed_property_count"] == 1
    assert g["is_complete"] is False


def test_rollup_flags_merged_groups():
    import polars as pl
    from lib.classes.silver_contract import to_silver

    rows = _block(2) + _block(2)
    for r in rows:
        r["contract_start_date"] = date(2026, 9, 20)
        r["contract_end_date"] = date(2027, 9, 19)
        r["annual_amount"] = Decimal("43200")
    res = to_silver(pl.DataFrame(rows))
    g = res.groups.row(0, named=True)
    assert g["observed_property_count"] == 4, "two 2-property contracts merged"
    assert g["is_complete"] is False
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: FAIL — `res.groups` is empty

- [ ] **Step 3: Implement the rollup**

Add to `lib/classes/silver_contract.py`:

```python
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
    annual_amount: Decimal
    total_properties: int
    observed_property_count: int
    total_area_sqft: Decimal
    record_ids: list[str]
    usages: list[str]
    is_complete: bool
```

```python
_GROUP_KEY = ("contract_start_date", "contract_end_date", "annual_amount", "version_en")


def _rollup(frame: pl.DataFrame, violations: dict[str, int]) -> pl.DataFrame:
    """Reconstruct contract blocks for rows declaring more than one property.
    CONTRACT_NUMBER is 100% null upstream, so the key is the best available
    proxy: it reconstructs 82 of 84 declared blocks exactly on the real
    payload. The 2 failures are two contracts sharing dates and amount, which
    is irreducibly ambiguous without a contract number."""
    if frame.height == 0 or "total_properties" not in frame.columns:
        return pl.DataFrame()

    multi = frame.filter(pl.col("total_properties") > 1)
    if multi.height == 0:
        return pl.DataFrame()

    rows = []
    for key, block in multi.group_by(list(_GROUP_KEY), maintain_order=True):
        # annual_amount is a _GROUP_KEY member, so it CANNOT disagree inside a
        # group. Do not add a len(amounts) != 1 branch: it is unreachable.
        declared = block["total_properties"][0]
        observed = block.height
        if observed > declared:
            # the key merged two distinct contracts: irreducibly ambiguous
            # without a contract number
            violations["merged_contract_group"] = violations.get(
                "merged_contract_group", 0
            ) + observed
        elif observed < declared:
            # the window captured only part of the contract
            violations["partial_contract_capture"] = violations.get(
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
```

In `to_silver`, replace `groups=pl.DataFrame(),` with `groups=_rollup(frame),`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_silver_contract.py -v`
Expected: PASS — 22 tests

- [ ] **Step 5: Verify rollup fidelity against the real payload (spec gate 9)**

`output/` is gitignored so this is a local check. Expected: 82 of 84 declared blocks reconstruct
with `is_complete = True`.

```bash
uv run python -c "
import polars as pl
from lib.classes.silver_contract import to_silver
d = pl.read_csv('output/rent_contracts_20260917.csv', null_values=[''], ignore_errors=True,
                schema_overrides={'ANNUAL_AMOUNT': pl.Float64, 'ACTUAL_AREA': pl.Float64})
d = d.rename({'AREA_EN':'area_name_en','PROP_SUB_TYPE_EN':'ejari_property_sub_type_en',
              'START_DATE':'contract_start_date','END_DATE':'contract_end_date',
              'USAGE_EN':'property_usage_en','VERSION_EN':'version_en'})
g = to_silver(d).groups
print('groups', g.height, '| complete', g.filter(pl.col('is_complete')).height)
print(g.sort('total_properties', descending=True).head(3)
        .select(['total_properties','observed_property_count','annual_amount','total_area_sqft','is_complete']))
"
```

Expected: `complete 82`; largest row `total_properties 87`, `annual_amount 3053700`,
`total_area_sqft ≈ 1742.61`, `is_complete True`.

- [ ] **Step 6: Commit**

```bash
git add lib/classes/silver_contract.py tests/test_silver_contract.py
git commit -m "feat: contract-level rollup with dedup-not-sum semantics

annual_amount is contract-level and repeated per property row, so it is
deduplicated; total_area_sqft is per-property, so it is summed. Summing the
money overstates the 87-property Muhaisanah block as AED 265,671,900 instead
of AED 3,053,700. Partial captures and merged groups are flagged, not dropped."
```

---

### Task 7: Wire into the ETL and stop the gate from lying

**Files:**
- Modify: `run_etl_pipeline.py:119-146` (`transform_rents`)
- Modify: `lib/transform/rents_transformer.py:22` (encoding) and `:53` (alias)
- Modify: `lib/config.py:190` (`required_fields`)
- Modify: `tests/test_etl_pipeline.py`

**Interfaces:**
- Consumes: `to_silver(df) -> SilverContractResult` (Tasks 4–6)
- Produces: `transform_rents` writes the validated frame to the same parquet path and logs the violation summary. `lib.config.DATA_QUALITY_RULES["required_fields"]` no longer lists `contract_id`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_etl_pipeline.py`, inside `class TestETLPipelineIntegration`:

```python
    def test_transform_rents_runs_the_silver_contract(self, tmp_path):
        """transform_rents must run to_silver and rewrite the parquet.

        RentsTransformer is stubbed to do a real CSV -> parquet write, so
        read_parquet and write_parquet are NOT patched. Asserting on a patched
        DataFrame.write_parquet would only see the path string, not the frame.
        """
        import polars as pl
        from unittest.mock import patch
        import run_etl_pipeline
        from lib.transform.rents_transformer import RentsTransformer

        csv = tmp_path / "rent_contracts_20260917.csv"
        parquet = tmp_path / "rent_contracts_20260917.parquet"
        pl.read_csv(
            'output/rent_contracts_20260917.csv', n_rows=50, null_values=[''],
            ignore_errors=True,
            schema_overrides={'ANNUAL_AMOUNT': pl.Float64, 'ACTUAL_AREA': pl.Float64},
        ).write_csv(csv)

        ok = run_etl_pipeline.transform_rents(str(csv), str(parquet))
        assert ok is True

        # spec gate 8: the written frame keeps 1 row per input row (ADR-02)
        assert parquet.exists(), "transform_rents must rewrite the parquet in place"
        written = pl.read_parquet(parquet)
        assert written.height == 50
        assert written['record_id'].n_unique() == 50
        assert 'violations' in written.columns
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_etl_pipeline.py -k silver_contract -v`
Expected: FAIL — `to_silver` is never called, so no Silver summary is logged

- [ ] **Step 3: Narrow the blanket except in transform_rents**

In `run_etl_pipeline.py`, replace the body of `transform_rents` (lines 119–146) with:

```python
def transform_rents(input_csv: str, output_parquet: str) -> bool:
    """Transform rents CSV to Parquet, then run the Silver contract."""
    logger.info("=== PHASE 2: TRANSFORM ===")
    try:
        if not RentsTransformer(input_csv, output_parquet).transform():
            logger.error("Transformation failed.")
            return False
        logger.info(f"Transformation complete: {output_parquet}")

        import polars as pl
        from lib.classes.silver_contract import to_silver

        result = to_silver(pl.read_parquet(output_parquet))
        logger.info(f"Silver contract: {len(result)} rows validated")
        if result.violation_counts:
            top = sorted(
                result.violation_counts.items(), key=lambda kv: -kv[1]
            )[:5]
            logger.info(f"Silver violations: {top}")

        result.frame.write_parquet(
            output_parquet, compression="zstd", compression_level=3
        )
        logger.info(f"Silver frame written: {output_parquet}")
        return True
    except Exception as e:
        logger.error(f"Transformation failed with exception: {e}")
        raise
```

The previous version wrapped the validator in `try/except Exception` and logged
`"Validation gate skipped"` — that is what let a module-level `NameError` masquerade as a passing
gate. The import now sits on the main path, so a broken module fails loudly.

- [ ] **Step 4: Fix the encoding and the dead alias**

In `lib/transform/rents_transformer.py`, change line 22 from `encoding="utf8-lossy",` to
`encoding="utf8",` — **not** `"utf-8"`. polars 1.44.2 accepts `utf-8` in `read_csv` (eager) but
**rejects it in `scan_csv` (lazy)** with `ValueError: csv encoding must be one of {'utf8',
'utf8-lossy'}`, and `RentsTransformer` uses `scan_csv`. Verified on this version. Delete the `"CONTRACT_NUMBER": "contract_id",` entry from the `aliases` dict
(line 53) — `CONTRACT_NUMBER` is 100% null, so the alias only manufactures an all-null column, and
`record_id` is produced downstream by the Silver contract where `RN` is still available.

- [ ] **Step 5: Drop contract_id from required_fields**

In `lib/config.py`, change `DATA_QUALITY_RULES["required_fields"]` to:

```python
    "required_fields": [
        "contract_start_date",
        "property_usage_en",
        "annual_amount",
    ],
```

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -q`
Expected: PASS — all files collect and pass

- [ ] **Step 7: Commit**

```bash
git add run_etl_pipeline.py lib/transform/rents_transformer.py lib/config.py tests/test_etl_pipeline.py
git commit -m "fix: run the Silver contract on the pipeline path, not in a swallowed except

transform_rents no longer wraps validation in except Exception, so an import
break fails loudly instead of logging 'Validation gate skipped' and reporting
success. Strict utf-8 replaces utf8-lossy (the payload is valid UTF-8). Drops
the CONTRACT_NUMBER -> contract_id alias, which only built an all-null column,
and contract_id from required_fields."
```

---

### Task 8: Split `validators.py` responsibilities

**Files:**
- Modify: `lib/classes/validators.py` — remove `_validate_rent_amounts`, `_validate_property_sizes`, `_validate_data_types` and their call sites
- Modify: `tests/test_etl_pipeline.py` — move `test_validate_rent_amounts` out

**Interfaces:**
- Consumes: `SilverRentContract` (Task 3) for all per-row range checks
- Produces: `RentContractValidator.validate_dataframe` continues to return `ValidationResult`, now covering only schema presence, null rates, date sanity, IQR outliers, and empty frames.

- [ ] **Step 1: Move the rent-amount test to the Silver test file**

In `tests/test_etl_pipeline.py`, delete the `test_validate_rent_amounts` method (lines 310–315).
Add to `tests/test_silver_contract.py`:

```python
def test_rent_below_zero_is_a_violation_not_an_error():
    """Per-row rent checks moved off validators.py; this asserts the coverage
    did not disappear with them."""
    import polars as pl
    from lib.classes.silver_contract import to_silver

    res = to_silver(_frame(1, annual_amount=Decimal("-1")))
    assert "annual_amount_not_positive" in res.violation_counts
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_silver_contract.py -k rent_below -v`
Expected: PASS immediately if `_row` overrides work, otherwise FAIL on the override

If it fails because `annual_amount=Decimal("-1")` is rejected by `Field(ge=0)` before the validator
runs, relax the model field to `Decimal` with no bound and let
`_derive_and_collect` own the check. Confirm by re-running before continuing.

- [ ] **Step 3: Remove the three per-row checks from validators.py**

Delete these methods from `lib/classes/validators.py`:
`_validate_data_types` (lines 214–223), `_validate_rent_amounts` (lines 225–258),
`_validate_property_sizes` (lines 260–292).

Delete their call sites in `validate_dataframe` (lines 173, 176, 177):

```python
        # Validate data types
        self._validate_data_types(df, result)

        # Validate ranges
        self._validate_rent_amounts(df, result)
        self._validate_property_sizes(df, result)
```

Replace with a comment noting where the checks went:

```python
        # Per-row types and ranges are the Silver contract's job —
        # see lib/classes/silver_contract.py. Kept here: schema presence,
        # null rates, date sanity, outliers.
```

Leave `_validate_schema`, `_validate_required_fields`, `_validate_dates`,
`_validate_business_logic` and `_detect_outliers` in place. Remove `VALIDATION_THRESHOLDS` from
the `lib.config` import if it becomes unused — check first, `_validate_business_logic` still uses
`min_contract_days` and `max_contract_days`.

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add lib/classes/validators.py tests/test_etl_pipeline.py tests/test_silver_contract.py
git commit -m "refactor: validators.py keeps only aggregate checks

Drops _validate_rent_amounts, _validate_property_sizes and
_validate_data_types — the Silver contract does all three per row, more
precisely, from the same VALIDATION_THRESHOLDS dict. validators.py retains
schema presence, null rates, date sanity and IQR outliers, which no per-row
model can do. Net smaller, no duplicated thresholds."
```

---

### Task 9: Gold prerequisite — the duplicate PSF site

`lib/classes/property_usage.py:90-96` re-derives `annual_amount / actual_area` guarded only by
`actual_area > 0`, bypassing the `>= 200` guard in `lib/transform/enrichment.py:90`. The shipped
`output/property_usage_20260913.csv` still reports Residential `avg_psf 4691.52` /
`median_psf 995.27`, both outside the 20–500 band at `lib/config.py:96`.

**Files:**
- Modify: `lib/classes/property_usage.py:88-101`
- Modify: `tests/test_p0_gates.py`

**Interfaces:**
- Consumes: the enriched `price_per_sqft` column from `lib/transform/enrichment.py:83`
- Produces: `PropertyUsage.transform` reports `avg_psf` / `median_psf` from `price_per_sqft`, so the `>= 200` guard applies exactly once, in one place.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_p0_gates.py`:

```python
def test_property_usage_psf_respects_the_200_sqft_floor():
    """PSF must come from the enriched price_per_sqft, which is null below 200
    sqft. Recomputing it from actual_area bypasses the guard and ships avg_psf
    4691 (output/property_usage_20260913.csv)."""
    import tempfile
    from pathlib import Path
    from lib.classes.property_usage import PropertyUsage

    df = pl.DataFrame({
        "property_usage_en": ["Residential", "Residential", "Residential"],
        "annual_amount": [100000.0, 200000.0, 300000.0],
        "actual_area": [1.0, 15.9, 1000.0],
        "price_per_sqft": [None, None, 300.0],
        "no_of_contracts": [1, 1, 1],
    })
    enriched = enrich_rent_contracts(df.with_columns(
        pl.col("price_per_sqft").alias("_drop_me")
    ).select([c for c in df.columns if c != "price_per_sqft"]))

    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "in.parquet"
        out = Path(tmp) / "out.csv"
        enriched.write_parquet(src)
        PropertyUsage(str(out)).transform(str(src))
        report = pl.read_csv(out)
    psf = report["avg_psf"][0]
    assert psf is None or 20 <= psf <= 500, f"avg_psf {psf} outside 20-500"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_p0_gates.py -k psf_respects -v`
Expected: FAIL — avg_psf computed from unfiltered `actual_area` exceeds 500

- [ ] **Step 3: Read `price_per_sqft` instead of recomputing**

In `lib/classes/property_usage.py`, replace the PSF block (lines 88–101) with:

```python
            # PSF comes from the enriched column, which is null below
            # VALIDATION_THRESHOLDS min_property_size (200 sqft). Recomputing
            # it here from actual_area bypasses that guard.
            if "price_per_sqft" in lf.collect_schema().names():
                psf_stats = lf.filter(
                    (pl.col("property_usage_en").is_not_null()) &
                    (pl.col("price_per_sqft").is_not_null())
                ).with_columns(
                    pl.col("price_per_sqft").cast(pl.Float64).alias("psf")
                ).group_by("property_usage_en").agg([
                    pl.col("psf").mean().alias("avg_psf"),
                    pl.col("psf").median().alias("median_psf"),
                ]).collect()
            else:
                psf_stats = None
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_p0_gates.py -v`
Expected: PASS

- [ ] **Step 5: Verify no `avg_psf > 500` can ship**

Run: `uv run python -c "import polars as pl; d=pl.read_csv('output/property_usage_20260913.csv'); print('shipped avg_psf:', d['avg_psf'].to_list(), 'median_psf:', d['median_psf'].to_list())"`
Expected: the historical file still shows 4691 — that is the pre-fix artifact. Confirm the guard by
asserting Step 1's test passes, not by editing the historical CSV.

- [ ] **Step 6: Commit**

```bash
git add lib/classes/property_usage.py tests/test_p0_gates.py
git commit -m "fix: PSF computed once, in enrichment, behind the 200 sqft guard

property_usage.py re-derived annual_amount/actual_area guarded only by
actual_area > 0, bypassing the >= 200 guard in enrichment.py and shipping
avg_psf 4691 / median_psf 995 against a 20-500 band. It now reads the enriched
price_per_sqft, so the guard applies in exactly one place."
```

---

### Task 10: Gold prerequisite — the `area_median_index` bulk leak

`docs/IMPLEMENTATION_PLAN.md:83` requires medians within 20k–500k and no `>1M` leak. The shipped
`output/area_median_index_20260913-17.csv` has 26 areas with `max_rent > 1M` (up to AED 4.3M),
`median_rent` reaching 590,000, and 74/121 areas with >20% mean/median skew.

**Files:**
- Find: the generator that writes `output/area_median_index_*.csv` — check `lib/analysis/build_weekly_duckdb.py` and `lib/analysis/metro_volume.py`
- Create: `lib/analysis/gold_indexes.py` if no importable generator exists
- Create: `tests/test_gold_indexes.py`

**Interfaces:**
- Consumes: `is_bulk_registration` from `lib/transform/enrichment.py:235`
- Produces: `build_area_median_index(df: pl.DataFrame) -> pl.DataFrame` — an importable, testable function. Returns one row per area with `area_name_en`, `n`, `median_rent`, `mean_rent`, `min_rent`, `max_rent`, and zero rows where `max_rent > 1_000_000`.

**Note on fixtures:** `.gitignore` excludes `**.csv` and `**.parquet`, so no data fixture can be
committed. The test therefore builds a **synthetic** frame in-process, matching the style already
used in `tests/test_p0_gates.py`. Do not write a test that globs `output/` — it would pass vacuously
in CI where those files do not exist.

- [ ] **Step 1: Locate the generator**

Run: `uv run grep -rn "area_median_index" lib/ run_etl_pipeline.py Makefile`
Expected: either the file and line that writes it, or no hit — in which case the CSV was produced
ad hoc and Step 3 creates the generator.

- [ ] **Step 2: Write the failing test**

Create `tests/test_gold_indexes.py`:

```python
import polars as pl


def _rows(n, area, amount, sub_type, area_sqft=500.0):
    """n rows sharing one (area, amount) pair — this is what trips the bulk filter."""
    return [
        {
            "area_name_en": area,
            "annual_amount": amount,
            "actual_area": area_sqft,
            "ejari_property_sub_type_en": sub_type,
        }
        for _ in range(n)
    ]


def _spread(n, area, amount, sub_type, step=100.0, area_sqft=500.0):
    """n rows spread over distinct amounts, so no (area, amount) group exceeds
    10. A legitimate area with many units looks like this; a bulk filing does not."""
    return [
        {
            "area_name_en": area,
            "annual_amount": amount + i * step,
            "actual_area": area_sqft,
            "ejari_property_sub_type_en": sub_type,
        }
        for i in range(n)
    ]


def test_bulk_block_is_excluded_from_the_median():
    """Naif-style bulk: 87 identical rows at AED 1.54M. Unfiltered, this drags
    mean_rent to 8x the median and pushes max_rent past 1M."""
    from lib.analysis.gold_indexes import build_area_median_index

    rows = _rows(87, "Naif", 1_540_471.0, "Hotel") + _spread(40, "Naif", 60_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out["median_rent"][0] == 60_000.0
    assert out["max_rent"][0] <= 1_000_000


def test_legitimate_area_survives_the_bulk_filter():
    """A real area with many units must NOT be mistaken for a bulk filing, so
    its amounts must be spread across groups of at most 10."""
    from lib.analysis.gold_indexes import build_area_median_index

    rows = _spread(40, "Dubai Marina", 90_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.height == 1
    assert out["area_name_en"][0] == "Dubai Marina"
    assert out["n"][0] == 40


def test_labor_camps_are_excluded():
    """IMPLEMENTATION_PLAN.md:46 excludes Hotel / Labor Camps / Virtual Unit."""
    from lib.analysis.gold_indexes import build_area_median_index

    rows = _spread(30, "Al Goze Industrial Second", 590_000.0, "Labor Camps")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.height == 0, "Labor Camps must not reach the index"


def test_every_emitted_area_has_n_at_least_10():
    from lib.analysis.gold_indexes import build_area_median_index

    rows = _spread(40, "Dubai Marina", 90_000.0, "Flat") + _rows(3, "Al Satwa", 55_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.filter(pl.col("n") < 10).height == 0
    assert "Dubai Marina" in out["area_name_en"].to_list()
    assert "Al Satwa" not in out["area_name_en"].to_list()


def test_median_within_plausible_dubai_band():
    from lib.analysis.gold_indexes import build_area_median_index

    rows = _spread(50, "Dubai Marina", 120_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    assert 20_000 <= out["median_rent"][0] <= 500_000
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `uv run pytest tests/test_gold_indexes.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'lib.analysis.gold_indexes'`

- [ ] **Step 4: Create the generator**

Create `lib/analysis/gold_indexes.py`:

```python
"""Gold index builders. Exclusions come from docs/IMPLEMENTATION_PLAN.md:46 —
HOTEL_AND_MASS_LANDLORD_SUBTYPES are never market comparables, and
is_bulk_registration marks DLD bulk filings that skew means by 8x.
"""
import polars as pl

EXCLUDED_SUBTYPES = ["Hotel", "Labor Camps", "Virtual Unit"]
BULK_THRESHOLD = 10
MEDIAN_FLOOR_AED = 20_000
MEDIAN_CEILING_AED = 500_000


def build_area_median_index(df: pl.DataFrame) -> pl.DataFrame:
    """One row per area, median-led. Emits only areas with n >= 10."""
    counts = df.group_by(["area_name_en", "annual_amount"]).agg(pl.len().alias("_n"))
    clean = (
        df.join(counts, on=["area_name_en", "annual_amount"], how="left")
        .filter(pl.col("_n") <= BULK_THRESHOLD)
        .filter(~pl.col("ejari_property_sub_type_en").is_in(EXCLUDED_SUBTYPES))
        .filter(pl.col("annual_amount").is_not_null() & (pl.col("annual_amount") > 0))
    )
    if clean.height == 0:
        return pl.DataFrame(
            schema={
                "area_name_en": pl.String,
                "n": pl.Int64,
                "median_rent": pl.Float64,
                "mean_rent": pl.Float64,
                "min_rent": pl.Float64,
                "max_rent": pl.Float64,
            }
        )
    return (
        clean.group_by("area_name_en")
        .agg(
            pl.len().alias("n"),
            pl.col("annual_amount").median().alias("median_rent"),
            pl.col("annual_amount").mean().alias("mean_rent"),
            pl.col("annual_amount").min().alias("min_rent"),
            pl.col("annual_amount").max().alias("max_rent"),
        )
        .filter(pl.col("n") >= BULK_THRESHOLD)
        .sort("median_rent")
    )
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_gold_indexes.py -v`
Expected: PASS — 4 tests

- [ ] **Step 6: Regenerate the real index (local only)**

`output/` is gitignored, so this step is a local check, not a CI gate:

```bash
uv run python -c "
import polars as pl
from lib.analysis.gold_indexes import build_area_median_index
d = pl.read_csv('output/rent_contracts_20260913.csv', null_values=[''], ignore_errors=True)
d = d.rename({'AREA_EN':'area_name_en','ANNUAL_AMOUNT':'annual_amount','ACTUAL_AREA':'actual_area','PROP_SUB_TYPE_EN':'ejari_property_sub_type_en'})
out = build_area_median_index(d)
out.write_csv('output/area_median_index_local.csv')
print('areas', out.height, '| max_rent>1M', out.filter(pl.col('max_rent')>1000000).height, '| median range', out['median_rent'].min(), out['median_rent'].max())
"
```
Expected: `max_rent>1M 0` and `median range` inside 20,000–500,000

- [ ] **Step 7: Commit**

```bash
git add lib/analysis/gold_indexes.py tests/test_gold_indexes.py
git commit -m "fix: area_median_index applies the bulk-registration exclusion

The shipped index failed its own gate at IMPLEMENTATION_PLAN.md:83 — 26 areas
with max_rent >1M, median_rent reaching 590k, 74/121 areas with >20% mean-median
skew. Adds build_area_median_index() applying is_bulk_registration (count>10)
and the Hotel/Labor Camps/Virtual Unit exclusion, emitting only areas with
n>=10. Tested synthetically because .gitignore excludes **.csv fixtures."
```

---

### Task 11: ADR-06, dependency files, and plan status

**Files:**
- Create: `docs/adr/0006-pydantic-silver-contract.md`
- Modify: `docs/IMPLEMENTATION_PLAN.md` (local only — gitignored, see note)
- Modify: `requirements.txt`
- Modify: `README.md:43`

**Interfaces:**
- Consumes: everything shipped in Tasks 1–10
- Produces: a committed ADR, a dependency list matching `pyproject.toml`, and a README that describes pydantic accurately.

**Note on `.gitignore`:** `docs/IMPLEMENTATION_PLAN.md` and `docs/EXPERT_REVIEW_AND_ROADMAP.md` are
both listed under "local agent bundle & internal docs — not public". An ADR written only there would
never be committed. So ADR-06 is written to a **tracked** `docs/adr/` file, and the
`IMPLEMENTATION_PLAN.md` edits in Steps 2–3 are local bookkeeping that will not appear in the diff.
Do not add a `.gitignore` exception unless the user asks — the exclusion looks deliberate.

- [ ] **Step 1: Write ADR-06 to a tracked file**

Create `docs/adr/0006-pydantic-silver-contract.md`:

```markdown
# ADR-06: Pydantic v2 for the per-row Silver contract

**Status:** Accepted · **Date:** 2026-09-27

## Context

`docs/IMPLEMENTATION_PLAN.md:68` requires an ADR before any new dependency. The Silver layer needs
to attribute data-quality violations to individual fields, so they can be counted and queried
rather than logged as a single aggregate line.

## Decision

Use pydantic v2 for the per-row contract in `lib/classes/silver_contract.py`.

## Rationale

- **Cost is negligible.** Measured 548,000 rows/sec on this payload, so a 90-day backfill of ~150k
  rows costs ~0.3s. ADR-01's streaming constraint is not a reason to keep this vectorised.
- **Field-level attribution.** `schema_overrides` in `lib/transform/rents_transformer.py:24` can
  coerce a column but cannot say *which* rule a specific row violated. 9 rows below
  `min_annual_rent` and 1 above `max_annual_rent` are indistinguishable from 3,860 rows below the
  PSF floor without a per-row model.
- **Fails loudly by default.** The original `BronzeRentContract` used leading-underscore field
  names, which pydantic v2 rejects at class-definition time with `NameError`. That took down
  `lib/classes/validators.py` entirely and the failure was invisible in production because
  `run_etl_pipeline.py` swallowed it. The module now has an import smoke test.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Extend `pl.scan_csv` `schema_overrides` only | Cannot attribute violations to rows or rules |
| Vectorised polars expressions throughout | Loses per-row context; a later reader cannot tell which rule fired |
| dataclasses instead of pydantic | No coercion, no declarative field bounds, more hand-written parsing |

## Consequences

pydantic v2 is a permanent runtime dependency. `requirements.txt` must track `pyproject.toml`.
Field naming is constrained: canonical snake_case pipeline names only, and no leading underscores.
```

- [ ] **Step 2: Correct the P0 exit gate locally**

In `docs/IMPLEMENTATION_PLAN.md`, amend the Phase 0 exit gate at line 31 to record that only
443/4306 (10.3%) are PSF-eligible, so `avg_psf 45-110` is not a stable expectation on a 1-day
sample. Add:

```markdown
> **Measured 2026-09-27:** only 10.3% of rows (443/4306) yield a PSF (446 clear the 200 floor, 3 of those exceed the 50000 max), and median
> area is 75 sqft. The `avg_psf 45-110` gate is not achievable from a 1-day window; the correct
> gate is "PSF null or within 20-500, with `n` published".
```

- [ ] **Step 3: Reconcile Phase 0 and Phase 2 status locally**

Note in the same document that `area_median_index` (121 areas) and `standard_lease_index` already
exist in `output/`, and that Phase 0 items 0.1–0.4 have shipped in `3263372`.

- [ ] **Step 4: Fix requirements.txt**

Add `pydantic>=2` and remove `psutil` (present in `requirements.txt`, absent from `pyproject.toml`).
Verify the two files agree:

Run: `uv run python -c "import tomllib,pathlib; a=set(tomllib.loads(pathlib.Path('pyproject.toml').read_text())['project']['dependencies']); b=set(pathlib.Path('requirements.txt').read_text().split()); print('only in pyproject:', sorted(a-b)); print('only in requirements:', sorted(b-a))"`
Expected: `only in requirements: []` — no package is missing from `requirements.txt`

- [ ] **Step 5: Correct the README claim**

`README.md:43` says pydantic is "for typed analysis result models". It is a Silver per-row
contract. Change to:

```markdown
- **Pydantic** for the per-row Silver contract and its violation reporting
```

- [ ] **Step 6: Run the full suite and commit**

Run: `uv run pytest -q`
Expected: PASS

```bash
git add docs/adr/0006-pydantic-silver-contract.md requirements.txt README.md
git commit -m "docs: ADR-06 for pydantic, sync deps, fix the README claim

ADR-06 goes to docs/adr/ because IMPLEMENTATION_PLAN.md is gitignored as an
internal doc, so an ADR written only there would never be committed. Records
the 548k rows/sec measurement that makes per-row validation affordable, and
the field-level attribution that schema_overrides cannot provide."
```

---

## Verification

**CI gate — must pass everywhere:**

```bash
uv run pytest -q
uv run grep -rn "_sqm" lib/ tests/ ; echo "grep exit=$? (1 = no matches, which is correct)"
```

**Local gate — requires `output/rent_contracts_20260917.csv`, which `.gitignore` excludes from the
repo. Run these only on a machine that has ETL output on disk:**

```bash
uv run python -c "
import polars as pl
from lib.classes.silver_contract import to_silver
d = pl.read_csv('output/rent_contracts_20260917.csv', null_values=[''], ignore_errors=True,
                schema_overrides={'ANNUAL_AMOUNT': pl.Float64, 'ACTUAL_AREA': pl.Float64})
d = d.rename({'AREA_EN':'area_name_en','PROP_SUB_TYPE_EN':'ejari_property_sub_type_en',
              'START_DATE':'contract_start_date','END_DATE':'contract_end_date',
              'USAGE_EN':'property_usage_en','VERSION_EN':'version_en'})
r = to_silver(d); f = r.frame
print('rows', len(r), '/', d.height, '(want 4306/4306)')
print('record_id distinct', f['record_id'].n_unique(), '(want 4306)')
print('psf_eligible', f['psf_eligible'].sum(), '(want 443)')
print('groups', r.groups.height, 'complete', r.groups.filter(pl.col('is_complete')).height, '(want 82 complete)')
"
```

Expected: 4306/4306 rows, 4306 distinct `record_id`, 443 PSF-eligible, 82 complete groups, and no
`_sqm` anywhere. If `record_id` is not 4306 distinct, Task 5's `RN` wiring is wrong. If complete
groups is not 82, the rollup key in Task 6 has drifted from the measured `(start, end, amount,
version)`.

## What this plan does not cover

- **Building the Gold marts.** Spec §3.7 specifies their corrected shape — natural keys instead of
  the eight degenerate `*_ID` columns, `rent_per_sqft` not `rent_per_sqm`, `n` on every aggregate,
  `nearest_metro` off `DimArea` — but they are a separate spec and a separate plan.
- **Phase 1.1 backfill** and **Phase 1.2** Silver consolidation to `rents_silver.parquet`.
- **A committed data fixture.** `.gitignore` excludes `**.csv` and `**.parquet`, so every test here
  is synthetic. If you want regression tests pinned to the real 1-day payload, that needs a
  `.gitignore` exception and a `tests/fixtures/` directory — ask before adding one.

# Silver Contract Layer — Design Spec

**Date:** 2026-09-27
**Status:** Approved
**Scope:** `lib/classes/silver_contract.py` (new) · `lib/classes/validators.py` · `lib/transform/rents_transformer.py` · `lib/classes/property_usage.py` · `run_etl_pipeline.py` · `tests/` · `docs/`
**Parent:** `docs/IMPLEMENTATION_PLAN.md` · `docs/EXPERT_REVIEW_AND_ROADMAP.md`
**Evidence base:** `output/rent_contracts_20260917.csv` — 4306 rows × 44 columns, decoded strict UTF-8; cross-checked against 4 further daily files (16,075 rows total)

---

## 1. Problem

Six defects in the raw Ejari payload were blocking a trustworthy Silver layer. An initial
`BronzeRentContract` pydantic model was drafted to address them but broke the module: Pydantic v2
rejects leading-underscore field names, so `lib/classes/validators.py` raised `NameError` at import,
`pytest` could not collect `test_etl_pipeline.py` or `test_p0_gates.py`, and
`run_etl_pipeline.py:139`'s blanket `except Exception` swallowed the failure — so the P0.3 fail-open
validation gate logged as passing while never executing.

Three further drafts were supplied — a `SilverRentContract`, a `SilverContractGroup` contract-level
rollup, and a set of Gold business marts. Verification against the payload found:

- the contract model **cannot be constructed at all** (four derived fields declared required, so
  `mode="after"` validators never run);
- **three of its six proposed treatments do not survive contact with the data** (encoding repair,
  the `CONTRACT_NUMBER` grouping key, and the `0 < area < 5000` guard);
- the rollup's two identity fields are empty upstream, though the rollup itself is sound on a
  different key;
- **all eight of the Gold marts' key columns are single-valued**, so every proposed dimension would
  materialise one row.

This spec records the corrected design for all three.

### 1.1 Baseline defects in the payload

| # | Defect | Measured in payload |
|---|---|---|
| 1 | Arabic columns corrupted | Upstream DLD **word-length mask**, not mojibake. `Free Hold`→`??`, `Non Free Hold`→`??? ??`, `New`→`??????`, `Renewed`→`?????`. File decodes as strict UTF-8; the `?` are literal ASCII already committed to disk. `MASTER_PROJECT_AR` is 100% *null*, not corrupted |
| 2 | `CONTRACT_AMOUNT` ≠ `ANNUAL_AMOUNT` | 358/4306 rows (8.3%) differ. `contract_amount / annual_amount` correlates **0.9956** with duration-in-years; mean absolute difference **0.0065 years** |
| 3 | Duplicate row blocks | **508 rows (11.8%)** declare `TOTAL_PROPERTIES > 1`; 3798 rows declare `1` and are already one contract per row. Largest blocks are Labor Camps: Muhaisanah Second 87 properties @AED 3.05M, Jabal Ali 26 and 25 @1.09M/1.05M, Al Goze Industrial 19 and 18 @1.14M. Some genuine blocks span multiple `AREA_EN` (one 19-property block covers 7 areas; one 18-property block covers 12) |
| 4 | `TOTAL` constant | Single distinct value `4306` across all rows |
| 5 | Nonsensical `ACTUAL_AREA` | min `1.0`, median `75.0`, 25th pct `40.69`, max `882158.0`. Only **443/4306 (10.3%)** yield a PSF — 446 clear the 200 floor, but 3 of those (86281, 148698, 882158 sqft) exceed the 50000 max and route to `actual_area_above_max` |
| 6 | `IS_FREE_HOLD` duplicated as int and text | Perfect 1:1 with `IS_FREE_HOLD_EN` — 2519 `Free Hold`, 1787 `Non Free Hold`, zero cross-tab exceptions |

---

## 2. Verified corrections to the draft

### 2.1 Encoding repair is impossible — defect 1

The draft proposed `v.encode("cp1252").decode("utf-8")` with a quarantine fallback. **This cannot
work.** The source bytes no longer exist: the mask is `?`-per-character applied upstream at DLD, and
the file is valid UTF-8, so there is nothing to reverse. Worse, the validator is unsafe as written —
on already-correct Arabic it raises `UnicodeEncodeError`, which the bare `except` converts to
"return unchanged", so the one path where repair *would* be legitimate never runs.

Treatment: **detect, drop, count.** No repair is attempted.

- Drop as unrecoverable: `VERSION_AR`, `IS_FREE_HOLD_AR` (100% masked), `MASTER_PROJECT_AR` (100% null)
- Null the masked cells in the partially-damaged columns and count them per column. Measured masked
  cell counts in retained `_AR` columns: `PROJECT_AR` 21, `NEAREST_METRO_AR` 2, `NEAREST_MALL_AR` 0,
  `NEAREST_LANDMARK_AR` 0 — **23 cells total (0.5%)**. This is a rounding error, not a migration.
- Retain intact: `AREA_AR`, `PROP_TYPE_AR` (100% Arabic), `PROP_SUB_TYPE_AR` (99.6%), `USAGE_AR` (99.5%)

### 2.2 Amount mismatch is a reconciliation check, not a short-term signal — defect 2

The draft derived `is_short_term` from `duration_days < 300` and presented the
`CONTRACT_AMOUNT`/`ANNUAL_AMOUNT` mismatch as its motivation. The causal story is inverted.

Any multi-period contract legitimately has `CONTRACT_AMOUNT ≠ ANNUAL_AMOUNT`; the 91.7% of rows where
they are equal are all single-year leases. The ratio *is* duration in years (corr 0.9956). Verified:
**zero** contracts with `duration < 300` days have ratio > 1.5. Conversely 82 short contracts have
ratio < 0.5 (sub-annual, priced monthly or quarterly — ratios 0.25, 0.333).

Treatment: `is_short_term` derives from `duration_days` alone, which is correct and independent. The
amount pair becomes a **free integrity check** — the gateway hands us two independent measurements of
the same quantity, so their disagreement is a quarantine signal:

```
implied_years = contract_amount / annual_amount
declared_years = duration_days / 365.25
VIOLATION when abs(implied_years - declared_years) / declared_years > 0.05
```

This holds to 0.0065 years on the current payload, so the 5% band is generous and will not fire on
healthy data.

The 300-day threshold is retained for `is_short_term` but is reconciled against the two existing
thresholds in the codebase — `VALIDATION_THRESHOLDS` `min_contract_days: 30` / `max_contract_days: 730`
(`lib/config.py:102`) and the 180/365 buckets in `_add_contract_duration`
(`lib/transform/enrichment.py:174`). All three now read from one constant in `lib/config.py`.

### 2.3 Multi-property blocks are real — defect 3

**Correction to an earlier reading of this payload.** An initial pass concluded that
`TOTAL_PROPERTIES` was not a block size (a composite key matched group size in only 286/1202 rows).
That measurement was wrong: the key used omitted `TOTAL_PROPERTIES`, which merged blocks that
differed only in that column. Re-tested with `TOTAL_PROPERTIES` included, it matches the true group
size for **3046/3378 groups covering 3291/4306 rows (76.4%)**, and every large block is exact —
87, 26, 25, 19, 18, 12, 10.

So multi-property contracts **are** present and **do** warrant a contract-level rollup. But the
draft's proposed key is still unusable, for two independent reasons:

- `CONTRACT_NUMBER` is **100% null** — 0 non-null of 4306 in `rent_contracts_20260917.csv`, and 0 in
  every one of the five daily files (1714 + 886 + 4589 + 4580 + 4306 = **16,075 rows**)
- `PROPERTY_ID` is **constant `0`** — 1 distinct value in every file, so `property_ids: list[int]`
  would be `[0, 0, …]`

Treatment: roll up on a **derived key**, and reconstruct blocks from what the payload does carry.
Candidate keys, measured on groups where `TOTAL_PROPERTIES > 1`:

| Candidate key | Groups | Exact | Exact % | Rows covered |
|---|---|---|---|---|
| `area+subtype+start+amount+area` | 3378 | 3046 | 90.2% | 76.4% |
| `start+end+amount` | 2581 | 2136 | 82.8% | 59.3% |
| `start+end+amount+version` | 2753 | 2343 | 85.1% | 64.1% |
| `start+end+amount+version+usage` | 2821 | 2430 | 86.1% | 66.1% |
| **`start+end+amount+version+usage+area`** | 3208 | 2963 | **92.4%** | **78.5%** |

Restricting to `TOTAL_PROPERTIES > 1` and keying on `(start_date, end_date, annual_amount,
version_en)`: **82/84 groups reconstruct exactly, covering 498/508 rows (98%)**. The 2 failures are
both *over*-counts (6 rows against a declared 2, and 4 against a declared 2) — two genuinely
distinct contracts that share start date, end date, amount and version, and are therefore
**irreducibly ambiguous** without a contract number.

Contiguous-run grouping was also tested and is worse (468/1198 rows): DLD interleaves rows, and
genuine blocks span up to 12 distinct `AREA_EN` values, so run boundaries do not align with contract
boundaries.

Rows with `TOTAL_PROPERTIES == 1` (3798, 88.2%) are already one contract per row and need no
rollup. Blocks where observed rows fall *below* the declared size (e.g. Al Karama, declared 3,
observed 1) are partial captures — the window saw only part of the contract. These are retained and
flagged, never dropped.

### 2.4 The area unit is square feet, not square metres — defect 5

The draft named the field `actual_area_sqm` and the derived metric `rent_per_sqm`. **This is a
10.76× corruption bug.** Verification: residential flats in the 400–1200 band have median
`ACTUAL_AREA` 744 and median `ANNUAL_AMOUNT` AED 60,000 → **80.6 AED/sqft**, squarely Dubai market.
Read as m² the same numbers become 868 AED/sqft, roughly 10× market. Independent confirmation:
`lib/classes/property_usage.py:81` has always aliased the metric **`avg_area_sqft`**.

Treatment: `actual_area` and `rent_per_sqft`, matching `lib/config.py:92` and
`lib/transform/enrichment.py:96` (`price_per_sqft`).

The draft's guard `0 < area < 5000` is also aimed the wrong way. It quarantines 14/4306 rows
(0.3%) — nearly nothing — while the actual defect is at the **low** end: median area is 75, minimum
is 1.0, and **3860/4306 (89.6%)** are below 200. The `<200 → no PSF` band in
`lib/transform/enrichment.py:83` is not a workaround, it is the truthful description of this feed.
This spec does not attempt to fix that; it records it, because the consequence for reporting is
material: **the PSF metric rests on 443 rows (10.3%) of this window**, and `IMPLEMENTATION_PLAN.md:31`
should not expect a stable `avg_psf 45-110` from a 1-day sample.

Upper bound is `lt=50000` per `VALIDATION_THRESHOLDS["max_property_size"]` (`lib/config.py:93`),
catching 3 rows.

### 2.5 Constant-column and flag collapse — defects 4 and 6

Both draft treatments are **confirmed correct** and adopted as written.

- `TOTAL` is dropped as batch metadata (single distinct value `4306`).
- `TOTAL_PROPERTIES` is retained as a plain attribute with **no grain meaning** — explicitly
  documented as such so no future reader mistakes it for a block size.
- `is_free_hold: bool` is derived from the `IS_FREE_HOLD` integer. The 1:1 correspondence with
  `IS_FREE_HOLD_EN` is verified with zero exceptions, so the text column is retained for lineage but
  is no longer load-bearing.

---

## 3. Architecture

### 3.1 Two keys, because the data supports two and not one

The endpoint exposes no contract number, so a stable unique key cannot be manufactured. Profiling
five candidate business keys:

| Candidate key | Distinct | Duplicates |
|---|---|---|
| `AREA_EN, START_DATE, ANNUAL_AMOUNT` | 3171 | 1135 |
| `+ PROP_SUB_TYPE_EN, ACTUAL_AREA` | 3378 | 928 |
| `+ RN` | 4306 | 0 |

Only `RN` — the gateway's per-response row ordinal — reaches full uniqueness, and a hash including it
changes if the gateway reorders rows, so it is not stable across re-runs. Therefore:

| Field | Built from | Cardinality | Purpose |
|---|---|---|---|
| `row_hash` | `area_name_en, ejari_property_sub_type_en, contract_start_date, annual_amount, actual_area` | 3378/4306 | Stable fingerprint for cross-day deduplication |
| `record_id` | `row_hash` + `RN` | 4306/4306 | Surrogate key for the 1-row-per-CSV-row fact grain |

`record_id` is unique within a file and **not** stable across re-runs. This is stated in the
docstring, not hidden. `IMPLEMENTATION_PLAN.md:82`'s reconciliation
(`count(fact) == sum daily CSVs`) remains verifiable through `record_id`. The alias at
`lib/transform/rents_transformer.py:53` currently points `CONTRACT_NUMBER` → `contract_id`; that
alias is repointed to `record_id`, and `contract_id` is removed from
`DATA_QUALITY_RULES["required_fields"]` (`lib/config.py:190`) because it is 100% null upstream.

### 3.2 Contract-level rollup — and the sum that must not be summed

`SilverContractGroup` is built for the 508 multi-property rows, keyed on the derived tuple from
§2.3. Its identity fields are replaced, because the drafted ones are empty upstream:

| Drafted | Status | Replacement |
|---|---|---|
| `contract_number: int` | unusable — 100% null | `group_id: str` — sha256 of the §2.3 key, 16 hex chars |
| `property_ids: list[int]` | unusable — `PROPERTY_ID` constant 0 | `record_ids: list[str]` — the member rows' `record_id` values (§3.1) |
| `total_properties: int` | **valid** — matches block size 76.4% overall, exactly on all large blocks | kept as-is |
| `total_annual_amount: Decimal` | **valid field, wrong name and wrong aggregate** | see below |
| `usages: list[str]` | valid | kept |
| `version_type`, `start_date`, `end_date`, `area_en` | valid | kept, renamed to canonical `property_usage_en` / `contract_start_date` / `contract_end_date` / `area_name_en` per §3.3 |

**`total_annual_amount` must not be a sum.** Within the 87-property Muhaisanah Second block there is
**exactly one distinct `ANNUAL_AMOUNT` (AED 3,053,700)** repeated across all 87 rows. The duration
reconciliation from §2.2 holds *inside* the block (declared 0.9966 years vs implied 1.0), which
proves the amount is **contract-level, repeated per property row** — not per-property. A naive
`sum()` returns **AED 265,671,900**, an 87× overstatement of a labor camp.

The asymmetry is the trap and it must be documented on the field:

- `annual_amount` — contract-level, **deduplicated** (take the single distinct value; assert exactly
  one, else raise a violation). Never summed.
- `actual_area` — genuinely per-property, **summed**. The same block sums to 1,742.61 sqft, a
  plausible camp footprint.

`observed_property_count` is emitted alongside the declared `total_properties` so partial captures
(declared 3, observed 1) are visible rather than silent. A group where
`observed_property_count > total_properties` is a violation — it means the §2.3 key merged two
contracts, which is exactly the 2-of-84 irreducible case. Those 10 rows are quarantined and
counted, not dropped.

The rollup is emitted as a **separate DataFrame** returned on `SilverContractResult.groups`. The
property-level `frame` is untouched, so ADR-02's 1-registration grain and the
`count(fact) == sum daily CSVs` reconciliation are both preserved. Labor Camps bulk blocks still get
excluded downstream by the existing `is_bulk_registration` flag
(`lib/transform/enrichment.py:235`); the rollup makes them legible, it does not replace that filter.

### 3.3 Canonical column names — no translation layer

The model uses the **existing pipeline snake_case names** (`area_name_en`, `annual_amount`,
`actual_area`, `contract_start_date`, `property_usage_en`, …), not the draft's
`area_en`/`property_type`. These names are load-bearing: 11 are referenced across
`lib/analysis/*.sql` (4 occurrences each) and consumed by `lib/classes/property_usage.py` and
`lib/classes/market_analytics.py`. Reusing them means the contract drops into the pipeline with **no
rename step and no downstream breakage**. The draft's naming would have required a translation layer
and broken every one of those references.

### 3.4 Violations are collected, never raised

Per ADR-03 (`docs/IMPLEMENTATION_PLAN.md:15`, fail-open at every stage) the model never raises on a
bad row. Violations are collected onto the instance and surfaced in aggregate:

```
SilverContractResult(
    frame: pl.DataFrame,          # typed, canonical, 1 row per registration
    quarantined: pl.DataFrame,    # rows failing a hard rule, retained for audit
    violation_counts: dict[str, int],
)
```

This preserves the 1-registration grain (ADR-02), never drops a row the gateway emitted, and keeps
every violation queryable in DuckDB rather than vanishing into a log line. Per-row pydantic
validation is affordable: measured **548,000 rows/sec**, so a 90-day backfill of ~150k rows costs
~0.3s. The ADR-01 streaming constraint is not a reason to keep this vectorised.

### 3.5 Responsibility split

The per-row model and the DataFrame-level validator must not both check ranges, or thresholds drift
apart. Each keeps only what it alone can do:

| Concern | Owner |
|---|---|
| Types, coercion, derivation, per-row ranges, encoding masks | `silver_contract.py` |
| Column presence, null rates, IQR outliers, empty-frame, row-count reconciliation | `validators.py` |

`validators.py` sheds `_validate_rent_amounts`, `_validate_property_sizes` and
`_validate_data_types`; both modules read thresholds from `VALIDATION_THRESHOLDS`
(`lib/config.py:86`), so there is exactly one source of truth. `validators.py` gets **smaller**.

### 3.6 Silent drops become recorded drops

With no `model_config`, the draft silently discarded 14 columns. The model instead sets
`ConfigDict(extra="ignore", frozen=True)` and exposes a module-level `dropped_columns` tuple naming
every discarded column and its reason, so a reader of the Silver parquet can tell the difference
between "column intentionally removed" and "column silently lost".

---

### 3.7 Gold business marts — and two prerequisites they inherit

The proposed marts (`DimArea`, `DimPropertyType`, `FctContract`, `AggAreaRentStats`,
`AggMetroPremium`, `AggMonthlyRegistrations`, `AggProjectRentStats`) are the right *shapes*, but
**every one of the eight proposed key columns is degenerate** — a single distinct value, so the
dimensions would materialise one row each and the fact's foreign keys would join to nothing:

| Proposed key | Distinct | Replacement that works |
|---|---|---|
| `AREA_ID` | **1** | `area_name_en` — **161** distinct, 0 null |
| `EJARI_PROPERTY_TYPE_ID` | **1** | `property_type_en` (5) + `property_sub_type_en` (29) |
| `PROPERTY_USAGE_ID` | **1** | `usage_en` — 5 distinct |
| `EJARI_PROPERTY_SUB_TYPE_ID` | **1** | as above |
| `CONTRACT_NUMBER` | **1** (all null) | `record_id` (§3.1) |
| `PROPERTY_ID` | **1** (const 0) | `record_id` |
| `VERSION_NUMBER` | **1** | `version_en` (New / Renewed) |
| `MASTER_PROJECT_EN` | **1** (all null) | drop from `AggProjectRentStats`; `project_name_en` has 696 distinct (2727 null) |

Surrogate keys are generated as `ROW_NUMBER() OVER ()` — which is what
`lib/analysis/dim_location.sql` already does, so the marts **extend the existing star rather than
replace it**.

Three further corrections:

- **`rent_per_sqm` → `rent_per_sqft`**, for the third time (§2.4). It appears in `FctContract`,
  `AggAreaRentStats` (`avg`, `p10`, `p90`) and `AggMetroPremium`. Read as m² every figure is 10.76×.
- **`DimArea.nearest_metro` must move off the dimension.** 77 of 161 areas have more than one
  nearest metro — Marsa Dubai has 8, Al Safouh Second 6, Palm Jumeirah 5. Metro varies *within* an
  area, so putting one on `DimArea` forces an arbitrary pick. It belongs on the fact, or in its own
  `DimMetro` (57 distinct metro names, 599 null cells).
- **Every aggregate must carry `n`.** The proposed models omit it, but `n` is what makes a median
  defensible: `docs/IMPLEMENTATION_PLAN.md:46` requires `HAVING count >= 10`, and ADR-04 requires
  medians precisely because the mean is not trustworthy. The existing
  `output/area_median_index_20260913-17.csv` already carries an `n` column — the marts must too.

**`AggMetroPremium.premium_vs_city_avg_pct` is not viable as drafted.** It chains a mean-based PSF
into a comparison against a city-wide mean, over a base that is 10.4% of rows. With the `n >= 10`
gate applied to PSF-eligible rows, only **11 of 161 areas** and **11 of 54 metros** clear the bar.
`p10`/`p90` on `n = 10` is noise, and a "premium vs city average" computed on 11 metros whose city
average is itself polluted by bulk registrations is the precise artifact this project exists to
eliminate. If a metro premium is wanted, it must be median-based, gated at a stated minimum `n`,
and published with that `n` visible.

**`AggMonthlyRegistrations` has two problems.** Monthly buckets over a 1-day snapshot (and a 5-day
backfill) are not a trend. And `total_annual_value` summing `annual_amount` across property rows
re-introduces the §3.2 landmine — a 87-property block contributes 87× its contract value. Monthly
value must aggregate over **deduplicated contracts**, not property rows.

**Two prerequisites, because the marts would otherwise inherit a leak:**

1. **A second PSF computation site.** `lib/classes/property_usage.py:90-96` re-derives
   `annual_amount / actual_area` guarded only by `actual_area > 0`, bypassing the `>= 200` guard at
   `lib/transform/enrichment.py:90`. The shipped `output/property_usage_20260913.csv` still reports
   Residential `avg_psf 4691.52` / `median_psf 995.27` — both far outside the 20–500 band at
   `lib/config.py:96`, and the exact value `docs/IMPLEMENTATION_PLAN.md:31` says must never ship. The
   guard is correct but never reaches the report. Fix: `PropertyUsage` reads the enriched
   `price_per_sqft` instead of recomputing it. One place, not two.
2. **The Phase 2 gate is not met.** `docs/IMPLEMENTATION_PLAN.md:83` requires medians within
   20k–500k and no `>1M` leak. Measured on the shipped `area_median_index_20260913-17.csv`:
   121/121 areas clear `n >= 10`, but **26 areas have `max_rent > 1M`** (up to AED 4.3M),
   **`median_rent` reaches AED 590,000** (Al Goze Industrial First — Labor Camps), and **74 of 121
   areas show a mean/median skew above 20%** with `mean_rent` published beside `median_rent` in the
   same file. The `is_bulk_registration` flag and the Hotel / Labor Camps / Virtual Unit exclusion
   from `docs/IMPLEMENTATION_PLAN.md:46` were never applied to this artifact.

Both are small, localised fixes. Neither is in scope for the Silver contract, but the marts are not
safe to build until they land.

## 4. File changes

| File | Change |
|---|---|
| `lib/classes/silver_contract.py` | **New.** `SilverRentContract`, `SilverContractResult`, `to_silver(df) -> SilverContractResult`, `ARABIC_DROPPED`, `dropped_columns`, violation constants |
| `lib/classes/validators.py` | Remove pydantic import and `BronzeRentContract`; shed the three per-row range checks; keep aggregate checks |
| `lib/transform/rents_transformer.py` | `encoding="utf-8-lossy"` → strict `"utf-8"` (`:22`); repoint `contract_id` alias to `record_id` (`:53`) |
| `run_etl_pipeline.py` | Narrow `except Exception` at `:139` so an import failure can no longer masquerade as a passing gate; invoke `to_silver()` in `transform_rents()`, overwriting the same parquet |
| `lib/config.py` | Remove `contract_id` from `required_fields` (`:190`); single `SHORT_TERM_DAYS = 300` constant reconciling the 180/365/300 thresholds |
| `tests/test_silver_contract.py` | **New.** Import smoke test, construction test, coercion counts, key cardinality, area-unit guard, reconciliation check |
| `pyproject.toml` | pydantic stays (added in the uncommitted change, `:25`) |
| `requirements.txt` | Add `pydantic>=2`; remove orphan `psutil` (in `requirements.txt` but not `pyproject.toml`) |
| `docs/IMPLEMENTATION_PLAN.md` | Add ADR-06 (pydantic); record the 10.4% PSF-eligible rate against the P0 exit gate at `:31`; reconcile Phase 0/2 status against what already shipped |
| `README.md` | Correct the pydantic description at `:43` — it is a Silver contract model, not "typed analysis result models" |
| `lib/classes/property_usage.py` | Read the enriched `price_per_sqft` instead of re-deriving PSF at `:90-96` (Gold prerequisite 1) |
| `lib/analysis/area_median_index` | Apply `is_bulk_registration` and the Hotel / Labor Camps / Virtual Unit exclusion; publish `median_rent` as the headline (Gold prerequisite 2) |

No new artifact is produced. The typed frame overwrites the existing
`output/rent_contracts_{date}.parquet`, so Phase 1.2's separate `rents_silver.parquet` consolidation
remains a distinct, later task.

---

## 5. Gates

Verification is runnable and each command is falsifiable:

1. `uv run pytest -q` green — including a test that **only imports** `silver_contract`, which is the
   check that would have caught the original `NameError`
2. `SilverRentContract(**row)` constructs successfully for a fixture row — the check that catches the
   required-but-derived field bug
3. `count(record_id) == 4306` on `output/rent_contracts_20260917.csv`; `count(row_hash) == 3378`
4. `grep -rn "_sqm" lib/ tests/` returns nothing — guards the unit regression
5. Flats in the 400–1200 band assert `60 <= median(rent_per_sqft) <= 120` — the check that would have
   caught the m²/sqft confusion
6. `implied_years` vs `declared_years` agree within 5% on the fixture
7. `to_silver()` raises nothing on the full 4306-row file; `violation_counts` matches expectation:
   `annual_amount < 10000` → 9, `annual_amount > 5,000,000` → 1, `actual_area < 200` → 3860,
   `actual_area > 50000` → 3, masked Arabic cells → 23
8. `count(record_id)` in the produced parquet equals the input CSV row count
9. Rollup: `SilverContractResult.groups` reconstructs 82/84 declared blocks exactly; every group
   satisfies `observed_property_count == total_properties` or is counted as a violation
10. Rollup: `annual_amount` is asserted to have exactly one distinct value per group. A test
    reproduces the 87-property block and asserts the rollup reports **AED 3,053,700**, not
    **AED 265,671,900**
11. No `rent_per_sqm` in `lib/`, `tests/` or `lib/analysis/*.sql`; Gold prerequisite 1 verified by
    `property_usage_*.csv` Residential `median_psf` landing in 20–500 or null
12. `area_median_index` has 0 rows with `max_rent > 1_000_000` and `median_rent` within 20k–500k

---

## 6. Rejected alternatives

| Alternative | Why rejected |
|---|---|
| Attempt cp1252→utf-8 repair on Arabic | Source bytes do not exist; upstream word-mask. Measured 0 recoverable cells |
| Group duplicates by `CONTRACT_NUMBER` | Column is 100% null across all 16,075 rows in 5 daily files |
| Use `TOTAL_PROPERTIES` as a plain non-key attribute | It **is** the block size — 76.4% exact, all large blocks exact. It is the rollup key (§3.2) |
| Group by contiguous row runs | Worse: 468/1198 rows. DLD interleaves rows and blocks span up to 12 `AREA_EN` values |
| Key `DimArea` / `DimPropertyType` / `FctContract` on the `*_ID` columns | All 8 are single-valued; dimensions would be 1 row each |
| `DimArea.nearest_metro` | 77/161 areas have >1 metro (Marsa Dubai has 8) — not 1:1, belongs on the fact |
| `total_annual_amount` as a `sum()` | 87× overstatement — AED 265,671,900 vs the real AED 3,053,700 |
| `premium_vs_city_avg_pct` as drafted | Mean-of-polluted-base over 11 qualifying metros; p10/p90 on n=10 is noise |
| `AggMonthlyRegistrations.total_annual_value` summing property rows | Double-counts multi-property blocks by the §3.2 factor |
| Use one hash for both dedup and grain key | No candidate is both stable and unique. Split into `row_hash` + `record_id` (§3.1) |
| Name the model fields `area_en` / `actual_area_sqm` | Breaks 11 SQL column references and both analytics classes; `sqm` corrupts values 10.76× |
| Raise on validation failure | Contradicts ADR-03 fail-open; loses field-level detail |
| Vectorised-only validation (no pydantic) | Measured 548k rows/sec — per-row costs ~0.3s on a 90-day backfill. Not a real constraint |
| Keep the existing `avg_psf` in `property_usage_*.csv` | Bypasses the `>= 200` guard; ships `avg_psf 4691` (§3.7) |
| Write a separate `*-silver.parquet` | New artifact for no gain; Phase 1.2 already owns consolidation |

---

## 7. Risks

| Risk | Mitigation |
|---|---|
| `utf-8` strict read fails on a future non-UTF-8 payload | The current file validates clean. If it ever fails, the failure is loud and quarantines the day rather than silently mangling Arabic |
| `record_id` not stable across gateway reordering | Documented in the docstring. Cross-day dedup uses `row_hash`, which is stable. `count(fact) == sum CSVs` is unaffected |
| Dropping 3 Arabic columns is irreversible | They are 100% null or 100% masked; there is no data to lose. Recorded in `dropped_columns` with reasons |
| `validators.py` loses range coverage if the split is botched | Gates 1 and 7 assert both modules still report; thresholds come from one dict |
| The 10.4% PSF-eligible rate is read as a regression | Recorded against `IMPLEMENTATION_PLAN.md:31` so the P0 gate's `avg_psf 45-110` expectation is not applied to a 443-row sample |

---

## 8. Out of scope

Phase 1.1 backfill and Phase 1.2 Silver consolidation to `rents_silver.parquet`.

**Building** the Gold marts is out of scope — §3.7 specifies their corrected shape so the work is not
guessed at later, but the marts are a separate spec. What *is* in scope are the two prerequisites
they depend on, because both are one-line-scale fixes to code that currently ships a known-bad
number: the duplicate PSF site at `lib/classes/property_usage.py:90-96` and the bulk leak in
`area_median_index`. No new mart, dimension or view is created here, and no star-schema table
definition changes.

`Area Median Index` and `Standard Lease Index` already have artifacts in `output/` — 121 areas and
one standard-lease median respectively — so Phase 2 is partly built. `docs/IMPLEMENTATION_PLAN.md`
still lists Phase 0 and Phase 2 as pending; that is reconciled in the ADR/plan update, not rebuilt
here.

---

*Trace: `docs/IMPLEMENTATION_PLAN.md:9` ADRs · `docs/EXPERT_REVIEW_AND_ROADMAP.md:51` Phase 0 ·
verified against `output/rent_contracts_20260917.csv` (4306 rows).*

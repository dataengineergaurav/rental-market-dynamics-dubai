# ADR-08: Bronze / Silver / Gold published as separate layer releases

**Status:** Accepted · **Date:** 2026-10-06

## Context

The project described itself as a bronze → silver → gold pipeline, but only two artifacts actually
left the repo: the raw daily CSV (`release-YYYY-MM-DD`) and a single weekly DuckDB
(`release-week-YYYYWww`) that mixed a `fact_rental_contract` **table** with the `gold_*` **views**
in one file. The validated Silver parquet (`rent_contracts_YYYYMMDD.parquet`) was local-only and
never published.

That left the middle layer unreleasable and the top layer entangled with it: a consumer wanting the
area medians had to download the whole fact table, and there was no way to release a corrected view
without redistributing the data. The Silver design spec §3.7 had already specified the corrected
mart shapes (`DimArea`, `DimPropertyType`, `FctContract`, `Agg*`) but explicitly deferred building
them.

## Decision

Publish three layers, each with its own tag and file:

| Layer | Artifact | Tag | Contents |
|-------|----------|-----|----------|
| Bronze | `rent_contracts_YYYYMMDD.csv` | `release-YYYY-MM-DD` | raw daily extract (unchanged) |
| Silver | `silver_YYYYWww.duckdb` | `release-silver-YYYYWww` | tables `DimArea`, `DimPropertyType`, `DimMetro`, `FctContract`, `_meta` |
| Gold | `gold_YYYYWww.duckdb` | `release-gold-YYYYWww` | seven views over the Silver tables |

- **Two DuckDBs per week.** Silver holds the data (tables); Gold holds definitions (views). Gold
  views reference the Silver database through a fixed catalog alias `silver`, so a Gold file is
  inert until Silver is attached under that alias. `lib/analysis/layers.connect_gold` centralizes it.
- **Normalized Silver.** `weekly_pool.pool_and_enrich` produces the pooled, deduped, enriched frame;
  `silver_layer` normalizes it into natural-key dimensions and a contract-grain fact. The spec's
  §3.7 corrections are applied: keys are natural, not the single-valued upstream `*_ID` columns;
  `nearest_metro` lives on the fact (`metro_key` → `DimMetro`), not on `DimArea`; every aggregate
  carries `n`.
- **Weekly cadence.** The two freshness gates and cross-file dedup (ADR-07) still run before either
  layer is written.

## Rationale

- **The layers have different change rates.** Views change when an analysis changes; tables change
  when the data changes. Shipping them together forced a data download to get a view fix. Splitting
  them lets Gold be re-released alone.
- **A DuckDB view is a definition, not a copy.** Keeping Gold view-only means the same fact is not
  duplicated on disk, and a Gold re-release is tiny. The cost — a mandatory `ATTACH` — is paid once
  in `connect_gold`.
- **Natural keys, per the spec.** All eight upstream `*_ID` columns are single-valued, so keying on
  them would produce one-row dimensions. `nearest_metro` is 1:many with area (77 of 161 areas carry
  more than one), so it cannot be an area attribute.
- **Enrichment stays in Silver.** The Gold aggregates need `is_bulk_registration`, `contract_duration_days`
  and the tier columns; computing them once in the Silver build keeps the fact self-sufficient.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| One DuckDB with Silver tables + Gold views | The entanglement the split exists to remove; a view fix re-ships the fact |
| Keep `release-week-YYYYWww` as the single tag | Gives up layer-typed releases and the whole point of the change |
| Gold as tables (materialized marts) | Duplicates the fact and enlarges the artifact; views suffice for the shipped marts |
| Key dims/fact on the upstream `*_ID` columns | All are single-valued in the payload — one-row dimensions (spec §3.7) |
| Put `nearest_metro` on `DimArea` | 77/161 areas have >1 metro; would force an arbitrary pick |
| Build the fact by running `to_silver()` on the pool | Introduces quarantine into the weekly grain and changes row counts the gates depend on; the pool already re-derives enrichment |

## Consequences

- **Gold consumers must attach Silver.** `connect_gold(gold_path)` does it; a bare
  `duckdb.connect(gold)` and `SELECT * FROM gold_area_median` fails with `Catalog "silver" does not
  exist`. This is documented in the module docstrings, RELEASE_NOTES and the ADR.
- `build_weekly_duckdb` is now an orchestrator over `build_silver_duckdb` + `build_gold_duckdb`; the
  pool/dedup/gate helpers moved to `weekly_pool` and the DDL to `silver_layer`/`gold_layer`, with the
  old names re-exported for compatibility.
- `metro_volume.py` now reads the Gold DB (attaching Silver) instead of the removed weekly DB.
- The pre-2026-10 `release-week-YYYYWww` artifact is superseded by the Silver + Gold pair.
- **Known gap:** the Agg views use Silver's `FctContract.price_per_sqft`, which comes from
  `enrichment.py`. The spec's preference — read the Silver *contract*'s `rent_per_sqft` and delete
  the second PSF site — is not met, because the weekly build pools raw CSVs rather than the daily
  Silver parquet. Closing that requires feeding `to_silver()` output into the weekly pool (and
  reconciling quarantine with the row-count gates); it is a follow-up, not part of this ADR.

## Verification

- `tests/test_build_silver_duckdb.py` — table set, contract grain == deduped rows, lowercase column
  names, key resolution, dimension deduplication.
- `tests/test_build_gold_duckdb.py` — exactly the seven views and no tables; views unresolvable
  without Silver; `connect_gold` resolves every view; every aggregate carries `n`; the
  `gold_area_median` six-column contract is preserved.
- `tests/test_weekly_dedup_and_gate.py` and `tests/test_metro_volume.py` updated to the layered
  paths, keeping every existing dedup/gate guarantee.
- Real-data build of 2026-09-29..2026-10-03: 21,847 pooled → 21,719 contracts; Silver dims
  (177 areas, 55 property types, 56 metros); Gold resolves all seven views.

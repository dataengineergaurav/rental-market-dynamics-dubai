# UAE / Dubai rental market — domain pack

Grounding for interpreting this repo's outputs. Regulatory specifics change: **verify current
figures and rules against official DLD/RERA sources before quoting them.**

## The data source — Ejari

- **Ejari** is the mandatory tenancy-contract registration system operated under the Dubai Land
  Department (DLD) / RERA. New and renewed tenancy contracts in Dubai are registered; each registered
  contract is one record with a contract number (`contract_id` here).
- A record is a **registered lease contract**, not a listing and not a sale. `annual_amount` is the
  contract's annual rent; `contract_amount` the total contract value.
- **Ejari is not the sales market.** Title deeds and sales transactions are out of scope. Do not
  infer yields/capital values from this feed alone (a yield needs sale prices this pipeline does not
  carry).
- Related but distinct datasets: DLD sales transactions and Dubai Pulse open data (rentals and
  transactions), RERA Rental Index. Use these as *external benchmarks*, not as substitutes.

## What a row means (read this before interpreting)

- **Registration date vs start date.** `contract_registration_date` is when Ejari recorded it;
  the move-in can be earlier/later. Trend on registration date for "market activity"; treat the
  start date as tenancy timing.
- **Contract types.** Renewals, new leases, and multi-property blocks co-exist. `TOTAL_PROPERTIES`
  marks blocks; `is_bulk_registration` flags suspiciously repeated area+amount clusters (e.g. one
  registration covering many units) and is excluded from rent medians.
- **Duration.** `contract_duration_days`; short-term (<6 months) is a different product from a
  standard annual lease. `gold_standard_lease` isolates the 180–365 day band.
- **Geography.** `area_name_en` (community), `project_name_en` (building/project), `master_project_en`
  (master development), `nearest_metro_en`.

## Area taxonomy (this repo)

`lib/config.py` maps communities to four tiers; unknown areas default to **Mid-Tier**:

- **Premium** — Downtown Dubai, Dubai Marina, Palm Jumeirah, Emirates Hills, Business Bay.
- **Mid-Tier** — JVC, JVT, Dubai Sports City, Motor City.
- **Budget** — International City, Deira, Bur Dubai.
- **Emerging** — Dubai South, Dubailand, Dubai Production City.

Tiers are a coarse prior for segmenting, not a rank of investment quality. RERA/DLD periodically
restructure community names and zones — when adding an area, add a test (see
`tests/test_p0_gates.py::test_enrichment_tier_mapping`).

## Seasonality and cycles

- **School-year / summer turnover.** A large share of tenancies follow the academic calendar and
  the summer expat relocation cycle (roughly May–September), so registration volume peaks then.
- **Ramadan** typically slows activity during its weeks.
- **Read volume vs price separately.** `AggMonthlyRegistrations` is volume (`n_contracts`,
  `total_annual_value`). A volume spike is not a rent-level move — check per-area medians for levels.
- **New-supply waves** (handovers in an emerging community) raise contract counts even when rents
  soften, because supply and demand both move.

## Regulation that shapes renewals (verify current)

- **RERA Rental Index / Smart Rental Index** sets reference rents used to decide permissible
  renewals increases.
- **Rent-increase caps** are tiered by how far the current rent sits below the index benchmark
  (larger gaps permit larger increases; small gaps permit none). Decree/Law No. 43 of 2013 is the
  governing framework.
- **Notice periods** (commonly 90 days) apply to changes at renewal — check the current rule.
- **Law No. 26 of 2007** (as amended) is the core tenancy law.

Implication for this data: renewals may show *constrained* rent movement relative to the index, so
a flat median can reflect regulation rather than a soft market. Never present a cap as a market
signal without that caveat.

## Reading the metrics

- **Median rent** — typical contract value in the group; the headline.
- **PSF (AED/sqft)** — comparability across unit sizes; only meaningful when area ≥ 200 sqft and
  inside the shared band.
- **Metro premium** (`AggMetroPremium`) — a metro's median rent vs the city median, as a percent.
- **Luxury flag** — top ~25% by PSF or top ~20% by rent (`MARKET_METRICS` in `lib/config.py`).

## When asked to brief on "the market"

State the as-of date (`_meta.data_through`), the sample size behind each number, and whether the
figure is a level, a volume, or a spread. Separate structural drivers (supply handovers, regulation)
from seasonal ones. Where an external benchmark is used, cite it and its date.

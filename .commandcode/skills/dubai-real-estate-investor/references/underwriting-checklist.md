# Underwriting checklist — Dubai rental investment

Use with the `dubai-real-estate-investor` skill. Every external figure needs `[source, date]`.
Never fill a line from memory — if the input is missing, write "missing" and say what it blocks.

## The return bridge

The repo gives the **rent** leg only. The rest must be supplied.

```
Gross yield   = annual rent / capital value            <- rent: repo; value: EXTERNAL
Net yield     = (annual rent - opex) / total cost      <- opex + cost: EXTERNAL
              opex  = service charge + maintenance + management + expected vacancy
Total cost    = capital value + DLD transfer + agency fee + other closing costs
Cash-on-cash  = (net rent - debt service) / equity     <- financing: EXTERNAL
```

If capital value is absent, stop: report the rent evidence and state that no yield is computable.

## Checklist

**Income (from this repo)**
- [ ] `gold_area_median` — median rent + `n` for the area; `n >= 10` or say the sample is thin.
- [ ] `AggAreaRentStats` — `p10`/`p90` spread, not just the median.
- [ ] `AggMetroPremium` — premium vs city median (context, not a valuation).
- [ ] `AggProjectRentStats` — building-level median where the unit sits.
- [ ] `gold_standard_lease` — the standard annual (180–365 day) benchmark.
- [ ] `_meta.data_through` — the as-of date; stale means ask `ci-triage`.

**Cost (external, verify current)**
- [ ] Capital value / sale price (DLD sales / Dubai Pulse).
- [ ] Service charge AED/sqft/yr.
- [ ] Maintenance + management % of rent.
- [ ] Expected vacancy (weeks/yr).
- [ ] DLD transfer + agency + other closing costs.
- [ ] Financing: rate, LTV, term (if levered).

**Risk**
- [ ] Supply pipeline / imminent handovers in the area.
- [ ] Regulatory: current RERA increase caps / index (verify; do not quote from memory).
- [ ] Liquidity: depth of the resale market for this asset class.
- [ ] Concentration / single-tenant dependence.
- [ ] Currency / repatriation for a non-AED investor.

**Data quality (hand to `data-engineer-expert` if in doubt)**
- [ ] Exclusions applied (Hotel / Labor Camps / Virtual Unit / `is_bulk_registration`).
- [ ] PSF figures inside the shared band and above the 200 sqft floor.
- [ ] Claim cited to a view + `n`, not a raw row.

## Falsifier discipline

Every brief ends with: *"this thesis is wrong if <observable>."* Examples: rents in the area fall
below the current `p10` for two consecutive months; a scheduled supply wave lands; the index
re-bases downward at renewal.

## Common traps

| Trap | Why it misleads |
|------|-----------------|
| Gross yield from a round-number price | Hides opex; overstates return 20–40% typically |
| Area median as *this unit's* rent | Median is a group statistic; the unit may sit at `p10` |
| Volume spike read as rent growth | `AggMonthlyRegistrations` counts contracts, not prices |
| Regulated renewal cap as demand | It reflects the index, not the market's willingness to pay |
| Bulk-registration rows inflating a median | Excluded by contract — verify they were |

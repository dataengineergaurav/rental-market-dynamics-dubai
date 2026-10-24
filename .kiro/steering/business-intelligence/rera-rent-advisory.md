---
inclusion: manual
name: rera-rent-advisory
description: Descriptive reference on the Dubai RERA rent-increase framework — Decree 43 bands, the RERA Rental Index inputs, and how New vs Renewed caps differ. Load to explain how a lawful rent increase is framed. NOT legal advice and does not recommend specific increases.
owner: "@dataengineergaurav"
department: business-intelligence
last-verified: 2026-10-24
---

# RERA Rent Advisory (descriptive reference)

> **Boundary: this is descriptive, not legal advice.** It explains how the
> framework is structured so the data can be interpreted correctly. It does not
> tell any landlord or tenant what increase to apply. For an actual decision,
> consult RERA's official calculator and a qualified professional.

Owns the regulatory vocabulary that makes New vs Renewed and rent-level claims
meaningful.

## Contract

- **owns:** descriptive explanation of the RERA/Decree-43 rent-increase framework.
- **hands-off-to:**
  - `rent-figure-trust` — any figure used in this context still passes the trust gate.
- **routes-from:**
  - `business-intelligence/_department` — any RERA / rent-cap / Decree-43 question.
  - `ejari-contract-semantics` — New vs Renewed meaning originates there.

## Triggers

Decree 43, RERA Rental Index, rent-increase cap, New vs Renewed cap, "how much
can rent go up", lawful increase bands.

## Core framework (descriptive)

- **The index sets the band, not a single number.** Under the framework, the
  permitted increase at renewal is tied to how far the current rent sits below
  the RERA Rental Index for comparable stock (area + property type + sub-type +
  bedrooms). The larger the gap below index, the larger the permitted band — in
  tiers, up to a capped maximum.
- **New vs Renewed is the pivot.** A **Renewed** lease is what the cap applies
  to. A **New** lease is at market and is not capped by the renewal bands. This
  is exactly why `rent-figure-trust` and the indices never pool the two when
  reasoning about rent level.
- **Freehold is irrelevant to the cap.** Title/zone (`IS_FREE_HOLD_EN`) does not
  change the renewal band; see `ejari-contract-semantics`.
- **This dataset is not the index.** The Ejari registrations here are a sample of
  actual contracts; the official RERA Rental Index is a separate published
  reference. Describe our medians as market observation, never as "the RERA cap".

## Gates

- Never state a specific permitted increase as advice — describe the band logic.
- Never equate a project median with the official RERA index value.
- Always attach the New-vs-Renewed distinction when discussing caps.

## References

- `lib/config.py` — `is_residential` — usage test used when scoping comparable stock.
- `lib/analysis/gold_indexes.py` — `build_residential_market_index` — the residential market surface a cap discussion would reference.

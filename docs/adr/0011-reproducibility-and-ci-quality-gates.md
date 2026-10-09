# ADR-11: Reproducible dependencies + a CI quality gate

**Status:** Accepted · **Date:** 2026-10-08

## Context

The pipeline's correctness rests on details that an unpinned dependency can silently move. The
push build (`.github/workflows/build_and_deploy.yml`) ran `make build && make test` only, and:

- **`polars` was unpinned** (`pyproject.toml`, `requirements.txt`) with no committed lockfile.
  `CONTRIBUTING.md` already documents the hazard: `pl.len()` yields `UInt32` while the published
  Gold `n` carries `Int64`, and `DataFrame.equals()` compares values, not dtypes — so a polars
  upgrade can move a dtype with a green suite.
- **No lint, type, or coverage gate.** Undefined names and unused/duplicate imports reached the
  tree unremarked.
- **The evidence was live, not hypothetical:** resolving the environment fresh installed
  `pydantic 2.14`, and an existing test broke because it asserted a `NameError` that newer pydantic
  raises as `PydanticUserError`. The suite that was green yesterday fails on the same code today.
- **PSF (price-per-sqft) had three private `200` literals** — `silver_contract.py`,
  `enrichment.py`, `market_analytics.py` — so the reporting floor could drift between surfaces.
- **`.gitignore` silently swallowed deliverables:** a bare `SKILL.md` rule hid the project Agent
  Skills, and `**.lock` meant `uv.lock` could never be committed.
- **`run_etl_pipeline` logged the raw `EJARI_URL`**, whose query string may carry a token.

## Decision

Adopt the reproducible, gated baseline for this repo:

1. **Lock and constrain.** Dependencies resolve through `uv`; a committed `uv.lock` is the source
   of truth. `polars` carries a coarse bound (`>=1.0,<2`) in `pyproject.toml` and
   `requirements.txt`. Dev tooling lives in the `dev` dependency group (`ruff`, `pytest-cov`).
2. **One CI quality gate.** `make lint` runs `ruff check` + `ruff format --check`; `make coverage`
   runs pytest with `--cov=lib --cov-fail-under=75`. `build_and_deploy.yml` runs both on every
   push. Lint is **high-signal only** (`F`, `E9`, `B`): real bugs, not style.
3. **A coverage floor is a ratchet.** Raise it as coverage improves; never lower it to pass.
4. **One owner per PSF rule.** `lib/config.PSF_MIN_AREA_SQFT` (the floor) and
   `lib.config.psf_expression` (the guarded division) are the single source; the three call sites
   import them.
5. **Secrets do not reach logs.** The download log redacts the `EJARI_URL` query string.
6. **Deliverables are trackable.** `.gitignore` re-includes `.commandcode/skills/**/SKILL.md` and
   no longer ignores `*.lock`.

## Rationale

- **A lockfile is the only real defense against transitive drift.** A bound narrows the window; the
  lock closes it. The pydantic failure is the proof, not a scare story.
- **A gate that only runs tests cannot catch what tests don't assert.** Lint catches the class of
  defect (unused imports, duplicate definitions, undefined names) that no test was ever going to.
- **Formatting is mechanical and reviewable once.** Applied in a single dedicated commit so later
  diffs stay about behavior.
- **One PSF owner makes "the surfaces cannot disagree" true, not aspirational.** Before this, the
  three `200`s agreed only because no one had changed one of them.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Pin exact versions in `requirements.txt` only | No transitive pinning; `uv`/pip drift returns |
| Full toolbox now (mypy, pre-commit, `.editorconfig`, Action bumps) | Larger surface than the failure warrants; deferred to a follow-up |
| `ruff` with the full default ruleset | Noisy style churn; high-signal `F/E9/B` is what catches defects |
| Set the coverage floor at the current 79% | Too tight to start; 75% ratchets up without flaking on env variation |
| Make coverage/gates advisory (warn, not fail) | An advisory gate is a gate that gets ignored |

## Consequences

- `uv.lock`, `requirements-dev.txt`, and the `dev` dependency group are added; `make lint`,
  `make coverage`, `make check` are the local gate.
- `lib/config.py` owns `PSF_MIN_AREA_SQFT` and `psf_expression`; `silver_contract`, `enrichment`
  and `market_analytics` import them. The stale "three PSF sites" comments are gone.
- `run_etl_pipeline._redact_url` guards the download log.
- The pydantic test now accepts the version-dependent exception type.
- **Deferred:** mypy/type-check, pre-commit, `.editorconfig`, Action version bumps, and turning
  off `ACTIONS_*_DEBUG`.

## Verification

- `make check` — ruff clean, coverage ≥ floor, the full test suite passes. (The suite grows; the
  gate is the floor, not a fixed test count.)
- `tests/test_logging_redaction.py` — the download log line carries neither the query string nor a
  token; `_redact_url` handles bare and unparseable input.
- `tests/test_p0_gates.py::test_psf_min_area_sqft_has_one_owner_and_all_surfaces_honour_it` —
  199 sqft nulled, 200 kept, on all three surfaces; the constant is the same object in `config` and
  `silver_contract`.
- `git check-ignore` / `git ls-files -o --exclude-standard` — skill `SKILL.md` files and `uv.lock`
  are trackable.

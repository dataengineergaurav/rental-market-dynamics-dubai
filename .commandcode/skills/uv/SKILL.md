---
name: uv
description: Use uv (Astral's fast Python package and project manager) in this repo — sync the environment, lock or upgrade dependencies, run modules and tests, add/remove packages, and fix a missing or stale lockfile. Use when setting up the project, running the pipeline (`uv run python -m lib...`) or pytest, adding a dependency, or whenever `uv`, `uvx`, or `uv.lock` comes up.
when_to_use: Setting up or reproducing the dev environment; running the ETL/layers modules or the test suite; adding, removing, or bumping a dependency; regenerating or validating `uv.lock`; anything the Makefile or CI runs through `uv`.
---

# uv — Python package and project manager

uv is Astral's Rust-based replacement for pip + venv + pip-tools + poetry. In this repo it is the
reproducible way to build the environment and run the pipeline: the `Makefile` and the daily GitHub
workflows shell out to it, and CI uses `astral-sh/setup-uv`. `pyproject.toml` + `uv.lock` are the
source of truth; `requirements.txt` is kept only for the legacy pip path.

## Core loop

```bash
uv sync              # create/refresh .venv from pyproject.toml + uv.lock
uv run <cmd>         # sync (if needed), then run <cmd> inside the project env
uv lock              # (re)resolve dependencies and write uv.lock
uv lock --check      # exit non-zero if uv.lock is out of date (the CI guard)
uv add <pkg>         # add a runtime dependency and update the lock
uv add --dev <pkg>   # add to the "dev" dependency group and update the lock
uv remove <pkg>      # remove a dependency and update the lock
```

`uv run` is the workhorse: it makes `python`, `pytest`, `ruff`, etc. resolve to the project
environment without activating anything. Prefer `uv run ...` over `python ...` so the same
interpreter and lockfile are always used.

## In this repo

- **Local editable package.** `pyproject.toml` declares
  `[tool.uv.sources] lib = { path = "./lib", editable = true }`, so `uv sync` installs `lib/` from
  source. Never `pip install lib/` — `uv sync` is what wires it in, and edits under `lib/` take
  effect with no reinstall.
- **Run the pipeline modules:**
  ```bash
  uv run python -m lib.analysis.build_layers_duckdb --db output/rents_layers.duckdb --csv output/rent_contracts_YYYYMMDD.csv
  uv run python -m lib.workspace.publish_layers --artifact output/rents_layers.duckdb --data-through YYYY-MM-DD
  ```
- **Tests:** `uv run pytest -q` (the `Makefile`'s `make test` is `pytest .`, which needs the
  environment already synced — prefer `uv run pytest`).
- **CI:** `.github/workflows/daily_layers.yml` uses `astral-sh/setup-uv@v5`; other jobs use pip.
  When both must work, add runtime deps to `pyproject.toml` *and* `requirements.txt`.

## Adding or bumping a dependency

1. `uv add <pkg>` (or `uv add --dev <pkg>` for tooling).
2. Commit both `pyproject.toml` and the regenerated `uv.lock`.
3. If the repo also ships a `requirements.txt`, mirror the change there so the pip path stays valid.

A bare name with no lock (`polars`) is a future incident — an upgrade can move a dtype with no test
change. The bound in `pyproject.toml` plus the committed `uv.lock` is the fix.

## Pitfalls

- **Lockfile is source of truth — commit it, ignore `.venv/`.** A gitignore rule that swallows
  `*.lock` silently drops `uv.lock`; check `git check-ignore -v uv.lock` if it won't commit.
- **Stale lock blocks `uv run`.** If `uv run` reports the lock is out of date, run `uv lock`, or use
  `uv run --no-sync` to run against the current env / `--frozen` to run against the lock as-is.
- **`--frozen` vs `--locked`.** `--locked` asserts `uv.lock` matches `pyproject.toml` (use in CI);
  `--frozen` uses the lock unchanged without asserting. Neither re-resolves.
- **`uv` not installed?** `pipx install uv`, `pip install uv`, or Astral's installer script. Then
  `uv --version`.
- **Ephemeral tools.** `uvx ruff check .` runs a tool without adding it as a project dependency.
- **Python version.** `uv python pin 3.12` writes `.python-version`; `uv python install 3.12`
  fetches an interpreter. Prefer matching CI (`3.12`).

## References

- [references/troubleshooting.md](references/troubleshooting.md) — symptom → cause → command.

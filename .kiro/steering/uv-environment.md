# Unified Environment: uv only

All execution operations in this project run through `uv`, on every system. There is one environment and one runner; do not fall back to bare `python`, `pip`, `pytest`, `ruff`, or a manually activated virtualenv.

## Rules

- Run Python code and tools through `uv run`, e.g. `uv run python ...`, `uv run pytest`, `uv run ruff format`, `uv run ruff check --fix`.
- Manage dependencies and the environment with `uv`: `uv sync` to install, `uv add` / `uv remove` to change dependencies. Do not call `pip install` directly.
- The lockfile `uv.lock` and `pyproject.toml` are the source of truth for dependencies. Keep them in sync via `uv`.
- Hooks, scripts, CI, and Makefile targets must invoke tooling through `uv` so behavior is identical on any machine.
- Do not assume a pre-activated virtualenv. `uv run` resolves and provisions the environment itself.

## Rationale

A single `uv`-managed environment keeps execution reproducible across developer machines and CI. Mixing in system Python or pip reintroduces the drift this rule exists to prevent.

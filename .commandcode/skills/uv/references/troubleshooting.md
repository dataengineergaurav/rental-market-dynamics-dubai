# uv troubleshooting — symptom → cause → command

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `uv: command not found` | uv not installed / not on PATH | `pipx install uv` or `pip install uv`; then `uv --version` |
| `uv run` refused: "lockfile ... out of date" | `pyproject.toml` changed without re-locking | `uv lock` |
| `uv.lock` won't `git add` | a `*.lock` rule in `.gitignore` | `git check-ignore -v uv.lock`; un-ignore it |
| `ModuleNotFoundError: No module named 'lib'` | ran `python`/`pytest` outside uv | `uv sync`, then `uv run ...` |
| `ModuleNotFoundError: polars` etc. | env not synced | `uv sync` |
| Edits to `lib/` not reflected | installed non-editable | ensure `[tool.uv.sources] lib = { editable = true }`; `uv sync` |
| Wrong Python version | no pin / no interpreter | `uv python pin 3.12`; `uv python install 3.12` |
| CI green locally, red in CI (deps) | `requirements.txt` and `pyproject.toml` drifted | update both; `uv lock` |
| Cache looks corrupt / stale | uv cache | `uv cache clean`; retry |

## Diagnostics

```bash
uv --version                 # installed?
uv lock --check              # is the lock current?
uv tree                      # resolved dependency graph
uv pip list                  # what is actually installed in .venv
uv run python -c "import sys; print(sys.executable)"   # which interpreter runs
```

## Notes for this repo

- The `Makefile` mixes `uv run` (layers targets) and bare `python`/`pytest` (build/etl/test). If a
  `make` target cannot find a module, run its command through `uv run` instead.
- `requirements.txt` exists for the pip-based CI jobs; keep it consistent with `pyproject.toml`.
- Do not commit `.venv/` (already gitignored); do commit `uv.lock`.

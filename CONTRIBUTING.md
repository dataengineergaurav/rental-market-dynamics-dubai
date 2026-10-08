# Contributing to Real Estate Analysis Dubai

Thank you for your interest in contributing to this project!

## How to Contribute

1. **Fork the repository** and create your branch:
   ```bash
   git checkout -b feature/your-feature-name

2. **Make your changes** while adhering to the project's coding standards.

3. **Run tests** to ensure your changes do not break functionality:
    ```bash
    make test
    ```

4. **Commit your changes** with a clear commit message.

5. **Open a Pull Request** against the dev branch.

## Code Style

- Follow PEP 8 guidelines.
- Include type annotations where applicable.
- Write clear and descriptive docstrings for functions and classes


## Testing Semantics

Four traps in this stack. Each one has produced a green suite over broken code.

### `DataFrame.equals()` compares values, not dtypes

```python
pl.DataFrame({"n": [1, 2]}, schema={"n": pl.UInt32}).equals(
    pl.DataFrame({"n": [1, 2]}, schema={"n": pl.Int64})
)  # True — a UInt32/Int64 regression is invisible here
```

Assert `frame.schema` explicitly when the dtype is the thing under test. This matters here
because `pl.len()` yields `UInt32` while the published Gold artifacts carry `n` as `Int64`, and
`polars` is unpinned in `pyproject.toml`, so an upgrade can move a dtype with no failure.

### `pl.DataFrame(records)` infers schema from the first 100 rows only

```python
pl.DataFrame(records)  # field null in rows 1-100, string at 101 -> ComputeError
pl.DataFrame(records, infer_schema_length=None)  # scan the whole thing
```

Real, not hypothetical: `output/rent_contracts_20260916.csv` has a `MASTER_PROJECT_EN` value at
row 101 ("Hills Park"). Four of the five daily files pass without the flag and one crashes, so
this looks like flaky data rather than a code bug. `lib/classes/silver_contract.py:506` sets it
on both of its constructions.

### Temporal accessors return `Int8`

```python
pl.col("t").dt.hour() * 3600  # 1 * 3600 wraps to 16
```

`dt.hour()`, `dt.minute()` and `dt.second()` are `Int8`; multiply before widening, or compute in
seconds and cast: `pl.col("t").dt.hour().cast(pl.Int32) * 3600`.

### pydantic v2 field-name and default rules

Three separate footguns, all silent unless you look:

- **A leading-underscore field with an explicit `Field()` raises at class-definition time** — not
  at instantiation — so the whole module fails to import, and everything that imports it dies with
  it. The exception *type* is pydantic-version dependent (older pydantic raised `NameError`,
  >= 2.13 raises `PydanticUserError`); either way it fails loudly. The bare-default
  (`_x: Optional[int] = None`) and bare-annotation
  (`_x: Optional[int]`) forms are accepted, but as *private attributes*, silently absent from
  `model_fields` and never validated. There is no correct form: don't name fields with a leading
  underscore. `tests/test_silver_contract.py:63` pins this.
- **A derived field declared without a default is required**, so construction fails before
  `mode="after"` validators run. Every derived field defaults `None`.
- **A pydantic bound rejects before the validator sees the value.** `Field(ge=0)` on
  `annual_amount` means a negative rent is a generic schema error instead of the named
  `annual_amount_not_positive` violation — and it is quarantined rather than counted. Keep range
  checks in the `mode="after"` validator.


## Reporting Issues
If you encounter any issues or have suggestions, please open an issue on GitHub.

Thank you for contributing!

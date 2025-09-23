# Repository Guidelines

## Project Structure & Module Organization
- `CLAUDE.md` is the knowledge base index; update it whenever you add guides or workflow assets so human and AI agents stay aligned.
- `tool__workstation.py` manages Spark sessions and presets that every helper module should import rather than building standalone sessions.
- `tool__dag_chainer.py`, `tool__table_polisher.py`, and `tool__table_indexer.py` form the core transformation toolkit; extend them instead of duplicating logic.
- `resources/` holds sample CSV data and `guide__tooling.md`; mirror its patterns in docs and lightweight regression fixtures.
- `models/` is reserved for persisted artifacts—keep generated outputs small or add them to `.gitignore`.
- `sample.ipynb` demonstrates the intended workflow; reuse its import order when bootstrapping new notebooks.

## Build, Test, and Development Commands
- `python -m pip install -r requirements.txt` syncs PySpark, Delta, and notebook dependencies after cloning or upgrading packages.
- `python - <<'PY'
from tool__workstation import get_spark
spark = get_spark("auto")
print(spark.version)
PY` verifies that workstation presets resolve to a usable Spark session.
- `pytest -q` runs the test suite; place new coverage under `tests/` with fixtures that reuse `SparkWorkstation` for deterministic sessions.
- `jupyter notebook sample.ipynb` launches the reference pipeline for exploratory or demo work.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation, docstrings, and type hints as illustrated in `tool__workstation.py`.
- Name modules `tool__<purpose>.py` and prefer keyword arguments for configuration instead of globals.
- Use module-level loggers (`logging.getLogger(__name__)`) and guard Databricks-only logic with explicit environment checks.

## Testing Guidelines
- Mirror module names in `tests/` (e.g., `tests/test_table_indexer.py`) and exercise both `auto` and mocked Databricks environments.
- Cover Delta write/read flows, schema inference, and workstation health checks to prevent regressions in production notebooks.

## Commit & Pull Request Guidelines
- Write imperative, present-tense commits under 72 characters (e.g., "Fix Databricks metastore issues"), consistent with current history.
- Keep PRs focused: explain the scenario, list validation commands, and link Databricks runs or notebooks when behavior differs across environments.
- Attach screenshots or spark-submit logs for UI or cluster-facing changes, and request review on documentation or resource updates that affect onboarding.

## Security & Configuration Tips
- Never commit workspace URLs, access tokens, or cluster configs; load secrets via environment variables consumed by `tool__workstation`.
- When sharing Delta outputs, scrub customer identifiers and rely on synthetic datasets from `resources/` for reproducible examples.

# SemT DAG Builder

Convert a Jupyter notebook that uses semt_py into production-ready Python scripts and an Airflow DAG. This tool deterministically parses a notebook, identifies SemT operations (load/reconcile/extend/download), and renders them into a runnable, stage-by-stage pipeline.

## Features

- Deterministic parsing of Jupyter notebooks (no LLM dependency)
- Generates:
  - Stage scripts (e.g., stage01_load_table.py, …)
  - A complete Airflow DAG (dags/<dag_name>.py)
  - A scripts-local requirements.txt for runtime dependencies
  - A minimal scripts/utils/file_utils.py to pass JSON state between stages
- Robust templating (Jinja2) with custom filters
- Intelligent data handoff: XCom + local JSON files
- Requirements for the generated scripts include semt-py from your GitHub repository

## Quickstart (with uv)

Prerequisites:
- Python 3.9+
- uv (https://docs.astral.sh/uv/)

Install locally:

```bash
uv venv
source .venv/bin/activate
uv pip install -e .
```

Confirm the CLI is available:

```bash
nb2scripts --help
# or
semt-dag-builder --help
```

Generate scripts and a DAG:

```bash
nb2scripts path/to/notebook.ipynb \
  --output-dir scripts \
  --generate-dag \
  --dag-name my_pipeline \
  --verbose
```

Outputs:
- scripts/: stage scripts plus support files (requirements.txt, README.md, utils/file_utils.py)
- dags/: Airflow DAG (my_pipeline.py)

## Running the Generated Scripts

Each stage is a standalone Python script you can run with environment variables:

```bash
export API_BASE_URL='http://localhost:3003'
export API_USERNAME='test'
export API_PASSWORD='test'
export DATASET_ID='5'
export DATA_DIR='/path/to/data'
export RUN_ID="$(date +%Y%m%d_%H%M%S)"

# For stage01 (load)
export INPUT_FILE_PATH="/path/to/input.csv"
python scripts/stage01_load_table.py

# For subsequent stages, pass the JSON path from the previous stage:
export INPUT_JSON_PATH="/path/to/generated/state.json"
python scripts/stage02_reconcile_poi.py
```

The scripts/utils/file_utils.py manages local JSON state in DATA_DIR/RUN_ID and coordinates with Airflow XCom in the DAG.

## Airflow DAG

- The DAG binds /app/data into the DockerOperator and uses XCom to pass JSON paths between tasks.
- Environment variables injected per task:
  - Stage 1: INPUT_FILE_PATH (resolved by `find_input_file`)
  - Subsequent stages: INPUT_JSON_PATH (XCom from previous task)
  - Common: API_BASE_URL, API_USERNAME, API_PASSWORD, DATA_DIR, RUN_ID, STAGE_NAME, STAGE_NUMBER

## Generated Scripts’ Runtime Requirements

Generated scripts depend on:
- pandas, requests, fake-useragent, PyJWT, python-dateutil
- semt-py directly from the repo: https://github.com/I2Tunimib/I2T-library.git

A scripts/requirements.txt is written automatically:

```
pandas>=1.3.0
requests>=2.31.0
fake-useragent>=1.3.0
PyJWT>=2.8.0
python-dateutil>=2.8.2
git+https://github.com/I2Tunimib/I2T-library.git#egg=semt-py
```

Install them in the environment that runs the scripts (e.g., your Docker image or local venv).

## Notes

- The tool does not require `semt_py` at generation time; it constructs scripts that will import it at runtime.
- If your notebook uses `get_input_with_default`, the tool extracts `table_name` and `dataset_id` defaults. When missing, the load stage derives `table_name` from the CSV filename.

## Development

Install dev extras:

```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

Lint/format (optional):

```bash
black src
autopep8 -r --in-place src
```

Run the CLI against a sample notebook and inspect scripts and DAG output.

## License

MIT (see LICENSE).
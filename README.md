# SemT DAG Builder

SemT DAG Builder converts a Jupyter notebook that uses the semt_py library into production-ready, deterministic Python stage scripts and an Airflow DAG. It enables you to:

- Detect and extract pipeline operations from a notebook (load, reconcile, extend, download).
- Generate runnable stage scripts that pass JSON state between steps.
- Generate a working Airflow DAG that orchestrates those scripts using DockerOperator.
- Optionally scaffold and run a complete local stack (Airflow + runner + backend) via Docker Compose.

This project can be used both by ML/Data engineers who already have Airflow and by beginners who want a fully dockerized sandbox to experiment with SemT pipelines.

Features

- Deterministic operation detection (no LLMs): load, reconcile, extend, download
- Notebook parsing with nbformat
- Jinja2-based code generation using stable templates
- JSON state handoff between stages (local files + Airflow XCom)
- Includes a scaffolded local stack (Airflow + Python runner + I2T backend) as templates
- Configurable Docker image and network for DAG tasks
- Robust script rendering with sane defaults

Requirements

- Python 3.9+ (3.10/3.11 recommended)
- uv (recommended) or pip
- Docker & Docker Compose (for the local stack)

Install the generator (with uv)

- Create and activate a virtual environment:

  uv venv
  source .venv/bin/activate

- Install SemT DAG Builder in editable mode:

  uv pip install -e .

Alternatively, using pip:

- pip install -e .

This installs the nb2scripts CLI (and an alias semt-dag-builder).

Basic Usage

There are two common paths:

- Generate scripts/DAG only (for users with their own Airflow environment)
- Generate scripts/DAG AND scaffold a full local stack (for users who want a one-command sandbox)

A. Convert a notebook to scripts and an Airflow DAG

- Convert:

  nb2scripts convert path/to/notebook.ipynb \
    --output-dir scripts \
    --generate-dag \
    --dag-name my_pipeline \
    --verbose

This will create:

- scripts/: stage scripts, scripts/requirements.txt, scripts/README.md, and scripts/utils/file_utils.py
- dags/: my_pipeline.py

- Run the scripts manually (without Airflow) by setting environment variables:

  export API_BASE_URL='http://localhost:3003'
  export API_USERNAME='test'
  export API_PASSWORD='test'
  export DATASET_ID='5'
  export DATA_DIR='/path/to/data'
  export RUN_ID="$(date +%Y%m%d_%H%M%S)"

  # Stage 1: Load
  export INPUT_FILE_PATH="/path/to/input.csv"
  python scripts/stage01_load_table.py

  # Stage 2: Reconcile (uses the JSON output of Stage 1)
  export INPUT_JSON_PATH="$(cat /path/to/data/$RUN_ID/current.path)"
  python scripts/stage02_reconcile_poi.py

  # Stage 3..N: continue similarly

B. Scaffold and run a local stack (Airflow + runner + backend)

- Initialize a local stack folder with templates:

  nb2scripts init-env --target stack

This creates stack/ with:

- docker-compose.yml (runner + backend)
- docker-compose.airflow.yml (Airflow)
- Dockerfile.python-runner (builds image: semt-pipeline:latest)
- Dockerfile.airflow
- entrypoint.sh
- .env.example (copy to .env and customize)
- dags/, scripts/, logs/, plugins/, data/

- (Optional) Clone the I2T backend into the stack:

  git clone https://github.com/I2Tunimib/I2T-backend.git stack/I2T-backend

- Copy environment variables:

  cp stack/.env.example stack/.env
  # Edit stack/.env for your credentials if needed

- Convert your notebook directly into the stack folders:

  nb2scripts convert path/to/notebook.ipynb \
    --output-dir stack/scripts \
    --generate-dag \
    --dag-name semt_pipeline \
    --docker-image semt-pipeline:latest \
    --docker-network app_network \
    --verbose

This creates:

- stack/scripts/: generated stage scripts + requirements.txt
- stack/dags/: semt_pipeline.py (Airflow DAG)

- Bring up the local stack:

  docker compose -f stack/docker-compose.yml -f stack/docker-compose.airflow.yml up -d --build

- Access your services:
  - Airflow Webserver: http://localhost:8080 (user/password from stack/.env; defaults: airflow/airflow)
  - Backend API (node-server-api): http://localhost:3003

- In Airflow:
  - The semt_pipeline DAG appears under the DAGs page.
  - Place your CSV file in stack/data/. The DAG’s first task discovers the latest CSV file.
  - Trigger the DAG.

What gets generated?

- Stage scripts (scripts/)
  - stage01_load_table.py, stage02_reconcile_...py, stage0X_extend_...py, stage0Y_download_...py, etc.
  - scripts/utils/file_utils.py: Minimal helper that saves JSON state under DATA_DIR/RUN_ID and provides load/save utilities. This is how stages pass data locally between steps and also produce an XCom-friendly path for Airflow.
  - scripts/requirements.txt: Runtime dependencies for the scripts (e.g. pandas, requests, fake-useragent, PyJWT, python-dateutil, and semt-py from GitHub).
  - scripts/README.md: Short reference for these scripts.

- Airflow DAG (dags/)
  - dag_name.py: A DAG that uses DockerOperator to run each stage script in the runner image.
  - Injects environment variables for each stage, including INPUT_FILE_PATH for stage 1, and INPUT_JSON_PATH for subsequent stages (via XCom).
  - Uses the configured docker image (default: semt-pipeline:latest) and network (default: app_network).
  - Mounts /app/data and /app/scripts inside the container (stack folders), so tasks see your files.

Important environment variables for the scripts

- API_BASE_URL: Backend API base (e.g. http://node-server-api:3003)
- API_USERNAME / API_PASSWORD: Backend credentials
- DATASET_ID: Default dataset ID (e.g. "5")
- DATA_DIR: Local directory to store JSON state files (mounted into containers as /app/data)
- RUN_ID: Used to isolate runs in separate subfolders under DATA_DIR
- INPUT_FILE_PATH (Stage 1): CSV path for the load stage
- INPUT_JSON_PATH (Stages 2..N): The JSON state path from previous stage
- TABLE_NAME: Optional override for the load stage if not derived from CSV file name

How operation detection works

The generator scans code cells to detect SemT operations:

- Load: pd.read_csv(...), table_manager.add_table(...)
- Reconcile: reconciliation_manager.reconcile(...)
- Extend: extension_manager.extend_column(...)
- Download: utility.download_csv(...)

It also extracts parameters (e.g., column_name, reconciliator_id, extender_id, properties). For the load stage, it extracts:

- csv_file from read_csv(...)
- dataset_id either from direct assignment (dataset_id = "5") or from get_input_with_default(..., "5")
- table_name either from direct assignment or get_input_with_default(..., "my_table"); if not found, it derives table name from the CSV filename.

Local stack details

- The local dockerized stack is scaffolded via:

  nb2scripts init-env --target stack

It contains:

- Runner image (Dockerfile.python-runner): built as semt-pipeline:latest
- Airflow (Dockerfile.airflow): a custom image with airflow providers and I2T library installed
- Compose files:
  - docker-compose.yml: python-task-runner and the backend (node-server-api)
  - docker-compose.airflow.yml: airflow-webserver, airflow-scheduler, postgres
- .env.example: environment configuration for the stack (copy to .env)
- Shared volumes:
  - ./scripts -> /opt/airflow/scripts and /app/scripts
  - ./dags -> /opt/airflow/dags
  - ./data -> /app/data
- The DAG uses DockerOperator with:
  - image: semt-pipeline:latest (override using --docker-image)
  - network: app_network (override using --docker-network)
  - mounts /app/data and /app/scripts

Customizing the DAG

When generating the DAG via convert:

- --docker-image: sets the runner image for DockerOperator tasks (default: semt-pipeline:latest)
- --docker-network: sets the Docker network for DockerOperator tasks (default: app_network)
- --dag-name: the file name and DAG id base (lowercased + sanitized)

Example:

nb2scripts convert notebook.ipynb \
  --output-dir stack/scripts \
  --generate-dag \
  --dag-name my_pipeline \
  --docker-image mycompany/semt-runner:1.2.3 \
  --docker-network my_airflow_net \
  -v

Troubleshooting

- TOML parse error for pyproject.toml
  - pyproject.toml must be valid TOML; it should begin with [build-system]. Remove any prose at the top.

- ImportError: cannot import name PipelineFileManager
  - Ensure you regenerated scripts after updating templates.
  - Confirm that scripts/__init__.py and scripts/utils/__init__.py exist (writer.create_support_files creates them).
  - The generated script_base includes a sys.path patch so from scripts.utils.file_utils import PipelineFileManager works from any CWD.

- Boolean rendering shows deduplicate = false (invalid Python)
  - Ensure you updated both templates/operations/reconcile.j2 and templates/operations/extend.j2 to:
    deduplicate = {{ 'True' if (params.deduplicate | default(false)) else 'False' }}
  - Delete old scripts and re-run the generator.

- DAG dependencies appear commented out / on one line
  - Make sure you’re using the updated dag_template.j2 that uses explicit newlines and doesn’t trim Jinja blocks where dependencies are printed.

- Airflow DockerOperator errors (Cannot connect to Docker)
  - Ensure /var/run/docker.sock is mounted into Airflow containers (done in docker-compose.airflow.yml).
  - Ensure the airflow user permissions are set; the provided Dockerfile.airflow config should suffice.

- Backend errors
  - Confirm node-server-api is healthy at http://localhost:3003/api/.
  - Check credentials: default is test/test (if your backend seeds them).

CLI Reference

- Convert:
  nb2scripts convert NOTEBOOK.ipynb \
    --output-dir scripts \
    [--max-scripts N] \
    [--generate-dag] \
    [--dag-name NAME] \
    [--docker-image IMAGE] \
    [--docker-network NET] \
    [-v]

- Scaffold:
  nb2scripts init-env --target stack

- Stack:
  nb2scripts stack up   --target stack
  nb2scripts stack down --target stack

Developer Guide

- Install dev dependencies:

  uv venv
  source .venv/bin/activate
  uv pip install -e ".[dev]"

- Code formatting:

  black src
  autopep8 -r --in-place src

- Run unit tests (if you add tests):

  pytest

- Project layout:

  src/nb2scripts/
    cli.py                 # CLI entrypoint
    loader.py              # Notebook loader
    classifier.py          # Operation classifier
    param_extractor.py     # Parameter extractor (supports get_input_with_default)
    renderer.py            # Jinja2 templates
    writer.py              # Writes scripts, support files, requirements
    dag_generator.py       # DAG renderer (image/network aware)
    templates/             # Script + DAG templates
    runtime_templates/     # Docker Compose + Dockerfiles + entrypoint + .env.example

- Packaging data files:

  Ensure pyproject.toml includes:

  [tool.setuptools.package-data]
  nb2scripts = [
    "templates/*.j2",
    "templates/operations/*.j2",
    "runtime_templates/*"
  ]

FAQ

- Do I need semt_py installed on my machine?  
  Not for generation. The generated scripts require semt_py at runtime — the generator writes scripts/requirements.txt that installs semt-py directly from the I2T GitHub repository.

- How does the pipeline pass data between stages?  
  Using scripts/utils/file_utils.py. Stage outputs are JSON files under DATA_DIR/RUN_ID. The path is saved to current.path and returned so Airflow can pass it via XCom to the next stage. You can also push directly to the backend and fetch the full table JSON again via TableManager if desired.

- Can I change the stage order or names?  
  Yes — the grouping is deterministic based on detected operations. You can rename scripts after generation if needed, but remember to update the DAG accordingly if you do.

- Where should I put my CSV input?  
  If you use the local stack: put it in stack/data. The DAG’s “find_input_file” looks for the most recent *.csv in /app/data (which maps to stack/data).

License

MIT. See LICENSE.

Acknowledgements

- semt_py: The underlying library used by the generated scripts to call the backend.
- Airflow: Orchestrates the stage scripts using DockerOperator.
- Jinja2: Provides deterministic templating for scripts and DAGs.

Issues / Contributing

- File issues or feature requests at:
  https://github.com/aliduabubakari/SemT-dag-builder/issues

- Contributions are welcome: fork, branch, make changes, and open a PR.
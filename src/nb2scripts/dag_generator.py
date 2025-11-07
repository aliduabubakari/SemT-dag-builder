# =============================================================================
# src/nb2scripts/dag_generator.py - DAG generator with image/network support
# =============================================================================

"""
Simplified DAG generator using deterministic templates.
"""
from __future__ import annotations

import datetime
import re
import logging
from typing import List, Dict, Any
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

from .schema import Script


class DAGGenerator:
    """Generates Airflow DAGs using Jinja2 templates."""

    def __init__(
        self, api_key: str | None = None, endpoint: str | None = None, deployment: str | None = None
    ):
        # Optional API config (not used in deterministic mode)
        self.api_key = api_key
        self.endpoint = endpoint
        self.logger = logging.getLogger(__name__)

        # Defaults for DockerOperator runtime
        self.docker_image = "semt-pipeline:latest"
        self.docker_network = "app_network"

        # Set up Jinja2
        template_dir = Path(__file__).parent / "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self.jinja_env.filters["regex_replace"] = self._regex_replace_filter

    def _regex_replace_filter(self, text: str, pattern: str, replacement: str = "") -> str:
        """Custom Jinja2 filter for regex replacement."""
        return re.sub(pattern, replacement, text)

    def generate_dag(self, scripts: List[Script], output_dir: Path, dag_name: str | None = None) -> Path:
        """Generate DAG file from scripts into output_dir/../dags/<dag_name>.py."""
        self.logger.info(f"🏗️ Generating DAG for {len(scripts)} scripts...")

        if not dag_name:
            dag_name = "generated_notebook_pipeline"

        # Prepare script data for template
        dag_scripts = self._prepare_script_data(scripts)

        # Render DAG template
        dag_content = self._render_dag_template(dag_name, dag_scripts)

        # Write DAG file to sibling 'dags' directory
        dag_file_path = output_dir.parent / "dags" / f"{dag_name}.py"
        dag_file_path.parent.mkdir(parents=True, exist_ok=True)
        dag_file_path.write_text(dag_content, encoding="utf-8")

        self.logger.info(f"✅ Generated DAG: {dag_file_path}")
        return dag_file_path

    def _prepare_script_data(self, scripts: List[Script]) -> List[Dict[str, Any]]:
        """Prepare script data for the DAG template."""
        dag_scripts: List[Dict[str, Any]] = []

        for script in scripts:
            # Extract environment variables from operations
            env_vars: Dict[str, str] = {}

            for op in script.operations:
                params = op.meta.get("params", {})

                if op.op_type == "load":
                    env_vars["DATASET_ID"] = str(params.get("dataset_id", "5"))
                    env_vars["TABLE_NAME"] = str(params.get("table_name", "table"))

                elif op.op_type == "reconcile":
                    env_vars["RECONCILE_COLUMN"] = str(params.get("column_name", ""))
                    env_vars["RECONCILIATOR_ID"] = str(params.get("reconciliator_id", ""))

                elif op.op_type == "extend":
                    env_vars["EXTEND_COLUMN"] = str(params.get("column_name", ""))
                    env_vars["EXTENDER_ID"] = str(params.get("extender_id", ""))
                    env_vars["PROPERTIES"] = str(params.get("properties", ""))

            dag_scripts.append(
                {
                    "name": script.name,
                    "filename": script.filename,
                    "stage": script.stage,
                    "stage_name": self._infer_stage_name(script),
                    "env_vars": env_vars,
                    "operations": [
                        {"op_type": op.op_type, "name": op.name, "params": op.meta.get("params", {})}
                        for op in script.operations
                    ],
                    "documentation": self._generate_documentation(script),
                }
            )

        return dag_scripts

    def _infer_stage_name(self, script: Script) -> str:
        """Infer a simple stage name from the script name."""
        name = script.name.lower()
        if "load" in name:
            return "load"
        if "reconcile" in name:
            return "reconcile"
        if "extend" in name:
            return "extend"
        return "process"

    def _generate_documentation(self, script: Script) -> str:
        """Generate a simple documentation string for the DAG doc_md."""
        lines = [f"## {script.name}", ""]
        for op in script.operations:
            lines.append(f"- {op.op_type}: {op.name}")
        return "\n".join(lines)

    def _render_dag_template(self, dag_name: str, scripts: List[Dict[str, Any]]) -> str:
        """Render the Jinja2 DAG template with provided scripts."""
        try:
            template = self.jinja_env.get_template("dag_template.j2")
            return template.render(
                dag_name=dag_name,
                dag_id=dag_name.lower().replace(" ", "_").replace("-", "_"),
                dag_documentation=f"# {dag_name}\n\nAuto-generated pipeline",
                dag_tags=["generated", "notebook"],
                scripts=scripts,
                stage_names=[s["stage_name"] for s in scripts],
                timestamp=datetime.datetime.now(),
                docker_image=getattr(self, "docker_image", "semt-pipeline:latest"),
                docker_network=getattr(self, "docker_network", "app_network"),
            )
        except Exception as e:
            self.logger.error(f"❌ Template rendering failed: {e}")
            import traceback

            self.logger.error(traceback.format_exc())
            # Return minimal fallback
            return self._generate_minimal_dag(dag_name, scripts)

    def _generate_minimal_dag(self, dag_name: str, scripts: List[Dict[str, Any]]) -> str:
        """Generate a minimal fallback DAG in case template rendering fails."""

        docker_image = getattr(self, "docker_image", "semt-pipeline:latest")
        docker_network = getattr(self, "docker_network", "app_network")

        # Build task definitions
        task_defs: List[str] = []
        dependencies: List[str] = []

        for i, script in enumerate(scripts):
            task_var = script["name"].replace("-", "_").replace(".", "_")
            task_defs.append(
                f"""
    {task_var}_task = DockerOperator(
        task_id="{script['name']}",
        image='{docker_image}',
        command=['python', '/app/scripts/{script['filename']}'],
        environment={{
            'RUN_ID': "{{{{ ds_nodash }}}}_{{{{ ts_nodash }}}}",
            'STAGE_NAME': "{script['name']}",
            'STAGE_NUMBER': "{i+1}",
            'API_BASE_URL': 'http://node-server-api:3003',
            'DATA_DIR': '/app/data',
        }},
        do_xcom_push=True,
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='{docker_network}',
    )"""
            )

            if i == 0:
                dependencies.append(f"    find_input >> {task_var}_task")
            else:
                prev_var = scripts[i - 1]["name"].replace("-", "_").replace(".", "_")
                dependencies.append(f"    {prev_var}_task >> {task_var}_task")

        return f'''# dags/{dag_name}.py - Fallback DAG
"""Auto-generated fallback DAG."""

from __future__ import annotations
import os
import glob
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator

def find_latest_csv_file(**kwargs):
    """Find latest CSV file."""
    files = glob.glob('/app/data/*.csv')
    if not files:
        raise FileNotFoundError("No CSV files found")
    return max(files, key=os.path.getmtime)

with DAG(
    dag_id='{dag_name.lower().replace(' ', '_').replace('-', '_')}',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["generated", "fallback"],
) as dag:

    find_input = PythonOperator(
        task_id='find_input_file',
        python_callable=find_latest_csv_file,
    )
    {''.join(task_defs)}

    # Dependencies
{chr(10).join(dependencies)}
'''
# tools/nb2scripts/dag_generator.py - COMPLETE WITH REGEX_REPLACE FILTER

"""
Simplified DAG generator using deterministic templates.
"""
import datetime
import re  # ← Add this import
import logging
from typing import List, Dict, Any
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

from .schema import Script

class DAGGenerator:
    """Generates Airflow DAGs using simple deterministic templates."""
    
    def __init__(self, api_key: str = None, endpoint: str = None, deployment: str = None):
        # API parameters optional now - only needed for LLM enhancements
        self.api_key = api_key
        self.endpoint = endpoint
        self.logger = logging.getLogger(__name__)
        
        # Set up Jinja2
        template_dir = Path(__file__).parent / "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # ← ADD THE REGEX_REPLACE FILTER HERE
        self.jinja_env.filters['regex_replace'] = self._regex_replace_filter
    
    def _regex_replace_filter(self, text: str, pattern: str, replacement: str = '') -> str:
        """Custom Jinja2 filter for regex replacement."""
        return re.sub(pattern, replacement, text)
    
    def generate_dag(self, scripts: List[Script], output_dir: Path, dag_name: str = None) -> Path:
        """Generate DAG file."""
        self.logger.info(f"🏗️ Generating DAG for {len(scripts)} scripts...")
        
        if not dag_name:
            dag_name = "generated_notebook_pipeline"
        
        # Prepare script data
        dag_scripts = self._prepare_script_data(scripts)
        
        # Render DAG template
        dag_content = self._render_dag_template(dag_name, dag_scripts)
        
        # Write DAG file
        dag_file_path = output_dir.parent / "dags" / f"{dag_name}.py"
        dag_file_path.parent.mkdir(parents=True, exist_ok=True)
        dag_file_path.write_text(dag_content, encoding='utf-8')
        
        self.logger.info(f"✅ Generated DAG: {dag_file_path}")
        return dag_file_path
    
    def _prepare_script_data(self, scripts: List[Script]) -> List[Dict[str, Any]]:
        """Prepare script data for template."""
        dag_scripts = []
        
        for script in scripts:
            # Extract environment variables from operations
            env_vars = {}
            
            for op in script.operations:
                params = op.meta.get('params', {})
                
                if op.op_type == 'load':
                    env_vars['DATASET_ID'] = params.get('dataset_id', '5')
                    env_vars['TABLE_NAME'] = params.get('table_name', 'table')
                
                elif op.op_type == 'reconcile':
                    env_vars['RECONCILE_COLUMN'] = params.get('column_name', '')
                    env_vars['RECONCILIATOR_ID'] = params.get('reconciliator_id', '')
                
                elif op.op_type == 'extend':
                    env_vars['EXTEND_COLUMN'] = params.get('column_name', '')
                    env_vars['EXTENDER_ID'] = params.get('extender_id', '')
                    env_vars['PROPERTIES'] = params.get('properties', '')
            
            dag_scripts.append({
                'name': script.name,
                'filename': script.filename,
                'stage': script.stage,
                'stage_name': self._infer_stage_name(script),
                'env_vars': env_vars,
                'operations': [
                    {
                        'op_type': op.op_type,
                        'name': op.name,
                        'params': op.meta.get('params', {})
                    }
                    for op in script.operations
                ],
                'documentation': self._generate_documentation(script)
            })
        
        return dag_scripts
    
    def _infer_stage_name(self, script: Script) -> str:
        """Infer stage name from script."""
        name = script.name.lower()
        if 'load' in name:
            return 'load'
        elif 'reconcile' in name:
            return 'reconcile'
        elif 'extend' in name:
            return 'extend'
        return 'process'
    
    def _generate_documentation(self, script: Script) -> str:
        """Generate simple documentation."""
        lines = [f"## {script.name}", ""]
        for op in script.operations:
            lines.append(f"- {op.op_type}: {op.name}")
        return "\n".join(lines)
    
    def _render_dag_template(self, dag_name: str, scripts: List[Dict]) -> str:
        """Render DAG template."""
        try:
            template = self.jinja_env.get_template("dag_template.j2")
            
            return template.render(
                dag_name=dag_name,
                dag_id=dag_name.lower().replace(' ', '_').replace('-', '_'),
                dag_documentation=f"# {dag_name}\n\nAuto-generated pipeline",
                dag_tags=["generated", "notebook"],
                scripts=scripts,
                stage_names=[s['stage_name'] for s in scripts],
                timestamp=datetime.datetime.now()
            )
        except Exception as e:
            self.logger.error(f"❌ Template rendering failed: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            # Return minimal fallback
            return self._generate_minimal_dag(dag_name, scripts)
    
    def _generate_minimal_dag(self, dag_name: str, scripts: List[Dict]) -> str:
        """Generate minimal fallback DAG."""
        
        # Build task definitions
        task_defs = []
        dependencies = []
        
        for i, script in enumerate(scripts):
            task_var = script['name'].replace('-', '_').replace('.', '_')
            task_defs.append(f"""
    {task_var}_task = DockerOperator(
        task_id="{script['name']}",
        image='semt-pipeline:latest',
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
        network_mode='semt_pipeline_network',
    )""")
            
            if i == 0:
                dependencies.append(f"    find_input >> {task_var}_task")
            else:
                prev_var = scripts[i-1]['name'].replace('-', '_').replace('.', '_')
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
from docker.types import Mount

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
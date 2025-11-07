# =============================================================================
# src/nb2scripts/cli.py - Full CLI with subcommands
# =============================================================================

"""
nb2scripts - Convert notebooks to SemT pipeline scripts and optional Airflow DAG,
and scaffold a local dockerized execution environment.

Subcommands:
  - convert   : Generate scripts and DAG from a notebook
  - init-env  : Scaffold a local stack (Airflow + runner + backend)
  - stack     : Manage the local stack (up/down)
"""
from __future__ import annotations

import argparse
import sys
import os
import logging
import shutil
import subprocess
from pathlib import Path
from importlib import resources as importlib_resources

from .loader import NotebookLoader
from .classifier import OperationClassifier
from .renderer import ScriptRenderer
from .writer import ScriptWriter
from .script_grouper import SemanticScriptGrouper
from .dag_generator import DAGGenerator

# Package folder containing stack runtime templates
# Ensure you have: src/nb2scripts/runtime_templates/ with the named files
import nb2scripts.runtime_templates as runtime_templates_pkg


def _copy_runtime_templates(target_dir: Path) -> None:
    """Copy docker-compose/Dockerfiles/.env.example into target."""
    target_dir.mkdir(parents=True, exist_ok=True)

    files = [
        "docker-compose.yml",
        "docker-compose.airflow.yml",
        "Dockerfile.python-runner",
        "Dockerfile.airflow",
        "entrypoint.sh",
        ".env.example",
    ]

    for name in files:
        with importlib_resources.as_file(
            importlib_resources.files(runtime_templates_pkg) / name
        ) as source_path:
            shutil.copyfile(source_path, target_dir / name)

    # Create directories expected by the stack and DAG/scripts
    for folder in ["dags", "scripts", "logs", "plugins", "data"]:
        (target_dir / folder).mkdir(exist_ok=True)


def cmd_init_env(args: argparse.Namespace) -> None:
    """Scaffold a local stack environment."""
    target = Path(args.target)
    _copy_runtime_templates(target)
    print(f"✅ Environment scaffolded at: {target.absolute()}")
    print("Next:")
    print(f"  - Put your generated scripts in: {target}/scripts")
    print(f"  - Put your generated DAGs in:    {target}/dags")
    print(f"  - Copy .env.example to .env and adjust credentials in: {target}")


def cmd_stack(args: argparse.Namespace) -> None:
    """Manage the local stack using docker compose."""
    target = Path(args.target).absolute()
    compose_cmd = [
        "docker",
        "compose",
        "-f",
        str(target / "docker-compose.yml"),
        "-f",
        str(target / "docker-compose.airflow.yml"),
    ]

    if args.action == "up":
        subprocess.run(compose_cmd + ["up", "-d", "--build"], check=True)
        print("✅ Stack is up:")
        print("   - Airflow Webserver: http://localhost:8080")
        print("   - Backend (node-server-api): http://localhost:3003")
    elif args.action == "down":
        subprocess.run(compose_cmd + ["down"], check=True)
        print("🛑 Stack stopped")
    else:
        print("Unknown action; use up|down")
        sys.exit(1)


def cmd_convert(args: argparse.Namespace) -> None:
    """Convert a notebook into scripts and optionally an Airflow DAG."""
    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    # Load notebook
    print(f"🔄 Loading notebook: {args.notebook}")
    loader = NotebookLoader()
    cells = loader.load_notebook(args.notebook)
    print(f"📓 Loaded {len(cells)} cells")

    # Classify operations
    print("🔍 Classifying operations...")
    classifier = OperationClassifier()
    operations = classifier.classify(cells)

    if not operations:
        print("❌ No operations found")
        sys.exit(1)

    print(f"📋 Found {len(operations)} operations")

    # Group operations into scripts
    print("📦 Grouping operations...")
    grouper = SemanticScriptGrouper()
    scripts = grouper.group_operations_intelligently(operations, args.max_scripts)

    print(f"📝 Generating {len(scripts)} scripts")

    # Render scripts
    print("🏗️ Rendering scripts...")
    renderer = ScriptRenderer()
    rendered_contents = [renderer.render_script(script) for script in scripts]

    # Write scripts and support files
    print("💾 Writing scripts...")
    output_dir = Path(args.output_dir)
    writer = ScriptWriter(str(output_dir))
    written_files = writer.write_all_scripts(scripts, rendered_contents)
    writer.create_support_files()
    writer.create_requirements_file()
    writer.create_readme(scripts)

    # Generate DAG if requested
    if args.generate_dag:
        print("🏗️ Generating DAG...")
        dag_generator = DAGGenerator()
        # Allow overriding image and network for DockerOperator
        dag_generator.docker_image = args.docker_image or "semt-pipeline:latest"
        dag_generator.docker_network = args.docker_network or "app_network"
        dag_file = dag_generator.generate_dag(
            scripts, output_dir, args.dag_name
        )
        print(f"✅ Generated DAG: {dag_file}")

    print(f"\n🎉 Successfully generated {len(written_files)} scripts!")
    print(f"📁 Output: {output_dir.absolute()}")


def main() -> None:
    """
    Entry point supporting subcommands:
      nb2scripts convert ...
      nb2scripts init-env ...
      nb2scripts stack up|down ...
    """
    parser = argparse.ArgumentParser(
        prog="nb2scripts",
        description="Convert notebooks to SemT pipeline scripts and DAGs, and scaffold a local stack",
    )
    subparsers = parser.add_subparsers(dest="cmd")

    # convert
    p_convert = subparsers.add_parser(
        "convert", help="Convert notebook to scripts and optionally an Airflow DAG"
    )
    p_convert.add_argument("notebook", help="Path to notebook (.ipynb)")
    p_convert.add_argument(
        "-o", "--output-dir", default="scripts", help="Output directory for scripts"
    )
    p_convert.add_argument(
        "--max-scripts", type=int, help="Maximum scripts to generate (optional)"
    )
    p_convert.add_argument(
        "--generate-dag", action="store_true", help="Generate Airflow DAG"
    )
    p_convert.add_argument("--dag-name", help="Custom DAG name")
    p_convert.add_argument(
        "--docker-image",
        help="Image used by DockerOperator (default: semt-pipeline:latest)",
    )
    p_convert.add_argument(
        "--docker-network",
        help="Network used by DockerOperator (default: app_network)",
    )
    p_convert.add_argument(
        "--verbose", "-v", action="store_true", help="Verbose output"
    )
    p_convert.set_defaults(func=cmd_convert)

    # init-env
    p_init = subparsers.add_parser(
        "init-env",
        help="Scaffold a local dockerized Airflow + runner + backend stack (to run your generated scripts)",
    )
    p_init.add_argument(
        "--target", default="stack", help="Target folder (default: stack)"
    )
    p_init.set_defaults(func=cmd_init_env)

    # stack
    p_stack = subparsers.add_parser(
        "stack", help="Manage local stack with docker compose"
    )
    p_stack.add_argument("action", choices=["up", "down"])
    p_stack.add_argument("--target", default="stack")
    p_stack.set_defaults(func=cmd_stack)

    # Backward compatibility: allow "nb2scripts notebook.ipynb ..." without subcommand.
    # If first arg looks like a notebook path, auto-route to 'convert'.
    if len(sys.argv) > 1 and sys.argv[1].endswith(".ipynb"):
        argv = ["convert"] + sys.argv[1:]
        args = parser.parse_args(argv)
    else:
        args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    try:
        args.func(args)
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed: {e}")
        sys.exit(e.returncode)
    except Exception as e:
        print(f"❌ Error: {e}")
        if getattr(args, "verbose", False):
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
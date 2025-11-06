# =============================================================================
# tools/nb2scripts/cli.py - SIMPLIFIED
# =============================================================================

"""
Simplified CLI for deterministic nb2scripts.
"""
import argparse
import sys
import os
import logging
from pathlib import Path

from .loader import NotebookLoader
from .classifier import OperationClassifier
from .renderer import ScriptRenderer
from .writer import ScriptWriter
from .script_grouper import SemanticScriptGrouper
from .dag_generator import DAGGenerator

def main():
    parser = argparse.ArgumentParser(
        description="Convert Jupyter notebooks to production scripts (Deterministic)"
    )
    parser.add_argument("notebook", help="Path to notebook (.ipynb)")
    parser.add_argument("-o", "--output-dir", default="scripts", help="Output directory")
    parser.add_argument("--max-scripts", type=int, help="Maximum scripts to generate")
    parser.add_argument("--generate-dag", action="store_true", help="Generate Airflow DAG")
    parser.add_argument("--dag-name", help="Custom DAG name")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    
    try:
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
        
        # Group into scripts
        print("📦 Grouping operations...")
        grouper = SemanticScriptGrouper()
        
        if args.max_scripts:
            scripts = grouper.group_operations_intelligently(operations, args.max_scripts)
        else:
            scripts = grouper.group_operations_intelligently(operations)
        
        print(f"📝 Generating {len(scripts)} scripts")
        
        # Render scripts
        print("🏗️ Rendering scripts...")
        renderer = ScriptRenderer()
        rendered_contents = [renderer.render_script(script) for script in scripts]
        
        # Write scripts
        print("💾 Writing scripts...")
        writer = ScriptWriter(args.output_dir)
        written_files = writer.write_all_scripts(scripts, rendered_contents)
        writer.create_support_files()
        
        # Generate DAG if requested
        if args.generate_dag:
            print("🏗️ Generating DAG...")
            dag_generator = DAGGenerator()
            dag_file = dag_generator.generate_dag(
                scripts,
                Path(args.output_dir),
                args.dag_name
            )
            print(f"✅ Generated DAG: {dag_file}")
        
        # Create additional files
        writer.create_requirements_file()
        writer.create_readme(scripts)
        
        print(f"\n🎉 Successfully generated {len(written_files)} scripts!")
        print(f"📁 Output: {Path(args.output_dir).absolute()}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
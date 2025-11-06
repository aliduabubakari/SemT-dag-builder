"""
File writing utilities for generated scripts.
"""
import os
import stat
from pathlib import Path
from typing import List
from .schema import Script

class ScriptWriter:
    """Handles writing generated scripts to disk."""
    
    def __init__(self, output_dir: str = "scripts"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def write_script(self, script: Script, content: str) -> Path:
        """
        Write a script to disk.
        
        Args:
            script: Script object
            content: Script content
            
        Returns:
            Path to written file
        """
        file_path = self.output_dir / script.filename
        
        # Write content
        file_path.write_text(content, encoding='utf-8')
        
        # Make executable
        current_permissions = file_path.stat().st_mode
        file_path.chmod(current_permissions | stat.S_IEXEC)
        
        return file_path
    
    def write_all_scripts(self, scripts: List[Script], contents: List[str]) -> List[Path]:
        """
        Write multiple scripts to disk.
        
        Args:
            scripts: List of Script objects
            contents: List of script contents
            
        Returns:
            List of paths to written files
        """
        if len(scripts) != len(contents):
            raise ValueError("Number of scripts and contents must match")
        
        written_files = []
        for script, content in zip(scripts, contents):
            file_path = self.write_script(script, content)
            written_files.append(file_path)
            print(f"✅ Wrote {file_path}")
        
        return written_files
    
    def create_requirements_file(self) -> Path:
        """Create a requirements.txt file for the generated scripts."""
        requirements = [
            # Core runtime deps for your generated scripts
            "pandas>=1.3.0",
            "requests>=2.31.0",
            "fake-useragent>=1.3.0",
            "PyJWT>=2.8.0",
            "python-dateutil>=2.8.2",
            # Install semt_py directly from your repo
            "git+https://github.com/I2Tunimib/I2T-library.git#egg=semt-py",
            # Optional: useful for logging/formatting but not strictly required
            # "jinja2>=3.1.3",
        ]
        req_file = self.output_dir / "requirements.txt"
        req_file.write_text("\n".join(requirements) + "\n")
        return req_file
    
    def create_support_files(self) -> None:
        """Create helper modules used by generated scripts."""
        (self.output_dir / "__init__.py").write_text("", encoding="utf-8")

        utils_dir = self.output_dir / "utils"
        utils_dir.mkdir(parents=True, exist_ok=True)
        (utils_dir / "__init__.py").write_text("", encoding="utf-8")

        file_utils_py = utils_dir / "file_utils.py"
        file_utils_py.write_text(
            """import os
    import json
    import re
    import datetime
    from typing import Optional, Dict, Any

    class PipelineFileManager:
        \"""
        Minimal local state manager to pass JSON between stages.
        - Each run gets its own directory: {DATA_DIR}/{RUN_ID}/
        - save_current_state writes a JSON file and updates 'current.path'
        - load_current_state uses INPUT_JSON_PATH env or 'current.path'
        \"""

        def __init__(self, data_dir: str, run_id: str):
            self.data_dir = data_dir
            self.run_id = str(run_id)
            self.run_dir = os.path.join(self.data_dir, self.run_id)
            os.makedirs(self.run_dir, exist_ok=True)

        def _marker_path(self) -> str:
            return os.path.join(self.run_dir, "current.path")

        def get_current_file_path(self) -> Optional[str]:
            if os.path.exists(self._marker_path()):
                with open(self._marker_path(), "r", encoding="utf-8") as f:
                    return f.read().strip()
            return None

        def load_current_state(self) -> Dict[str, Any]:
            env_path = os.environ.get("INPUT_JSON_PATH")
            path = env_path or self.get_current_file_path()

            if not path or not os.path.exists(path):
                raise RuntimeError("No JSON state found. Set INPUT_JSON_PATH or ensure a previous stage saved the state.")

            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

        def _slugify(self, s: str) -> str:
            return re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_").lower()

        def save_current_state(self, data: Dict[str, Any], filename: Optional[str] = None) -> str:
            if filename is None:
                filename = f"state_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            full_path = os.path.join(self.run_dir, filename)
            with open(full_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            with open(self._marker_path(), "w", encoding="utf-8") as marker:
                marker.write(full_path)
            return full_path

        def save_stage_snapshot(self, tag: str, data: Dict[str, Any], stage_number: int) -> str:
            filename = f"stage{int(stage_number):02d}_{self._slugify(tag)}.json"
            return self.save_current_state(data, filename)
    """,
            encoding="utf-8",
        )
       
    def create_readme(self, scripts: List[Script]) -> Path:
        """Create a README file explaining the generated scripts."""
        readme_content = f"""# Generated Scripts

This directory contains {len(scripts)} auto-generated scripts from a Jupyter notebook.

## Scripts

"""
        
        for script in sorted(scripts, key=lambda s: s.stage):
            readme_content += f"### {script.filename}\n"
            readme_content += f"- Stage: {script.stage}\n"
            readme_content += f"- Operations: {len(script.operations)}\n"
            for op in script.operations:
                readme_content += f"  - {op.op_type}: {op.name}\n"
            readme_content += "\n"
        
        readme_content += """
## Usage

Each script can be run independently:

```bash
python 01_load_table.py
python 02_reconcile_poi.py
# ... etc
```

## Environment Variables

Set these environment variables before running:

```bash
export API_BASE_URL='http://localhost:3003'
export API_USERNAME='your_username'
export API_PASSWORD='your_password'
export DATASET_ID='5'
export DATA_DIR='/path/to/data'
export RUN_ID='$(date +%Y%m%d_%H%M%S)'
```

## Dependencies

Install dependencies with:

```bash
pip install -r requirements.txt
```
"""
        
        readme_file = self.output_dir / "README.md"
        readme_file.write_text(readme_content)
        return readme_file
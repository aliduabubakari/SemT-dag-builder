"""
Jinja2-based template rendering - FIXED VERSION.
"""
import datetime
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape
from .schema import Script, Operation

class ScriptRenderer:
    """Renders scripts using deterministic templates."""
    
    def __init__(self, template_dir: str = None):
        if template_dir is None:
            template_dir = Path(__file__).parent / "templates"
        
        self.env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=select_autoescape(),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Add custom filters
        self.env.filters['clean_name'] = self._clean_name_filter
        self.env.filters['regex_replace'] = self._regex_replace_filter  # ADD THIS
    
    def _regex_replace_filter(self, text: str, pattern: str, replacement: str = '') -> str:
        """Custom Jinja2 filter for regex replacement."""
        import re
        return re.sub(pattern, replacement, text)
    
    def render_script(self, script: Script) -> str:
        """
        Render a complete script using deterministic templates.
        """
        # Render each operation with its specific template
        rendered_operations = []
        
        for i, operation in enumerate(script.operations, 1):
            # Get parameters from meta
            params = operation.meta.get('params', {})
            
            # If params is empty, try to extract from other meta fields
            if not params:
                params = {
                    'column_name': operation.meta.get('column_name'),
                    'reconciliator_id': operation.meta.get('reconciliator_id'),
                    'extender_id': operation.meta.get('extender_id'),
                    'properties': operation.meta.get('properties'),
                    'optional_columns': operation.meta.get('optional_columns', []),
                    'deduplicate': operation.meta.get('deduplicate', False),
                    'output_file': operation.meta.get('output_file')
                }
                # Remove None values
                params = {k: v for k, v in params.items() if v is not None}
            
            # Render operation using appropriate template
            op_template_name = f"operations/{operation.op_type}.j2"
            
            try:
                template = self.env.get_template(op_template_name)
                
                # Prepare operation data
                op_data = {
                    'op': operation,
                    'index': i,
                    'params': params  # ← This is the key fix
                }
                
                rendered = template.render(**op_data)
                
                # Store rendered template
                operation.rendered_template = rendered
                rendered_operations.append(operation)
                
            except Exception as e:
                print(f"⚠️ Warning: Could not render {operation.op_type}: {e}")
                print(f"   Params available: {params}")
                # Create a minimal fallback rendering
                operation.rendered_template = self._create_fallback_operation(operation, i, params)
                rendered_operations.append(operation)
        
        # Render the main script structure
        script_template = self.env.get_template("script_base.j2")
        
        script_data = {
            'script': {
                'name': script.name,
                'filename': script.filename,
                'stage': script.stage,
                'safe_name': self._clean_name_filter(script.name)
            },
            'operations': rendered_operations,
            'timestamp': datetime.datetime.now()
        }
        
        return script_template.render(**script_data)
    
    def _create_fallback_operation(self, operation: Operation, index: int, params: dict) -> str:
        """Create a minimal fallback operation function."""
        return f'''def run_{operation.op_type}_operation_{index}(config: Config, file_manager: PipelineFileManager):
    """
    {operation.op_type.upper()} Operation: {operation.name}
    WARNING: This is a fallback rendering - template failed to load
    """
    logger.info("=== {operation.op_type.upper()} OPERATION: {operation.name} ===")
    logger.warning("⚠️  Using fallback implementation - please check operation")
    
    # Load current state
    table_data = file_manager.load_current_state()
    if not table_data:
        raise RuntimeError("No table_data from previous stage")
    
    # TODO: Implement {operation.op_type} operation
    # Parameters detected: {list(params.keys())}
    
    logger.info("✅ Operation completed (fallback)")
    return table_data
'''
    
    def _clean_name_filter(self, text: str) -> str:
        """Clean text for use as function/variable names."""
        import re
        text = re.sub(r'[^\w]', '_', text)
        text = re.sub(r'_+', '_', text)
        return text.strip('_').lower()

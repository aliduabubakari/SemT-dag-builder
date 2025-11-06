"""
Enhanced parameter extractor with proper LOAD detection.
"""
import re
import json
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

@dataclass
class ExtractedOperation:
    """Represents an operation with extracted parameters."""
    op_type: str
    name: str
    params: Dict[str, Any]
    source_cell: int
    confidence: float = 1.0
    raw_source: str = ""

class ParameterExtractor:
    """Extract parameters with improved load detection."""
    
    # Noise patterns - BE MORE SPECIFIC to avoid filtering load operations
    NOISE_PATTERNS = [
        r'^\s*#.*This is a base file',
        r'^\s*from semt_py import\s+\w+Manager\s*$',  # Only if it's JUST the import
        r'^import semt_py\s*$',  # Only if it's JUST the import
        r'def get_input_with_default',
        r'^\s*base_url\s*=\s*get_input',
        # DON'T filter these if they appear with actual operations:
        # - table_manager.add_table() is an operation!
        # - pd.read_csv() is an operation!
    ]
    
    # Patterns for parameters
    PATTERNS = {
        'load': {
            'table_name': r'table_name\s*=\s*["\']([^"\']+)["\']',
            'table_name_alt': r'table_name\s*=\s*get_input_with_default\([^,]+,\s*["\']([^"\']+)["\']\)',
            'dataset_id': r'dataset_id\s*=\s*["\']([^"\']+)["\']',
            'dataset_id_alt': r'dataset_id\s*=\s*get_input_with_default\([^,]+,\s*["\']([^"\']+)["\']\)',
            'columns_to_delete': r'columns_to_delete\s*=\s*(\[[^\]]*\])',
            'csv_file': r'(?:pd\.)?read_csv\(["\']([^"\']+)["\']'
        },
        'reconcile': {
            'column_name': r'column_name\s*=\s*["\']([^"\']+)["\']',
            'reconciliator_id': r'reconciliator_id\s*=\s*["\']([^"\']+)["\']',
            'optional_columns': r'optional_columns\s*=\s*(\[[^\]]*\])',
            'deduplicate': r'deduplicate\s*=\s*(True|False)'
        },
        'extend': {
            'column_name': r'column_name\s*=\s*["\']([^"\']+)["\']',
            'extender_id': r'extender_id\s*=\s*["\']([^"\']+)["\']',
            'properties': r'properties\s*=\s*["\']([^"\']+)["\']',
            'other_params': r'other_params\s*=\s*(\{[^}]*\})',
            'deduplicate': r'deduplicate\s*=\s*(True|False)'
        },
        'download': {
            'output_file': r'output_file\s*=\s*["\']([^"\']+)["\']'
        }
    }

    # Operation signatures - PRIORITIZE LOAD DETECTION
    OPERATION_SIGNATURES = {
        'load': [
            # Most specific patterns first
            r'\.add_table\(',
            r'table_manager\.add_table\(',
            r'pd\.read_csv\(',
            r'read_csv\(["\']',
            r'import pandas',
        ],
        'reconcile': [
            r'reconciliation_manager\.reconcile\(',
            r'\.reconcile\(',
        ],
        'extend': [
            r'extension_manager\.extend_column\(',
            r'\.extend_column\(',
        ],
        'download': [
            r'utility\.download_csv\(',
            r'\.download_csv\(',
        ]
    }
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def is_noise_cell(self, cell_source: str) -> bool:
        """Check if cell is noise - IMPROVED TO NOT FILTER LOAD OPERATIONS."""
        if not cell_source.strip():
            return True
        
        # First check if it contains operation signatures
        for op_type, signatures in self.OPERATION_SIGNATURES.items():
            for signature in signatures:
                if re.search(signature, cell_source):
                    # This cell has an operation, it's NOT noise
                    return False
        
        # Now check noise patterns
        for pattern in self.NOISE_PATTERNS:
            if re.search(pattern, cell_source, re.MULTILINE | re.IGNORECASE):
                return True
        
        # Cell is only comments
        lines = [line.strip() for line in cell_source.split('\n') if line.strip()]
        if all(line.startswith('#') for line in lines):
            return True
        
        return False
    
    def identify_operation_type(self, cell_source: str) -> Optional[str]:
        """Identify operation type - CHECK LOAD FIRST."""
        if self.is_noise_cell(cell_source):
            return None
        
        # Check load FIRST since it's often missed
        for signature in self.OPERATION_SIGNATURES['load']:
            if re.search(signature, cell_source):
                self.logger.debug(f"Detected LOAD operation via signature: {signature}")
                return 'load'
        
        # Then check other operation types
        for op_type in ['reconcile', 'extend', 'download']:
            for signature in self.OPERATION_SIGNATURES[op_type]:
                if re.search(signature, cell_source):
                    self.logger.debug(f"Detected {op_type.upper()} operation")
                    return op_type
        
        return None
    
    def extract_parameters(self, cell_source: str, op_type: str) -> Dict[str, Any]:
        """Extract parameters from cell source."""
        if op_type not in self.PATTERNS:
            return {}

        params = {}
        patterns = self.PATTERNS[op_type]

        # Pass 1: capture all matches
        raw = {}
        for param_name, pattern in patterns.items():
            match = re.search(pattern, cell_source)
            if match:
                raw[param_name] = self._safe_eval(match.group(1))

        # Normalize alternates for 'load'
        if op_type == 'load':
            if 'table_name' in raw:
                params['table_name'] = raw['table_name']
            elif 'table_name_alt' in raw:
                params['table_name'] = raw['table_name_alt']

            if 'dataset_id' in raw:
                params['dataset_id'] = raw['dataset_id']
            elif 'dataset_id_alt' in raw:
                params['dataset_id'] = raw['dataset_id_alt']

            # Keep other recognized items
            for key in ['columns_to_delete', 'csv_file']:
                if key in raw:
                    params[key] = raw[key]
            return params

        # Other op types keep what we found
        for k, v in raw.items():
            # Skip *_alt keys for other ops (not used)
            if k.endswith('_alt'):
                continue
            params[k] = v

        return params
    
    def _safe_eval(self, value: str) -> Any:
        """Safely evaluate Python literals."""
        value = value.strip()
        
        if value in ('True', 'False'):
            return value == 'True'
        if value == 'None':
            return None
        
        if value.startswith('[') and value.endswith(']'):
            content = value[1:-1].strip()
            if not content:
                return []
            items = []
            for item in content.split(','):
                item = item.strip().strip("'\"")
                if item:
                    items.append(item)
            return items
        
        if value.startswith('{') and value.endswith('}'):
            try:
                return json.loads(value.replace("'", '"'))
            except:
                return {}
        
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except:
            pass
        
        if (value.startswith('"') and value.endswith('"')) or \
           (value.startswith("'") and value.endswith("'")):
            return value[1:-1]
        
        return value
    
    def extract_from_cell(self, cell, cell_index: int) -> Optional[ExtractedOperation]:
        """Extract operation from cell."""
        if cell.kind != 'code':
            return None
        
        source = cell.source
        
        # Don't filter noise yet - let identify_operation_type decide
        op_type = self.identify_operation_type(source)
        if not op_type:
            return None
        
        params = self.extract_parameters(source, op_type)
        name = self._generate_operation_name(op_type, params, cell_index)
        
        return ExtractedOperation(
            op_type=op_type,
            name=name,
            params=params,
            source_cell=cell_index,
            confidence=self._calculate_confidence(params, op_type),
            raw_source=source
        )
    
    def _generate_operation_name(self, op_type: str, params: Dict, cell_index: int) -> str:
        """Generate descriptive name."""
        if op_type == 'load':
            # Try to get a meaningful name
            csv_file = params.get('csv_file', '')
            table_name = params.get('table_name', '')
            
            if csv_file:
                # Extract filename without path and extension
                csv_name = csv_file.split('/')[-1].replace('.csv', '')
                return f"Load {csv_name}"
            elif table_name:
                return f"Load {table_name}"
            else:
                return "Load data"
        
        elif op_type == 'reconcile':
            column = params.get('column_name', 'column')
            reconciliator = params.get('reconciliator_id', 'reconciliator')
            return f"Reconcile {column} by {reconciliator}"
        
        elif op_type == 'extend':
            column = params.get('column_name', 'column')
            extender = params.get('extender_id', 'extender')
            return f"Extend {column} by {extender}"
        
        elif op_type == 'download':
            output = params.get('output_file', 'output.csv')
            return f"Download {output}"
        
        return f"{op_type} operation {cell_index}"
    
    def _calculate_confidence(self, params: Dict, op_type: str) -> float:
        """Calculate confidence score."""
        if op_type == 'load':
            # For load, we're confident if we found either table_name OR csv_file
            if params.get('table_name') or params.get('csv_file'):
                return 1.0
            # Even without params, if we detected add_table or read_csv, we're fairly confident
            return 0.8
        
        required_params = {
            'reconcile': ['column_name', 'reconciliator_id'],
            'extend': ['column_name', 'extender_id', 'properties'],
            'download': []
        }
        
        required = required_params.get(op_type, [])
        if not required:
            return 1.0
        
        found = sum(1 for param in required if param in params)
        return found / len(required)
    
    def extract_from_notebook(self, cells) -> List[ExtractedOperation]:
        """Extract all operations from notebook."""
        operations = []
        
        for i, cell in enumerate(cells):
            op = self.extract_from_cell(cell, i)
            if op and op.confidence >= 0.5:
                operations.append(op)
                self.logger.info(f"✓ Cell {i}: {op.op_type} - {op.name} (confidence: {op.confidence:.0%})")
            elif op:
                self.logger.info(f"✗ Cell {i}: {op.op_type} - Low confidence ({op.confidence:.0%}), skipping")
        
        return operations
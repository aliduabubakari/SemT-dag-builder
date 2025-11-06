"""
Enhanced script grouping with better operation type recognition.
"""
import re
from typing import List, Dict, Any
from .schema import Operation, Script

class SemanticScriptGrouper:
    """Groups operations with improved semantic understanding."""
    
    def __init__(self):
        self.operation_keywords = {
            'reconcile': {
                'poi': ['point_of_interest', 'poi', 'point', 'interest'],
                'place': ['place', 'location', 'geographic', 'geo'],
            },
            'extend': {
                'poi': ['point_of_interest', 'poi', 'point', 'interest'],
                'place': ['place', 'location', 'geographic', 'geo'],
            }
        }
    
    def group_operations_intelligently(self, operations: List[Operation], max_scripts: int = None) -> List[Script]:
        """Group operations with numbered script names."""
        
        load_ops = [op for op in operations if op.op_type == 'load']
        reconcile_ops = [op for op in operations if op.op_type == 'reconcile']
        extend_ops = [op for op in operations if op.op_type == 'extend']
        download_ops = [op for op in operations if op.op_type == 'download']
        
        scripts = []
        script_counter = 1
        
        # Use stage_ prefix instead of numbers
        if load_ops:
            for op in load_ops:
                scripts.append(Script(
                    name=f"stage{script_counter:02d}_load_table",
                    stage=script_counter,
                    operations=[op]
                ))
                script_counter += 1
        
        for op in reconcile_ops:
            subject = self._identify_operation_subject(op, 'reconcile')
            scripts.append(Script(
                name=f"stage{script_counter:02d}_reconcile_{subject}",
                stage=script_counter,
                operations=[op]
            ))
            script_counter += 1
        
        for op in extend_ops:
            subject = self._identify_operation_subject(op, 'extend')
            scripts.append(Script(
                name=f"stage{script_counter:02d}_extend_{subject}",
                stage=script_counter,
                operations=[op]
            ))
            script_counter += 1
        
        for op in download_ops:
            output_file = op.meta.get('params', {}).get('output_file', 'output.csv')
            clean_name = output_file.replace('.csv', '').replace('.', '_').lower()
            scripts.append(Script(
                name=f"stage{script_counter:02d}_download_{clean_name}",
                stage=script_counter,
                operations=[op]
            ))
            script_counter += 1
        
        return scripts
    
    def _identify_operation_subject(self, operation: Operation, op_type: str) -> str:
        """Identify subject of operation from column name or description."""
        
        # Check column_name in params
        params = operation.meta.get('params', {})
        column_name = params.get('column_name', '').lower()
        
        # Also check direct meta fields
        if not column_name:
            column_name = operation.meta.get('column_name', '').lower()
        
        # Check operation name
        op_name = operation.name.lower()
        
        # Combined text for analysis
        text = f"{column_name} {op_name}"
        
        # Match against keywords
        if op_type in self.operation_keywords:
            for subject, keywords in self.operation_keywords[op_type].items():
                for keyword in keywords:
                    if keyword in text:
                        return subject
        
        # Fallback: use column name if available
        if column_name:
            return column_name.replace(' ', '_')[:20]  # Truncate long names
        
        # Last resort
        return 'data'
    
    def _consolidate_scripts(self, scripts: List[Script], max_scripts: int) -> List[Script]:
        """Consolidate scripts to meet max_scripts limit."""
        if len(scripts) <= max_scripts:
            return scripts
        
        # Strategy: Keep first (load) and last (download) separate, consolidate middle
        result = []
        
        # Keep first script
        result.append(scripts[0])
        
        # Consolidate middle scripts
        middle_scripts = scripts[1:-1]
        ops_per_script = len(middle_scripts) // (max_scripts - 2) + 1
        
        for i in range(0, len(middle_scripts), ops_per_script):
            batch = middle_scripts[i:i+ops_per_script]
            all_ops = []
            for s in batch:
                all_ops.extend(s.operations)
            
            # Generate consolidated name
            op_types = list(set(op.op_type for s in batch for op in s.operations))
            name = f"{len(result)+1:02d}_{'_'.join(op_types)}_operations"
            
            result.append(Script(
                name=name,
                stage=len(result) + 1,
                operations=all_ops
            ))
        
        # Keep last script
        result.append(Script(
            name=f"{len(result)+1:02d}_{scripts[-1].name.split('_', 1)[-1]}",
            stage=len(result) + 1,
            operations=scripts[-1].operations
        ))
        
        return result
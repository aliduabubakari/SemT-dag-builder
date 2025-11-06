# =============================================================================
# tools/nb2scripts/classifier.py - UPDATED
# =============================================================================

"""
Cell classification and operation detection - SIMPLIFIED VERSION.
"""
import logging
from typing import List
from .schema import Cell, Operation
from .param_extractor import ParameterExtractor, ExtractedOperation

class OperationClassifier:
    """Classifies notebook cells into operations using parameter extraction."""
    
    def __init__(self):
        self.extractor = ParameterExtractor()
        self.logger = logging.getLogger(__name__)
    
    def classify(self, cells: List[Cell]) -> List[Operation]:
        """
        Classify cells into operations.
        
        Args:
            cells: List of cells to classify
            
        Returns:
            List of Operation objects
        """
        self.logger.info(f"🔍 Classifying {len(cells)} cells...")
        
        # Extract operations using parameter extractor
        extracted_ops = self.extractor.extract_from_notebook(cells)
        
        # Convert to Operation objects
        operations = []
        for i, ext_op in enumerate(extracted_ops, 1):
            operation = Operation(
                op_type=ext_op.op_type,
                name=ext_op.name,
                cells=[],  # We don't need to store cells anymore
                meta={
                    'params': ext_op.params,
                    'confidence': ext_op.confidence,
                    'source_cell': ext_op.source_cell,
                    'raw_source': ext_op.raw_source,
                    **ext_op.params  # Flatten params into meta for easy access
                },
                order=i
            )
            operations.append(operation)
        
        self.logger.info(f"📋 Found {len(operations)} operations")
        for op in operations:
            self.logger.info(f"   - {op.op_type}: {op.name} (confidence: {op.meta.get('confidence', 0):.0%})")
        
        return operations
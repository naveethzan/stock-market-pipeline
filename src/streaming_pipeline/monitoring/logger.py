"""
Structured logging with layer tracking for medallion architecture.

Provides comprehensive logging capabilities with:
- Layer-aware logging (Bronze, Silver, Gold)
- Correlation ID tracking
- Performance metrics
- Error context preservation
"""

import logging
import json
import time
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
from contextlib import contextmanager
from dataclasses import dataclass, asdict


class MedallionLayer(Enum):
    """Medallion architecture layers."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    INGESTION = "ingestion"
    PROCESSING = "processing"


@dataclass
class LogContext:
    """Context information for structured logging."""
    correlation_id: str
    layer: MedallionLayer
    component: str
    operation: str
    timestamp: str
    metadata: Dict[str, Any]


class LayerTracker:
    """Tracks data flow across medallion architecture layers."""
    
    def __init__(self):
        self._active_contexts: Dict[str, LogContext] = {}
        self._layer_transitions: List[Dict[str, Any]] = []
    
    def start_layer_processing(
        self, 
        layer: MedallionLayer, 
        component: str, 
        operation: str,
        correlation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Start tracking processing in a specific layer."""
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
        
        context = LogContext(
            correlation_id=correlation_id,
            layer=layer,
            component=component,
            operation=operation,
            timestamp=datetime.utcnow().isoformat(),
            metadata=metadata or {}
        )
        
        self._active_contexts[correlation_id] = context
        
        # Track layer transition
        self._layer_transitions.append({
            'correlation_id': correlation_id,
            'layer': layer.value,
            'component': component,
            'operation': operation,
            'event': 'start',
            'timestamp': context.timestamp,
            'metadata': metadata or {}
        })
        
        return correlation_id
    
    def end_layer_processing(
        self, 
        correlation_id: str, 
        success: bool = True,
        error_details: Optional[Dict[str, Any]] = None
    ):
        """End tracking for a specific correlation ID."""
        if correlation_id in self._active_contexts:
            context = self._active_contexts[correlation_id]
            
            # Track completion
            self._layer_transitions.append({
                'correlation_id': correlation_id,
                'layer': context.layer.value,
                'component': context.component,
                'operation': context.operation,
                'event': 'end',
                'timestamp': datetime.utcnow().isoformat(),
                'success': success,
                'error_details': error_details
            })
            
            del self._active_contexts[correlation_id]
    
    def get_active_contexts(self) -> Dict[str, LogContext]:
        """Get all active processing contexts."""
        return self._active_contexts.copy()
    
    def get_layer_transitions(self) -> List[Dict[str, Any]]:
        """Get all layer transitions for lineage tracking."""
        return self._layer_transitions.copy()
    
    def clear_transitions(self):
        """Clear transition history (useful for testing)."""
        self._layer_transitions.clear()


class PipelineLogger:
    """Enhanced logger with medallion architecture awareness."""
    
    def __init__(self, name: str, layer_tracker: Optional[LayerTracker] = None):
        self.logger = logging.getLogger(name)
        self.layer_tracker = layer_tracker or LayerTracker()
        
        # Configure structured logging format
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _create_log_entry(
        self,
        level: str,
        message: str,
        correlation_id: Optional[str] = None,
        layer: Optional[MedallionLayer] = None,
        component: Optional[str] = None,
        operation: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error_details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create structured log entry."""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': level,
            'message': message,
            'correlation_id': correlation_id,
            'layer': layer.value if layer else None,
            'component': component,
            'operation': operation,
            'metadata': metadata or {},
            'error_details': error_details
        }
        
        # Remove None values
        return {k: v for k, v in log_entry.items() if v is not None}
    
    def info(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        layer: Optional[MedallionLayer] = None,
        component: Optional[str] = None,
        operation: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log info message with structured data."""
        log_entry = self._create_log_entry(
            'INFO', message, correlation_id, layer, component, operation, metadata
        )
        self.logger.info(json.dumps(log_entry))
    
    def error(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        layer: Optional[MedallionLayer] = None,
        component: Optional[str] = None,
        operation: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error_details: Optional[Dict[str, Any]] = None
    ):
        """Log error message with structured data."""
        log_entry = self._create_log_entry(
            'ERROR', message, correlation_id, layer, component, operation, 
            metadata, error_details
        )
        self.logger.error(json.dumps(log_entry))
    
    def warning(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        layer: Optional[MedallionLayer] = None,
        component: Optional[str] = None,
        operation: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log warning message with structured data."""
        log_entry = self._create_log_entry(
            'WARNING', message, correlation_id, layer, component, operation, metadata
        )
        self.logger.warning(json.dumps(log_entry))
    
    @contextmanager
    def layer_context(
        self,
        layer: MedallionLayer,
        component: str,
        operation: str,
        correlation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Context manager for layer processing with automatic tracking."""
        correlation_id = self.layer_tracker.start_layer_processing(
            layer, component, operation, correlation_id, metadata
        )
        
        start_time = time.time()
        
        try:
            self.info(
                f"Starting {operation} in {layer.value} layer",
                correlation_id=correlation_id,
                layer=layer,
                component=component,
                operation=operation,
                metadata=metadata
            )
            
            yield correlation_id
            
            duration = time.time() - start_time
            self.info(
                f"Completed {operation} in {layer.value} layer",
                correlation_id=correlation_id,
                layer=layer,
                component=component,
                operation=operation,
                metadata={**(metadata or {}), 'duration_seconds': duration}
            )
            
            self.layer_tracker.end_layer_processing(correlation_id, success=True)
            
        except Exception as e:
            duration = time.time() - start_time
            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'duration_seconds': duration
            }
            
            self.error(
                f"Failed {operation} in {layer.value} layer: {str(e)}",
                correlation_id=correlation_id,
                layer=layer,
                component=component,
                operation=operation,
                metadata=metadata,
                error_details=error_details
            )
            
            self.layer_tracker.end_layer_processing(
                correlation_id, success=False, error_details=error_details
            )
            
            raise
    
    def log_data_flow(
        self,
        source_layer: MedallionLayer,
        target_layer: MedallionLayer,
        record_count: int,
        correlation_id: str,
        component: str,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log data flow between layers."""
        flow_metadata = {
            'source_layer': source_layer.value,
            'target_layer': target_layer.value,
            'record_count': record_count,
            **(metadata or {})
        }
        
        self.info(
            f"Data flow: {source_layer.value} → {target_layer.value} ({record_count} records)",
            correlation_id=correlation_id,
            component=component,
            operation='data_flow',
            metadata=flow_metadata
        )
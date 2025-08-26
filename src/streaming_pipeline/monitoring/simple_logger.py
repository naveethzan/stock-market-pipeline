"""
Simplified logging with basic layer awareness for medallion architecture.

Provides essential logging capabilities:
- Layer-aware log messages (Bronze, Silver, Gold)
- Correlation ID support for debugging
- Simple structured logging
- Basic pipeline status tracking
"""

import logging
import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum
from contextlib import contextmanager


class MedallionLayer(Enum):
    """Medallion architecture layers."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    INGESTION = "ingestion"
    PROCESSING = "processing"


class SimplePipelineLogger:
    """Simple logger with medallion architecture awareness."""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        
        # Configure simple logging format
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _format_message(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        layer: Optional[MedallionLayer] = None,
        component: Optional[str] = None,
        **kwargs
    ) -> str:
        """Format log message with context."""
        parts = []
        
        if correlation_id:
            parts.append(f"[{correlation_id[:8]}]")
        
        if layer:
            parts.append(f"[{layer.value.upper()}]")
        
        if component:
            parts.append(f"[{component}]")
        
        parts.append(message)
        
        # Add any additional context
        if kwargs:
            context_str = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            parts.append(f"({context_str})")
        
        return " ".join(parts)
    
    def info(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        layer: Optional[MedallionLayer] = None,
        component: Optional[str] = None,
        **kwargs
    ):
        """Log info message with layer context."""
        formatted_message = self._format_message(message, correlation_id, layer, component, **kwargs)
        self.logger.info(formatted_message)
    
    def error(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        layer: Optional[MedallionLayer] = None,
        component: Optional[str] = None,
        error: Optional[Exception] = None,
        **kwargs
    ):
        """Log error message with layer context."""
        if error:
            kwargs['error_type'] = type(error).__name__
            kwargs['error_message'] = str(error)
        
        formatted_message = self._format_message(message, correlation_id, layer, component, **kwargs)
        self.logger.error(formatted_message)
    
    def warning(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        layer: Optional[MedallionLayer] = None,
        component: Optional[str] = None,
        **kwargs
    ):
        """Log warning message with layer context."""
        formatted_message = self._format_message(message, correlation_id, layer, component, **kwargs)
        self.logger.warning(formatted_message)
    
    def debug(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        layer: Optional[MedallionLayer] = None,
        component: Optional[str] = None,
        **kwargs
    ):
        """Log debug message with layer context."""
        formatted_message = self._format_message(message, correlation_id, layer, component, **kwargs)
        self.logger.debug(formatted_message)
    
    @contextmanager
    def layer_context(
        self,
        layer: MedallionLayer,
        component: str,
        operation: str,
        correlation_id: Optional[str] = None
    ):
        """Context manager for layer processing with automatic logging."""
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
        
        start_time = datetime.utcnow()
        
        self.info(
            f"Starting {operation}",
            correlation_id=correlation_id,
            layer=layer,
            component=component,
            operation=operation
        )
        
        try:
            yield correlation_id
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.info(
                f"Completed {operation}",
                correlation_id=correlation_id,
                layer=layer,
                component=component,
                operation=operation,
                duration_seconds=duration
            )
            
        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.error(
                f"Failed {operation}",
                correlation_id=correlation_id,
                layer=layer,
                component=component,
                operation=operation,
                duration_seconds=duration,
                error=e
            )
            raise


def create_logger(name: str) -> SimplePipelineLogger:
    """Create a simple pipeline logger."""
    return SimplePipelineLogger(name)


# Convenience function for backward compatibility
def PipelineLogger(name: str) -> SimplePipelineLogger:
    """Create a pipeline logger (backward compatibility)."""
    return SimplePipelineLogger(name)
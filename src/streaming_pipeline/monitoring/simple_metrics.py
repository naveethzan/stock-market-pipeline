"""
Simple metrics collection for core data engineering monitoring.

Focuses on essential pipeline metrics without complex infrastructure:
- Record processing counts
- Basic error tracking
- Pipeline status monitoring
"""

import json
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict
from enum import Enum

from .simple_logger import MedallionLayer


@dataclass
class SimpleMetric:
    """Basic metric for pipeline monitoring."""
    name: str
    value: float
    timestamp: str
    layer: Optional[str] = None
    component: Optional[str] = None


class SimpleMetricsCollector:
    """Lightweight metrics collector focused on core data engineering metrics."""
    
    def __init__(self):
        # Basic counters for pipeline monitoring
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = defaultdict(float)
        self._metrics_history: Dict[str, list] = defaultdict(list)
        
        # Initialize medallion layer counters
        self._initialize_layer_metrics()
    
    def _initialize_layer_metrics(self):
        """Initialize basic metrics for medallion architecture."""
        for layer in MedallionLayer:
            self._counters[f"{layer.value}_records_processed"] = 0
            self._counters[f"{layer.value}_errors"] = 0
            self._gauges[f"{layer.value}_status"] = 0  # 0=stopped, 1=running, -1=error
    
    def increment_counter(self, name: str, value: float = 1, layer: Optional[str] = None, component: Optional[str] = None):
        """Increment a counter metric."""
        self._counters[name] += value
        
        metric = SimpleMetric(
            name=name,
            value=self._counters[name],
            timestamp=datetime.utcnow().isoformat(),
            layer=layer,
            component=component
        )
        
        self._metrics_history[name].append(metric)
        # Keep only last 100 metrics per counter
        if len(self._metrics_history[name]) > 100:
            self._metrics_history[name].pop(0)
    
    def set_gauge(self, name: str, value: float, layer: Optional[str] = None, component: Optional[str] = None):
        """Set a gauge metric value."""
        self._gauges[name] = value
        
        metric = SimpleMetric(
            name=name,
            value=value,
            timestamp=datetime.utcnow().isoformat(),
            layer=layer,
            component=component
        )
        
        self._metrics_history[name].append(metric)
        # Keep only last 20 gauge values
        if len(self._metrics_history[name]) > 20:
            self._metrics_history[name].pop(0)
    
    def record_layer_processing(
        self,
        layer: MedallionLayer,
        record_count: int,
        success: bool = True,
        component: Optional[str] = None
    ):
        """Record processing metrics for a medallion layer."""
        if success:
            self.increment_counter(
                f"{layer.value}_records_processed", 
                record_count, 
                layer=layer.value, 
                component=component
            )
            self.set_gauge(f"{layer.value}_status", 1, layer=layer.value, component=component)
        else:
            self.increment_counter(
                f"{layer.value}_errors", 
                1, 
                layer=layer.value, 
                component=component
            )
            self.set_gauge(f"{layer.value}_status", -1, layer=layer.value, component=component)
    
    def get_counter(self, name: str) -> float:
        """Get current counter value."""
        return self._counters.get(name, 0)
    
    def get_gauge(self, name: str) -> float:
        """Get current gauge value."""
        return self._gauges.get(name, 0)
    
    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get summary of pipeline metrics."""
        summary = {
            'timestamp': datetime.utcnow().isoformat(),
            'layers': {}
        }
        
        for layer in MedallionLayer:
            layer_name = layer.value
            summary['layers'][layer_name] = {
                'records_processed': self._counters.get(f"{layer_name}_records_processed", 0),
                'errors': self._counters.get(f"{layer_name}_errors", 0),
                'status': self._gauges.get(f"{layer_name}_status", 0)
            }
        
        return summary
    
    def export_prometheus_format(self) -> str:
        """Export metrics in Prometheus format for optional scraping."""
        lines = []
        
        # Export counters
        for name, value in self._counters.items():
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {value}")
        
        # Export gauges
        for name, value in self._gauges.items():
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")
        
        return '\n'.join(lines)
    
    def clear_metrics(self):
        """Clear all metrics (useful for testing)."""
        self._counters.clear()
        self._gauges.clear()
        self._metrics_history.clear()
        self._initialize_layer_metrics()


# Global metrics collector instance
metrics_collector = SimpleMetricsCollector()


def get_metrics_collector() -> SimpleMetricsCollector:
    """Get the global metrics collector instance."""
    return metrics_collector
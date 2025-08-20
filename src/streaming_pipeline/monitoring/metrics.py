"""
Metrics collection and monitoring for the streaming pipeline.

Provides comprehensive metrics collection including:
- Pipeline performance metrics
- Data quality metrics
- System resource metrics
- Business metrics
"""

import json
import time
import psutil
import threading
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum

from .logger import PipelineLogger, MedallionLayer


class MetricType(Enum):
    """Types of metrics collected."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class Metric:
    """Represents a single metric measurement."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: str
    labels: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MetricSummary:
    """Summary statistics for a metric over time."""
    name: str
    count: int
    min_value: float
    max_value: float
    avg_value: float
    sum_value: float
    latest_value: float
    latest_timestamp: str


class MetricsCollector:
    """Collects and manages pipeline metrics."""
    
    def __init__(self, logger: Optional[PipelineLogger] = None, max_history_size: int = 1000):
        self.logger = logger or PipelineLogger(__name__)
        self.max_history_size = max_history_size
        
        # Metric storage
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history_size))
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = defaultdict(float)
        
        # System monitoring
        self._system_monitoring_active = False
        self._system_monitoring_thread: Optional[threading.Thread] = None
        
        # Performance tracking
        self._active_timers: Dict[str, float] = {}
        
        # Initialize medallion layer metrics
        self._initialize_medallion_metrics()
    
    def _initialize_medallion_metrics(self):
        """Initialize metrics for medallion architecture layers."""
        for layer in MedallionLayer:
            # Initialize counters for each layer
            self.increment_counter(f"{layer.value}_records_processed", 0)
            self.increment_counter(f"{layer.value}_errors", 0)
            self.set_gauge(f"{layer.value}_processing_latency_ms", 0)
            self.set_gauge(f"{layer.value}_throughput_records_per_sec", 0)
    
    def increment_counter(self, name: str, value: float = 1, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        self._counters[name] += value
        
        metric = Metric(
            name=name,
            value=self._counters[name],
            metric_type=MetricType.COUNTER,
            timestamp=datetime.utcnow().isoformat(),
            labels=labels or {},
            metadata={'increment': value}
        )
        
        self._metrics[name].append(metric)
        
        self.logger.info(
            f"Counter incremented: {name} = {self._counters[name]}",
            component="metrics_collector",
            operation="increment_counter",
            metadata={
                'metric_name': name,
                'current_value': self._counters[name],
                'increment': value,
                'labels': labels
            }
        )
    
    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric value."""
        self._gauges[name] = value
        
        metric = Metric(
            name=name,
            value=value,
            metric_type=MetricType.GAUGE,
            timestamp=datetime.utcnow().isoformat(),
            labels=labels or {}
        )
        
        self._metrics[name].append(metric)
    
    def record_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a histogram metric value."""
        metric = Metric(
            name=name,
            value=value,
            metric_type=MetricType.HISTOGRAM,
            timestamp=datetime.utcnow().isoformat(),
            labels=labels or {}
        )
        
        self._metrics[name].append(metric)
    
    def start_timer(self, name: str) -> str:
        """Start a timer for measuring duration."""
        timer_id = f"{name}_{int(time.time() * 1000)}"
        self._active_timers[timer_id] = time.time()
        return timer_id
    
    def end_timer(self, timer_id: str, labels: Optional[Dict[str, str]] = None) -> float:
        """End a timer and record the duration."""
        if timer_id not in self._active_timers:
            raise ValueError(f"Timer {timer_id} not found")
        
        start_time = self._active_timers.pop(timer_id)
        duration = time.time() - start_time
        duration_ms = duration * 1000
        
        # Extract metric name from timer_id
        metric_name = timer_id.rsplit('_', 1)[0]
        
        metric = Metric(
            name=f"{metric_name}_duration_ms",
            value=duration_ms,
            metric_type=MetricType.TIMER,
            timestamp=datetime.utcnow().isoformat(),
            labels=labels or {},
            metadata={'duration_seconds': duration}
        )
        
        self._metrics[f"{metric_name}_duration_ms"].append(metric)
        
        return duration_ms
    
    def record_medallion_processing(
        self,
        layer: MedallionLayer,
        record_count: int,
        processing_time_ms: float,
        error_count: int = 0,
        quality_score: Optional[float] = None
    ):
        """Record processing metrics for a medallion layer."""
        # Record throughput
        throughput = record_count / (processing_time_ms / 1000) if processing_time_ms > 0 else 0
        self.set_gauge(f"{layer.value}_throughput_records_per_sec", throughput)
        
        # Record processing metrics
        self.increment_counter(f"{layer.value}_records_processed", record_count)
        self.set_gauge(f"{layer.value}_processing_latency_ms", processing_time_ms)
        
        if error_count > 0:
            self.increment_counter(f"{layer.value}_errors", error_count)
        
        if quality_score is not None:
            self.set_gauge(f"{layer.value}_quality_score", quality_score)
        
        # Log processing metrics
        self.logger.info(
            f"Recorded {layer.value} layer processing metrics",
            layer=layer,
            component="metrics_collector",
            operation="record_processing",
            metadata={
                'record_count': record_count,
                'processing_time_ms': processing_time_ms,
                'throughput': throughput,
                'error_count': error_count,
                'quality_score': quality_score
            }
        )
    
    def record_data_quality_metrics(
        self,
        layer: MedallionLayer,
        total_records: int,
        valid_records: int,
        invalid_records: int,
        quality_checks: Dict[str, bool]
    ):
        """Record data quality metrics for a layer."""
        quality_score = valid_records / total_records if total_records > 0 else 0
        
        self.set_gauge(f"{layer.value}_quality_score", quality_score)
        self.increment_counter(f"{layer.value}_quality_valid_records", valid_records)
        self.increment_counter(f"{layer.value}_quality_invalid_records", invalid_records)
        
        # Record individual quality check results
        for check_name, passed in quality_checks.items():
            metric_name = f"{layer.value}_quality_check_{check_name}"
            self.set_gauge(metric_name, 1.0 if passed else 0.0)
        
        self.logger.info(
            f"Recorded data quality metrics for {layer.value} layer",
            layer=layer,
            component="metrics_collector",
            operation="record_quality",
            metadata={
                'total_records': total_records,
                'valid_records': valid_records,
                'invalid_records': invalid_records,
                'quality_score': quality_score,
                'quality_checks': quality_checks
            }
        )
    
    def get_metric_summary(self, name: str) -> Optional[MetricSummary]:
        """Get summary statistics for a metric."""
        if name not in self._metrics or not self._metrics[name]:
            return None
        
        metrics = list(self._metrics[name])
        values = [m.value for m in metrics]
        
        return MetricSummary(
            name=name,
            count=len(values),
            min_value=min(values),
            max_value=max(values),
            avg_value=sum(values) / len(values),
            sum_value=sum(values),
            latest_value=values[-1],
            latest_timestamp=metrics[-1].timestamp
        )
    
    def get_medallion_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary for medallion architecture."""
        summary = {
            'timestamp': datetime.utcnow().isoformat(),
            'layers': {}
        }
        
        for layer in MedallionLayer:
            layer_metrics = {}
            
            # Get processing metrics
            throughput_summary = self.get_metric_summary(f"{layer.value}_throughput_records_per_sec")
            latency_summary = self.get_metric_summary(f"{layer.value}_processing_latency_ms")
            quality_summary = self.get_metric_summary(f"{layer.value}_quality_score")
            
            layer_metrics['throughput'] = throughput_summary
            layer_metrics['latency'] = latency_summary
            layer_metrics['quality'] = quality_summary
            
            # Get counter values
            layer_metrics['total_records'] = self._counters.get(f"{layer.value}_records_processed", 0)
            layer_metrics['total_errors'] = self._counters.get(f"{layer.value}_errors", 0)
            
            summary['layers'][layer.value] = layer_metrics
        
        return summary
    
    def start_system_monitoring(self, interval_seconds: int = 30):
        """Start continuous system resource monitoring."""
        if self._system_monitoring_active:
            return
        
        self._system_monitoring_active = True
        self._system_monitoring_thread = threading.Thread(
            target=self._system_monitoring_loop,
            args=(interval_seconds,),
            daemon=True
        )
        self._system_monitoring_thread.start()
        
        self.logger.info(
            "Started system monitoring",
            component="metrics_collector",
            operation="start_monitoring",
            metadata={'interval_seconds': interval_seconds}
        )
    
    def stop_system_monitoring(self):
        """Stop system resource monitoring."""
        self._system_monitoring_active = False
        if self._system_monitoring_thread:
            self._system_monitoring_thread.join(timeout=5)
        
        self.logger.info(
            "Stopped system monitoring",
            component="metrics_collector",
            operation="stop_monitoring"
        )
    
    def _system_monitoring_loop(self, interval_seconds: int):
        """System monitoring loop (runs in separate thread)."""
        while self._system_monitoring_active:
            try:
                # CPU metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                self.set_gauge("system_cpu_percent", cpu_percent)
                
                # Memory metrics
                memory = psutil.virtual_memory()
                self.set_gauge("system_memory_percent", memory.percent)
                self.set_gauge("system_memory_available_gb", memory.available / (1024**3))
                
                # Disk metrics
                disk = psutil.disk_usage('/')
                self.set_gauge("system_disk_percent", disk.percent)
                self.set_gauge("system_disk_free_gb", disk.free / (1024**3))
                
                # Network metrics (if available)
                try:
                    network = psutil.net_io_counters()
                    self.increment_counter("system_network_bytes_sent", network.bytes_sent)
                    self.increment_counter("system_network_bytes_recv", network.bytes_recv)
                except:
                    pass  # Network metrics not available on all systems
                
                time.sleep(interval_seconds)
                
            except Exception as e:
                self.logger.error(
                    f"Error in system monitoring: {str(e)}",
                    component="metrics_collector",
                    operation="system_monitoring",
                    error_details={'error': str(e)}
                )
                time.sleep(interval_seconds)
    
    def export_metrics(self, format_type: str = "prometheus") -> str:
        """Export metrics in specified format."""
        if format_type == "prometheus":
            return self._export_prometheus_format()
        elif format_type == "json":
            return self._export_json_format()
        else:
            raise ValueError(f"Unsupported format: {format_type}")
    
    def _export_prometheus_format(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []
        
        for metric_name, metric_deque in self._metrics.items():
            if not metric_deque:
                continue
            
            latest_metric = metric_deque[-1]
            
            # Add help and type comments
            lines.append(f"# HELP {metric_name} Pipeline metric")
            lines.append(f"# TYPE {metric_name} {latest_metric.metric_type.value}")
            
            # Add metric value with labels
            labels_str = ""
            if latest_metric.labels:
                label_pairs = [f'{k}="{v}"' for k, v in latest_metric.labels.items()]
                labels_str = "{" + ",".join(label_pairs) + "}"
            
            lines.append(f"{metric_name}{labels_str} {latest_metric.value}")
        
        return "\n".join(lines)
    
    def _export_json_format(self) -> str:
        """Export metrics in JSON format."""
        export_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'metrics': {}
        }
        
        for metric_name, metric_deque in self._metrics.items():
            if not metric_deque:
                continue
            
            latest_metric = metric_deque[-1]
            export_data['metrics'][metric_name] = {
                'value': latest_metric.value,
                'type': latest_metric.metric_type.value,
                'timestamp': latest_metric.timestamp,
                'labels': latest_metric.labels,
                'metadata': latest_metric.metadata
            }
        
        return json.dumps(export_data, indent=2)
    
    def clear_old_metrics(self, older_than_hours: int = 24):
        """Clear old metrics to manage memory usage."""
        cutoff_time = datetime.utcnow() - timedelta(hours=older_than_hours)
        cleared_count = 0
        
        for metric_name, metric_deque in self._metrics.items():
            initial_size = len(metric_deque)
            
            # Filter out old metrics
            filtered_metrics = deque(
                [m for m in metric_deque 
                 if datetime.fromisoformat(m.timestamp.replace('Z', '+00:00')) > cutoff_time],
                maxlen=self.max_history_size
            )
            
            self._metrics[metric_name] = filtered_metrics
            cleared_count += initial_size - len(filtered_metrics)
        
        self.logger.info(
            f"Cleared {cleared_count} old metrics",
            component="metrics_collector",
            operation="cleanup",
            metadata={
                'cleared_count': cleared_count,
                'cutoff_hours': older_than_hours
            }
        )
"""
Base data producer classes and metrics for streaming pipeline.
Provides common functionality for different types of data producers.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Any, Optional


logger = logging.getLogger(__name__)


@dataclass
class ProducerMetrics:
    """
    Metrics tracking for data producers.
    
    Tracks messages sent, failed, bytes sent, and other operational metrics.
    """
    messages_sent: int = 0
    messages_failed: int = 0
    bytes_sent: int = 0
    bytes_failed: int = 0
    api_requests: int = 0
    api_errors: int = 0
    last_sent_timestamp: Optional[datetime] = None
    last_error_timestamp: Optional[datetime] = None
    last_error_message: Optional[str] = None
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def record_success(self, bytes_count: int = 0) -> None:
        """Record a successful message send."""
        self.messages_sent += 1
        self.bytes_sent += bytes_count
        self.last_sent_timestamp = datetime.now(timezone.utc)
    
    def record_failure(self, error_message: str, bytes_count: int = 0) -> None:
        """Record a failed message send."""
        self.messages_failed += 1
        self.bytes_failed += bytes_count
        self.last_error_timestamp = datetime.now(timezone.utc)
        self.last_error_message = error_message
    
    def get_success_rate(self) -> float:
        """Calculate success rate as a percentage."""
        total_messages = self.messages_sent + self.messages_failed
        if total_messages == 0:
            return 0.0
        return (self.messages_sent / total_messages) * 100.0
    
    def get_throughput_mps(self) -> float:
        """Get messages per second throughput."""
        elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        if elapsed == 0:
            return 0.0
        return self.messages_sent / elapsed
    
    def get_throughput_bps(self) -> float:
        """Get bytes per second throughput."""
        elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        if elapsed == 0:
            return 0.0
        return self.bytes_sent / elapsed
    
    def get_throughput_per_second(self) -> float:
        """Alias for get_throughput_mps for backward compatibility."""
        return self.get_throughput_mps()
    
    def get_runtime_seconds(self) -> float:
        """Get runtime in seconds since start."""
        return (datetime.now(timezone.utc) - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary format."""
        return {
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "bytes_sent": self.bytes_sent,
            "bytes_failed": self.bytes_failed,
            "success_rate_percent": self.get_success_rate(),
            "throughput_messages_per_second": self.get_throughput_mps(),
            "throughput_bytes_per_second": self.get_throughput_bps(),
            "last_sent_timestamp": self.last_sent_timestamp.isoformat() if self.last_sent_timestamp else None,
            "last_error_timestamp": self.last_error_timestamp.isoformat() if self.last_error_timestamp else None,
            "last_error_message": self.last_error_message,
            "uptime_seconds": (datetime.now(timezone.utc) - self.start_time).total_seconds()
        }
    
    def reset(self) -> None:
        """Reset all metrics to zero."""
        self.messages_sent = 0
        self.messages_failed = 0
        self.bytes_sent = 0
        self.bytes_failed = 0
        self.api_requests = 0
        self.api_errors = 0
        self.last_sent_timestamp = None
        self.last_error_timestamp = None
        self.last_error_message = None
        self.start_time = datetime.now(timezone.utc)


class BaseDataProducer:
    """
    Base class for data producers in the streaming pipeline.
    
    Provides common functionality and interface for different types of producers.
    """
    
    def __init__(self, name: str = "BaseProducer"):
        """
        Initialize base producer.
        
        Args:
            name: Name of the producer for logging
        """
        self.name = name
        self.metrics = ProducerMetrics()
        self._is_running = False
        
        logger.info(f"Initialized {self.name}")
    
    def start(self) -> None:
        """Start the producer."""
        self._is_running = True
        logger.info(f"{self.name} started")
    
    def stop(self) -> None:
        """Stop the producer."""
        self._is_running = False
        logger.info(f"{self.name} stopped")
    
    def is_running(self) -> bool:
        """Check if producer is running."""
        return self._is_running
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics."""
        return {
            "producer_name": self.name,
            "is_running": self._is_running,
            **self.metrics.to_dict()
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        return {
            "status": "healthy" if self._is_running else "stopped",
            "producer_name": self.name,
            "metrics": self.get_metrics()
        }
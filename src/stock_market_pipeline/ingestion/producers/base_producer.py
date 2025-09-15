"""
Base producer implementation for Kafka publishing.
Provides common functionality and metrics tracking.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime

from stock_market_pipeline.core.interfaces import DataProducer
from stock_market_pipeline.core.exceptions import KafkaProducerError
from stock_market_pipeline.utils import PipelineLogger


class BaseKafkaProducer(DataProducer):
    """Base implementation for Kafka producers."""
    
    def __init__(self, config: Any, logger: PipelineLogger):
        self.config = config
        self.logger = logger
        self._is_healthy = True
        self._metrics = {
            'messages_produced': 0,
            'messages_failed': 0,
            'last_produce_time': None,
            'producer_errors': 0
        }
    
    def is_healthy(self) -> bool:
        """Check if producer is healthy."""
        return self._is_healthy and self._metrics['messages_failed'] < 100
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics."""
        return self._metrics.copy()
    
    def _update_metrics(self, success: bool) -> None:
        """Update internal metrics."""
        if success:
            self._metrics['messages_produced'] += 1
        else:
            self._metrics['messages_failed'] += 1
            self._metrics['producer_errors'] += 1
        self._metrics['last_produce_time'] = datetime.utcnow().isoformat()
    
    @abstractmethod
    def produce_stock_quote(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Produce stock quote to Kafka."""
        pass
    
    @abstractmethod
    def produce_intraday_data(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Produce intraday data to Kafka."""
        pass

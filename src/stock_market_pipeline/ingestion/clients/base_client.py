"""
Base client implementation for data sources.
Provides common functionality and metrics tracking.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime

from stock_market_pipeline.core.interfaces import DataClient
from stock_market_pipeline.core.exceptions import IngestionError
from stock_market_pipeline.utils import PipelineLogger


class BaseDataClient(DataClient):
    """Base implementation for all data clients."""
    
    def __init__(self, config: Any, logger: PipelineLogger):
        self.config = config
        self.logger = logger
        self._is_healthy = True
        self._metrics = {
            'requests_total': 0,
            'requests_success': 0,
            'requests_failed': 0,
            'last_request_time': None,
            'rate_limit_hits': 0
        }
    
    def is_healthy(self) -> bool:
        """Check if client is healthy."""
        return self._is_healthy and self._metrics['requests_failed'] < 10
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        return self._metrics.copy()
    
    def _update_metrics(self, success: bool) -> None:
        """Update internal metrics."""
        self._metrics['requests_total'] += 1
        if success:
            self._metrics['requests_success'] += 1
        else:
            self._metrics['requests_failed'] += 1
        self._metrics['last_request_time'] = datetime.utcnow().isoformat()
    
    @abstractmethod
    def get_real_time_quote(self, symbol: str) -> Dict[str, Any]:
        """Get real-time stock quote."""
        pass
    
    @abstractmethod
    def get_intraday_data(self, symbol: str, interval: str = "5min") -> Dict[str, Any]:
        """Get intraday data."""
        pass

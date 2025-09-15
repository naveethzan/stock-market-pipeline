"""
Real Alpha Vantage API client implementation.
Clean, focused, follows SOLID principles.
"""

import requests
import time
from typing import Dict, Any, List
from datetime import datetime, timezone

from stock_market_pipeline.ingestion.clients.base_client import BaseDataClient
from stock_market_pipeline.core.exceptions import AlphaVantageAPIError, IngestionError
from stock_market_pipeline.utils import PipelineLogger


class AlphaVantageClient(BaseDataClient):
    """
    Real Alpha Vantage API client for live market data.
    
    Provides access to Alpha Vantage's financial data APIs including
    real-time quotes and intraday time series data with built-in rate
    limiting, retry logic, and comprehensive error handling.
    """
    
    def __init__(self, config: Any):
        super().__init__(config, PipelineLogger(__name__))
        self.api_key = config.api_key
        self.base_url = config.base_url
        self.rate_limit = config.rate_limit
        self.timeout = config.timeout
        self.retry_attempts = config.retry_attempts
        self.backoff_factor = config.backoff_factor
        self._last_request_time = 0
    
    def fetch_data(self, symbol: str) -> Dict[str, Any]:
        """Fetch data for a given symbol (implements DataClient interface)."""
        return self.get_real_time_quote(symbol)
    
    def get_real_time_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Get real-time stock quote using GLOBAL_QUOTE API.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL', 'GOOGL')
            
        Returns:
            Dictionary containing real-time quote data with OHLCV information
            
        Raises:
            AlphaVantageAPIError: If API request fails or returns error
        """
        self._enforce_rate_limit()
        
        try:
            response = self._make_request('GLOBAL_QUOTE', symbol)
            self._update_metrics(True)
            return response
        except Exception as e:
            self._update_metrics(False)
            raise AlphaVantageAPIError(
                f"Failed to fetch quote for {symbol}: {str(e)}",
                component="alpha_vantage_client",
                context={"symbol": symbol, "function": "GLOBAL_QUOTE"}
            )
    
    def get_intraday_data(self, symbol: str, interval: str = "5min") -> Dict[str, Any]:
        """
        Get intraday data using TIME_SERIES_INTRADAY API.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL', 'GOOGL')
            interval: Time interval for data points (1min, 5min, 15min, 30min, 60min)
            
        Returns:
            Dictionary containing time series data with OHLCV for each interval
            
        Raises:
            AlphaVantageAPIError: If API request fails or returns error
        """
        self._enforce_rate_limit()
        
        try:
            response = self._make_request('TIME_SERIES_INTRADAY', symbol, interval=interval)
            self._update_metrics(True)
            return response
        except Exception as e:
            self._update_metrics(False)
            raise AlphaVantageAPIError(
                f"Failed to fetch intraday data for {symbol}: {str(e)}",
                component="alpha_vantage_client",
                context={"symbol": symbol, "function": "TIME_SERIES_INTRADAY", "interval": interval}
            )
    
    def _make_request(self, function: str, symbol: str, **params) -> Dict[str, Any]:
        """Make API request with retry logic."""
        url = f"{self.base_url}"
        params.update({
            'function': function,
            'symbol': symbol,
            'apikey': self.api_key
        })
        
        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                
                # Check for API errors
                if 'Error Message' in data:
                    raise AlphaVantageAPIError(
                        data['Error Message'],
                        status_code=response.status_code,
                        response_data=data,
                        component="alpha_vantage_client"
                    )
                if 'Note' in data:
                    raise AlphaVantageAPIError(
                        data['Note'],
                        status_code=response.status_code,
                        response_data=data,
                        component="alpha_vantage_client"
                    )
                
                return data
            except requests.RequestException as e:
                if attempt == self.retry_attempts - 1:
                    raise AlphaVantageAPIError(
                        f"API request failed after {self.retry_attempts} attempts: {str(e)}",
                        status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                        component="alpha_vantage_client"
                    )
                time.sleep(self.backoff_factor ** attempt)
    
    def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting."""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        min_interval = 60.0 / self.rate_limit
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
            self._metrics['rate_limit_hits'] += 1
        
        self._last_request_time = time.time()

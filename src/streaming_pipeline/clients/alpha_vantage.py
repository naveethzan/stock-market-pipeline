"""
Alpha Vantage API client for real-time stock market data ingestion.
Implements authentication, rate limiting, and error handling.
"""
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional, Any
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config.settings import AlphaVantageConfig


logger = logging.getLogger(__name__)


class AlphaVantageAPIError(Exception):
    """Custom exception for Alpha Vantage API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[Dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class AlphaVantageClient:
    """
    Alpha Vantage API client with authentication, rate limiting, and error handling.
    
    Supports real-time quotes and intraday data retrieval with comprehensive
    logging and monitoring capabilities.
    """
    
    def __init__(self, config: AlphaVantageConfig):
        """
        Initialize Alpha Vantage client.
        
        Args:
            config: Alpha Vantage configuration object
        """
        self.config = config
        self.session = self._create_session()
        self.last_request_time = 0.0
        self.request_count = 0
        self.quota_reset_time = time.time() + 60  # Reset quota every minute
        
        logger.info(
            "Alpha Vantage client initialized",
            extra={
                "base_url": config.base_url,
                "rate_limit": config.rate_limit_per_minute,
                "timeout": config.timeout_seconds
            }
        )
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy and timeouts."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.retry_attempts,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default timeout
        session.timeout = self.config.timeout_seconds
        
        # Set user agent
        session.headers.update({
            'User-Agent': 'StreamingPipeline/1.0 (Python/requests)'
        })
        
        return session
    
    def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting to respect API limits."""
        current_time = time.time()
        
        # Reset quota if minute has passed
        if current_time >= self.quota_reset_time:
            self.request_count = 0
            self.quota_reset_time = current_time + 60
            logger.debug("Rate limit quota reset")
        
        # Check if we've exceeded rate limit
        if self.request_count >= self.config.rate_limit_per_minute:
            sleep_time = self.quota_reset_time - current_time
            if sleep_time > 0:
                logger.warning(
                    f"Rate limit exceeded, sleeping for {sleep_time:.2f} seconds",
                    extra={
                        "request_count": self.request_count,
                        "rate_limit": self.config.rate_limit_per_minute,
                        "sleep_time": sleep_time
                    }
                )
                time.sleep(sleep_time)
                # Reset after sleep
                self.request_count = 0
                self.quota_reset_time = time.time() + 60
        
        # Ensure minimum time between requests (additional safety)
        time_since_last = current_time - self.last_request_time
        min_interval = 60.0 / self.config.rate_limit_per_minute
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            logger.debug(f"Enforcing minimum interval, sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def _make_request(self, params: Dict[str, str]) -> Dict[str, Any]:
        """
        Make authenticated request to Alpha Vantage API.
        
        Args:
            params: Query parameters for the API request
            
        Returns:
            API response data
            
        Raises:
            AlphaVantageAPIError: If API request fails
        """
        # Add API key to parameters
        params['apikey'] = self.config.api_key
        
        # Enforce rate limiting
        self._enforce_rate_limit()
        
        # Build URL
        url = f"{self.config.base_url}?{urlencode(params)}"
        
        # Log request
        request_id = f"req_{int(time.time() * 1000)}"
        logger.info(
            "Making Alpha Vantage API request",
            extra={
                "request_id": request_id,
                "function": params.get('function'),
                "symbol": params.get('symbol'),
                "url": url.replace(self.config.api_key, "***REDACTED***")
            }
        )
        
        start_time = time.time()
        
        try:
            response = self.session.get(url, timeout=self.config.timeout_seconds)
            response_time = time.time() - start_time
            
            # Log response
            logger.info(
                "Alpha Vantage API response received",
                extra={
                    "request_id": request_id,
                    "status_code": response.status_code,
                    "response_time_ms": round(response_time * 1000, 2),
                    "content_length": len(response.content)
                }
            )
            
            # Check for HTTP errors
            if response.status_code != 200:
                error_msg = f"HTTP {response.status_code}: {response.reason}"
                logger.error(
                    "Alpha Vantage API HTTP error",
                    extra={
                        "request_id": request_id,
                        "status_code": response.status_code,
                        "error": error_msg
                    }
                )
                raise AlphaVantageAPIError(error_msg, response.status_code)
            
            # Parse JSON response
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                error_msg = f"Invalid JSON response: {str(e)}"
                logger.error(
                    "Alpha Vantage API JSON parsing error",
                    extra={
                        "request_id": request_id,
                        "error": error_msg,
                        "response_text": response.text[:500]
                    }
                )
                raise AlphaVantageAPIError(error_msg, response.status_code, {"raw_response": response.text})
            
            # Check for API errors in response
            if "Error Message" in data:
                error_msg = data["Error Message"]
                logger.error(
                    "Alpha Vantage API error in response",
                    extra={
                        "request_id": request_id,
                        "api_error": error_msg
                    }
                )
                raise AlphaVantageAPIError(f"API Error: {error_msg}", response.status_code, data)
            
            # Check for rate limit message
            if "Note" in data and "rate limit" in data["Note"].lower():
                error_msg = data["Note"]
                logger.warning(
                    "Alpha Vantage rate limit warning",
                    extra={
                        "request_id": request_id,
                        "note": error_msg
                    }
                )
                raise AlphaVantageAPIError(f"Rate Limit: {error_msg}", response.status_code, data)
            
            # Check for Information message (commonly used for rate limit messages)
            if "Information" in data and ("rate limit" in data["Information"].lower() or "premium" in data["Information"].lower()):
                error_msg = data["Information"]
                logger.warning(
                    "Alpha Vantage rate limit or subscription message",
                    extra={
                        "request_id": request_id,
                        "information": error_msg
                    }
                )
                raise AlphaVantageAPIError(f"API Limit: {error_msg}", response.status_code, data)
            
            # Log successful response
            logger.debug(
                "Alpha Vantage API request successful",
                extra={
                    "request_id": request_id,
                    "data_keys": list(data.keys()) if isinstance(data, dict) else "non-dict-response"
                }
            )
            
            return data
            
        except requests.exceptions.Timeout:
            error_msg = f"Request timeout after {self.config.timeout_seconds} seconds"
            logger.error(
                "Alpha Vantage API timeout",
                extra={
                    "request_id": request_id,
                    "timeout_seconds": self.config.timeout_seconds
                }
            )
            raise AlphaVantageAPIError(error_msg)
            
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(
                "Alpha Vantage API connection error",
                extra={
                    "request_id": request_id,
                    "error": error_msg
                }
            )
            raise AlphaVantageAPIError(error_msg)
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error: {str(e)}"
            logger.error(
                "Alpha Vantage API request error",
                extra={
                    "request_id": request_id,
                    "error": error_msg
                }
            )
            raise AlphaVantageAPIError(error_msg)
    
    def get_real_time_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Get real-time quote for a stock symbol.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            
        Returns:
            Real-time quote data
            
        Raises:
            AlphaVantageAPIError: If API request fails
        """
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol.upper()
        }
        
        logger.info(
            "Requesting real-time quote",
            extra={
                "symbol": symbol,
                "function": "GLOBAL_QUOTE"
            }
        )
        
        try:
            data = self._make_request(params)
            
            # Extract quote data
            if "Global Quote" not in data:
                error_msg = f"Unexpected response format for symbol {symbol}"
                logger.error(
                    "Invalid quote response format",
                    extra={
                        "symbol": symbol,
                        "response_keys": list(data.keys()) if isinstance(data, dict) else "non-dict",
                        "full_response": json.dumps(data, indent=2) if isinstance(data, dict) and len(str(data)) < 2000 else "response_too_large",
                        "response_preview": str(data)[:500] + "..." if len(str(data)) > 500 else str(data)
                    }
                )
                raise AlphaVantageAPIError(error_msg, response_data=data)
            
            quote_data = data["Global Quote"]
            
            # Add metadata
            quote_data["_metadata"] = {
                "symbol": symbol,
                "request_timestamp": datetime.now(timezone.utc).isoformat(),
                "data_source": "alpha_vantage",
                "function": "GLOBAL_QUOTE"
            }
            
            logger.info(
                "Real-time quote retrieved successfully",
                extra={
                    "symbol": symbol,
                    "price": quote_data.get("05. price"),
                    "change": quote_data.get("09. change"),
                    "change_percent": quote_data.get("10. change percent")
                }
            )
            
            return quote_data
            
        except AlphaVantageAPIError:
            raise
        except Exception as e:
            error_msg = f"Unexpected error getting quote for {symbol}: {str(e)}"
            logger.error(
                "Unexpected error in get_real_time_quote",
                extra={
                    "symbol": symbol,
                    "error": error_msg
                },
                exc_info=True
            )
            raise AlphaVantageAPIError(error_msg)
    
    def get_intraday_data(self, symbol: str, interval: str = "1min", outputsize: str = "compact") -> Dict[str, Any]:
        """
        Get intraday data for a stock symbol.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            interval: Time interval ('1min', '5min', '15min', '30min', '60min')
            outputsize: 'compact' (latest 100 data points) or 'full' (full-length data)
            
        Returns:
            Intraday time series data
            
        Raises:
            AlphaVantageAPIError: If API request fails
        """
        # Validate interval
        valid_intervals = ['1min', '5min', '15min', '30min', '60min']
        if interval not in valid_intervals:
            raise ValueError(f"Invalid interval '{interval}'. Must be one of: {valid_intervals}")
        
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol.upper(),
            'interval': interval,
            'outputsize': outputsize
        }
        
        logger.info(
            "Requesting intraday data",
            extra={
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "function": "TIME_SERIES_INTRADAY"
            }
        )
        
        try:
            data = self._make_request(params)
            
            # Extract time series data
            time_series_key = f"Time Series ({interval})"
            if time_series_key not in data:
                error_msg = f"Unexpected response format for symbol {symbol}, interval {interval}"
                logger.error(
                    "Invalid intraday response format",
                    extra={
                        "symbol": symbol,
                        "interval": interval,
                        "expected_key": time_series_key,
                        "response_keys": list(data.keys()) if isinstance(data, dict) else "non-dict"
                    }
                )
                raise AlphaVantageAPIError(error_msg, response_data=data)
            
            # Add metadata
            result = {
                "Meta Data": data.get("Meta Data", {}),
                "Time Series": data[time_series_key],
                "_metadata": {
                    "symbol": symbol,
                    "interval": interval,
                    "outputsize": outputsize,
                    "request_timestamp": datetime.now(timezone.utc).isoformat(),
                    "data_source": "alpha_vantage",
                    "function": "TIME_SERIES_INTRADAY",
                    "data_points": len(data[time_series_key])
                }
            }
            
            logger.info(
                "Intraday data retrieved successfully",
                extra={
                    "symbol": symbol,
                    "interval": interval,
                    "data_points": len(data[time_series_key]),
                    "latest_timestamp": max(data[time_series_key].keys()) if data[time_series_key] else None
                }
            )
            
            return result
            
        except AlphaVantageAPIError:
            raise
        except Exception as e:
            error_msg = f"Unexpected error getting intraday data for {symbol}: {str(e)}"
            logger.error(
                "Unexpected error in get_intraday_data",
                extra={
                    "symbol": symbol,
                    "interval": interval,
                    "error": error_msg
                },
                exc_info=True
            )
            raise AlphaVantageAPIError(error_msg)
    
    def get_client_status(self) -> Dict[str, Any]:
        """
        Get current client status and metrics.
        
        Returns:
            Client status information
        """
        current_time = time.time()
        return {
            "config": {
                "base_url": self.config.base_url,
                "rate_limit_per_minute": self.config.rate_limit_per_minute,
                "timeout_seconds": self.config.timeout_seconds,
                "retry_attempts": self.config.retry_attempts
            },
            "rate_limiting": {
                "current_request_count": self.request_count,
                "quota_reset_time": self.quota_reset_time,
                "seconds_until_reset": max(0, self.quota_reset_time - current_time),
                "requests_remaining": max(0, self.config.rate_limit_per_minute - self.request_count)
            },
            "session": {
                "last_request_time": self.last_request_time,
                "time_since_last_request": current_time - self.last_request_time if self.last_request_time > 0 else None
            }
        }
    
    def close(self) -> None:
        """Close the HTTP session."""
        if self.session:
            self.session.close()
            logger.info("Alpha Vantage client session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
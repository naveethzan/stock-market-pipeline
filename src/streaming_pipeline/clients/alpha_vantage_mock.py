"""
Mock Alpha Vantage API client for testing and development.
Generates realistic stock market data without hitting API rate limits.
"""
import json
import logging
import random
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any
from dataclasses import dataclass


logger = logging.getLogger(__name__)


class AlphaVantageAPIError(Exception):
    """Mock exception for Alpha Vantage API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[Dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


@dataclass
class MockAlphaVantageConfig:
    """Configuration for Mock Alpha Vantage client."""
    api_key: str = "mock_api_key"
    base_url: str = "mock://alpha-vantage-api"
    rate_limit_per_minute: int = 999999  # Unlimited for mock
    timeout_seconds: int = 30
    retry_attempts: int = 3
    retry_backoff_factor: float = 2.0
    mock_mode: bool = True


class MockAlphaVantageClient:
    """
    Mock Alpha Vantage API client that generates realistic stock market data.
    
    Provides the same interface as AlphaVantageClient but returns mock data
    instead of making actual API calls. Useful for testing and development
    when rate limits are a concern.
    """
    
    def __init__(self, config):
        """
        Initialize Mock Alpha Vantage client.
        
        Args:
            config: Alpha Vantage configuration object (used for logging compatibility)
        """
        self.config = config
        self.request_count = 0
        
        # Stock symbol base prices for realistic price generation
        self.base_prices = {
            'AAPL': 175.50,
            'GOOGL': 140.25,
            'MSFT': 378.85,
            'AMZN': 145.75,
            'TSLA': 248.50,
            'META': 325.20,
            'NVDA': 480.30,
            'NFLX': 445.80,
            'AMD': 125.40,
            'INTC': 42.85,
            'ORCL': 115.60,
            'CRM': 285.40,
            'UBER': 62.25,
            'LYFT': 15.80,
            'SNAP': 12.45,
            'TWTR': 35.20,  # For historical reference
            'BABA': 88.50,
            'SHOP': 65.30,
            'SQ': 75.40,
            'PYPL': 62.90,
            'V': 245.75,
            'MA': 385.60,
            'JPM': 155.20,
            'BAC': 32.45,
            'WFC': 42.80,
            'C': 48.70,
            'GS': 385.90,
            'MS': 88.40,
            'XOM': 110.25,
            'CVX': 155.80,
            'WMT': 165.40,
            'PG': 155.30,
            'JNJ': 165.90,
            'KO': 58.75,
            'PEP': 175.60,
            'MCD': 295.80,
            'HD': 335.20,
            'LOW': 235.40
        }
        
        # Track price movements for each symbol to maintain continuity
        self.current_prices = {}
        self.price_trends = {}  # Track if stock is trending up or down
        
        logger.info(
            "Mock Alpha Vantage client initialized",
            extra={
                "base_url": "mock://alpha-vantage-api",
                "supported_symbols": len(self.base_prices),
                "mock_mode": True
            }
        )
    
    def _get_realistic_price_change(self, symbol: str) -> float:
        """
        Generate realistic price change for a stock symbol.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Price change as a percentage (-5% to +5%)
        """
        # Initialize current price if not exists
        if symbol not in self.current_prices:
            base_price = self.base_prices.get(symbol, 100.0)
            # Add some random variation to base price
            variation = random.uniform(-0.05, 0.05)  # ±5% variation
            self.current_prices[symbol] = base_price * (1 + variation)
            self.price_trends[symbol] = random.choice([-1, 0, 1])  # -1: down, 0: sideways, 1: up
        
        # Determine if trend should change (20% chance)
        if random.random() < 0.2:
            self.price_trends[symbol] = random.choice([-1, 0, 1])
        
        # Generate price change based on trend
        trend = self.price_trends.get(symbol, 0)  # Default to sideways if not set
        
        if trend == 1:  # Upward trend
            change = random.uniform(0.001, 0.03)  # 0.1% to 3% positive
        elif trend == -1:  # Downward trend
            change = random.uniform(-0.03, -0.001)  # 0.1% to 3% negative
        else:  # Sideways trend
            change = random.uniform(-0.01, 0.01)  # ±1% variation
        
        return change
    
    def _generate_mock_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Generate mock real-time quote data.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Mock quote data in Alpha Vantage format
        """
        # Get current price or initialize
        if symbol not in self.current_prices:
            self.current_prices[symbol] = self.base_prices.get(symbol, 100.0)
        
        current_price = self.current_prices[symbol]
        
        # Generate price change
        change_percent = self._get_realistic_price_change(symbol)
        price_change = current_price * change_percent
        new_price = current_price + price_change
        
        # Update current price
        self.current_prices[symbol] = new_price
        
        # Generate other realistic values
        open_price = current_price * random.uniform(0.98, 1.02)  # ±2% from previous close
        high_price = max(open_price, new_price) * random.uniform(1.0, 1.015)  # Up to 1.5% higher
        low_price = min(open_price, new_price) * random.uniform(0.985, 1.0)  # Up to 1.5% lower
        volume = random.randint(1000000, 50000000)  # 1M to 50M shares
        
        # Format change percent
        change_percent_str = f"{change_percent * 100:.4f}%"
        
        return {
            "01. symbol": symbol,
            "02. open": f"{open_price:.4f}",
            "03. high": f"{high_price:.4f}",
            "04. low": f"{low_price:.4f}",
            "05. price": f"{new_price:.4f}",
            "06. volume": str(volume),
            "07. latest trading day": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "08. previous close": f"{current_price:.4f}",
            "09. change": f"{price_change:.4f}",
            "10. change percent": change_percent_str
        }
    
    def _generate_mock_intraday_data(self, symbol: str, interval: str = "1min", outputsize: str = "compact") -> Dict[str, Any]:
        """
        Generate mock intraday data.
        
        Args:
            symbol: Stock symbol
            interval: Time interval
            outputsize: Output size (compact or full)
            
        Returns:
            Mock intraday data in Alpha Vantage format
        """
        # Determine number of data points
        if outputsize == "full":
            data_points = random.randint(800, 1000)  # Simulate full day of data
        else:
            data_points = random.randint(80, 100)  # Compact data
        
        # Parse interval to get minutes
        interval_minutes = {
            "1min": 1,
            "5min": 5,
            "15min": 15,
            "30min": 30,
            "60min": 60
        }.get(interval, 1)
        
        # Generate time series data
        current_time = datetime.now(timezone.utc)
        time_series = {}
        
        # Get base price
        base_price = self.current_prices.get(symbol, self.base_prices.get(symbol, 100.0))
        current_price = base_price
        
        for i in range(data_points):
            # Generate timestamp (going backwards in time)
            timestamp = current_time - timedelta(minutes=i * interval_minutes)
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            
            # Generate price movement
            change = random.uniform(-0.02, 0.02)  # ±2% change per interval
            new_price = current_price * (1 + change)
            
            # Generate OHLCV data
            open_price = current_price
            high_price = max(open_price, new_price) * random.uniform(1.0, 1.01)
            low_price = min(open_price, new_price) * random.uniform(0.99, 1.0)
            close_price = new_price
            volume = random.randint(10000, 1000000)
            
            time_series[timestamp_str] = {
                "1. open": f"{open_price:.4f}",
                "2. high": f"{high_price:.4f}",
                "3. low": f"{low_price:.4f}",
                "4. close": f"{close_price:.4f}",
                "5. volume": str(volume)
            }
            
            current_price = new_price
        
        # Generate metadata
        meta_data = {
            "1. Information": f"Intraday ({interval}) open, high, low, close prices and volume",
            "2. Symbol": symbol,
            "3. Last Refreshed": current_time.strftime("%Y-%m-%d %H:%M:%S"),
            "4. Interval": interval,
            "5. Output Size": outputsize.capitalize(),
            "6. Time Zone": "US/Eastern"
        }
        
        return {
            "Meta Data": meta_data,
            f"Time Series ({interval})": time_series
        }
    
    def get_real_time_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Get mock real-time quote for a stock symbol.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            
        Returns:
            Mock real-time quote data
        """
        self.request_count += 1
        
        # Simulate some processing time
        time.sleep(random.uniform(0.1, 0.3))
        
        logger.info(
            "Generating mock real-time quote",
            extra={
                "symbol": symbol,
                "function": "GLOBAL_QUOTE",
                "mock_mode": True,
                "request_count": self.request_count
            }
        )
        
        # Check if symbol is supported
        if symbol.upper() not in self.base_prices and symbol.upper() not in ['SPY', 'QQQ', 'IWM', 'DIA']:
            # Add new symbol with random base price
            self.base_prices[symbol.upper()] = random.uniform(50, 300)
            logger.info(f"Added new mock symbol: {symbol.upper()} with base price ${self.base_prices[symbol.upper()]:.2f}")
        
        quote_data = self._generate_mock_quote(symbol.upper())
        
        # Wrap in Global Quote structure
        result = {
            "Global Quote": quote_data,
            "_metadata": {
                "symbol": symbol.upper(),
                "request_timestamp": datetime.now(timezone.utc).isoformat(),
                "data_source": "alpha_vantage_mock",
                "function": "GLOBAL_QUOTE",
                "mock_mode": True
            }
        }
        
        logger.info(
            "Mock real-time quote generated successfully",
            extra={
                "symbol": symbol,
                "price": quote_data.get("05. price"),
                "change": quote_data.get("09. change"),
                "change_percent": quote_data.get("10. change percent"),
                "mock_mode": True
            }
        )
        
        return result
    
    def get_intraday_data(self, symbol: str, interval: str = "1min", outputsize: str = "compact") -> Dict[str, Any]:
        """
        Get mock intraday data for a stock symbol.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            interval: Time interval ('1min', '5min', '15min', '30min', '60min')
            outputsize: 'compact' (latest 100 data points) or 'full' (full-length data)
            
        Returns:
            Mock intraday time series data
        """
        self.request_count += 1
        
        # Validate interval
        valid_intervals = ['1min', '5min', '15min', '30min', '60min']
        if interval not in valid_intervals:
            raise ValueError(f"Invalid interval '{interval}'. Must be one of: {valid_intervals}")
        
        # Simulate some processing time
        time.sleep(random.uniform(0.2, 0.5))
        
        logger.info(
            "Generating mock intraday data",
            extra={
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "function": "TIME_SERIES_INTRADAY",
                "mock_mode": True,
                "request_count": self.request_count
            }
        )
        
        # Check if symbol is supported
        if symbol.upper() not in self.base_prices:
            # Add new symbol with random base price
            self.base_prices[symbol.upper()] = random.uniform(50, 300)
            logger.info(f"Added new mock symbol: {symbol.upper()} with base price ${self.base_prices[symbol.upper()]:.2f}")
        
        intraday_data = self._generate_mock_intraday_data(symbol.upper(), interval, outputsize)
        
        # Add metadata
        result = {
            **intraday_data,
            "_metadata": {
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "request_timestamp": datetime.now(timezone.utc).isoformat(),
                "data_source": "alpha_vantage_mock",
                "function": "TIME_SERIES_INTRADAY",
                "data_points": len(intraday_data[f"Time Series ({interval})"]),
                "mock_mode": True
            }
        }
        
        logger.info(
            "Mock intraday data generated successfully",
            extra={
                "symbol": symbol,
                "interval": interval,
                "data_points": len(intraday_data[f"Time Series ({interval})"]),
                "mock_mode": True
            }
        )
        
        return result
    
    def get_client_status(self) -> Dict[str, Any]:
        """
        Get mock client status and metrics.
        
        Returns:
            Mock client status information
        """
        return {
            "config": {
                "base_url": "mock://alpha-vantage-api",
                "rate_limit_per_minute": "unlimited",
                "timeout_seconds": self.config.timeout_seconds,
                "retry_attempts": self.config.retry_attempts,
                "mock_mode": True
            },
            "rate_limiting": {
                "current_request_count": self.request_count,
                "quota_reset_time": "not_applicable",
                "seconds_until_reset": 0,
                "requests_remaining": "unlimited"
            },
            "session": {
                "mock_symbols_supported": len(self.base_prices),
                "total_mock_requests": self.request_count,
                "mock_mode": True
            },
            "mock_data": {
                "supported_symbols": list(self.base_prices.keys()),
                "current_prices": {k: f"${v:.2f}" for k, v in self.current_prices.items()},
                "price_trends": self.price_trends
            }
        }
    
    def close(self) -> None:
        """Close the mock client (no-op for compatibility)."""
        logger.info("Mock Alpha Vantage client closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
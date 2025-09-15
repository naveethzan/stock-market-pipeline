"""
Mock Alpha Vantage API client for testing and development.
Generates realistic stock market data.
"""

import random
import time
from typing import Dict, Any, List
from datetime import datetime, timezone, timedelta

from stock_market_pipeline.ingestion.clients.base_client import BaseDataClient
from stock_market_pipeline.core.exceptions import AlphaVantageAPIError
from stock_market_pipeline.utils import PipelineLogger


class MockAlphaVantageClient(BaseDataClient):
    """
    Mock Alpha Vantage API client for testing and development.
    
    Generates realistic stock market data without requiring external API calls.
    Useful for development, testing, and demonstration purposes. Simulates
    real market behavior with price movements and realistic data patterns.
    """
    
    def __init__(self, config: Any):
        super().__init__(config, PipelineLogger(__name__))
        self.base_prices = {
            'AAPL': 175.50, 'GOOGL': 140.25, 'MSFT': 378.85,
            'AMZN': 145.75, 'TSLA': 248.50, 'META': 325.20,
            'NVDA': 480.30, 'NFLX': 445.80, 'AMD': 125.40,
            'INTC': 42.85, 'ORCL': 115.60, 'CRM': 285.40,
            'UBER': 62.25, 'LYFT': 15.80, 'SNAP': 12.45,
            'BABA': 88.50, 'SHOP': 65.30, 'SQ': 75.40,
            'PYPL': 62.90, 'V': 245.75, 'MA': 385.60,
            'JPM': 155.20, 'BAC': 32.45, 'WFC': 42.80,
            'C': 48.70, 'GS': 385.90, 'MS': 88.40,
            'XOM': 110.25, 'CVX': 155.80, 'WMT': 165.40,
            'PG': 155.30, 'JNJ': 165.90, 'KO': 58.75,
            'PEP': 175.60, 'MCD': 295.80, 'HD': 335.20,
            'LOW': 235.40
        }
        self.current_prices = {}
        self.price_trends = {}
    
    def fetch_data(self, symbol: str) -> Dict[str, Any]:
        """Fetch data for a given symbol."""
        return self.get_real_time_quote(symbol)
    
    def get_real_time_quote(self, symbol: str) -> Dict[str, Any]:
        """Get mock real-time stock quote."""
        try:
            quote = self._generate_quote(symbol)
            self._update_metrics(True)
            return quote
        except Exception as e:
            self._update_metrics(False)
            raise AlphaVantageAPIError(
                f"Failed to generate mock quote for {symbol}: {str(e)}",
                component="mock_alpha_vantage_client",
                context={"symbol": symbol, "function": "GLOBAL_QUOTE"}
            )
    
    def get_intraday_data(self, symbol: str, interval: str = "5min") -> Dict[str, Any]:
        """Get mock intraday data."""
        try:
            data = self._generate_intraday_data(symbol, interval)
            self._update_metrics(True)
            return data
        except Exception as e:
            self._update_metrics(False)
            raise AlphaVantageAPIError(
                f"Failed to generate mock intraday data for {symbol}: {str(e)}",
                component="mock_alpha_vantage_client",
                context={"symbol": symbol, "function": "TIME_SERIES_INTRADAY", "interval": interval}
            )
    
    def _generate_quote(self, symbol: str) -> Dict[str, Any]:
        """Generate realistic stock quote."""
        if symbol not in self.base_prices:
            self.base_prices[symbol] = random.uniform(50, 300)
        
        base_price = self.base_prices[symbol]
        current_price = self.current_prices.get(symbol, base_price)
        
        # Generate price movement
        change_percent = random.uniform(-0.05, 0.05)  # ±5% change
        new_price = current_price * (1 + change_percent)
        self.current_prices[symbol] = new_price
        
        change = new_price - current_price
        change_percent_actual = (change / current_price) * 100
        
        return {
            "Global Quote": {
                "01. symbol": symbol,
                "02. open": f"{current_price:.2f}",
                "03. high": f"{max(current_price, new_price):.2f}",
                "04. low": f"{min(current_price, new_price):.2f}",
                "05. price": f"{new_price:.2f}",
                "06. volume": str(random.randint(1000000, 10000000)),
                "07. latest trading day": datetime.now().strftime("%Y-%m-%d"),
                "08. previous close": f"{current_price:.2f}",
                "09. change": f"{change:.2f}",
                "10. change percent": f"{change_percent_actual:.2f}%"
            }
        }
    
    def _generate_intraday_data(self, symbol: str, interval: str = "5min") -> Dict[str, Any]:
        """Generate mock intraday data with realistic patterns."""
        # Determine number of data points
        data_points = random.randint(80, 100)  # Compact data
        
        # Parse interval to get minutes
        interval_minutes = {
            "1min": 1,
            "5min": 5,
            "15min": 15,
            "30min": 30,
            "60min": 60
        }.get(interval, 5)
        
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
            "5. Output Size": "Compact",
            "6. Time Zone": "US/Eastern"
        }
        
        return {
            "Meta Data": meta_data,
            f"Time Series ({interval})": time_series
        }

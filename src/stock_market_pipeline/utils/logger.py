"""
Simple logging for the stock market pipeline.
Uses YAML configuration - professional enterprise practice.
"""

import logging
import logging.config
import os
from datetime import datetime
from typing import Optional
from contextlib import contextmanager
from pathlib import Path


class PipelineLogger:
    """
    Professional pipeline logger with YAML configuration support.
    
    Provides enterprise-grade logging capabilities with structured logging,
    performance metrics, operation timing, and comprehensive error handling.
    Supports both YAML configuration and fallback basic logging for reliability.
    """
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(name)
        self._setup_logger()
    
    def _setup_logger(self):
        """
        Setup logger using YAML configuration.
        
        Initializes the logger with YAML-based configuration if available,
        otherwise falls back to basic logging configuration for reliability.
        """
        # Load YAML config if not already loaded
        if not hasattr(PipelineLogger, '_config_loaded'):
            self._load_yaml_config()
            PipelineLogger._config_loaded = True
    
    def _load_yaml_config(self):
        """
        Load logging configuration from YAML file.
        
        Attempts to load enterprise logging configuration from YAML file,
        with automatic fallback to basic configuration if YAML loading fails.
        Ensures logs directory exists and handles configuration errors gracefully.
        """
        try:
            import yaml
            
            logs_dir = Path('logs')
            logs_dir.mkdir(exist_ok=True)
            
            # Load YAML config
            config_path = Path('config/logging.yaml')
            if config_path.exists():
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                logging.config.dictConfig(config)
            else:
                # Fallback to basic config
                self._setup_basic_config()
        except Exception as e:
            # Fallback to basic config if YAML loading fails
            print(f"Warning: Failed to load YAML config: {e}. Using basic logging.")
            self._setup_basic_config()
    
    def _setup_basic_config(self):
        """
        Setup basic logging configuration as fallback.
        
        Configures basic logging with console and file handlers when YAML
        configuration is not available or fails to load. Provides reliable
        logging functionality for all pipeline components.
        """
        logs_dir = Path('logs')
        logs_dir.mkdir(exist_ok=True)
        
        # Basic configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('logs/pipeline.log')
            ]
        )
    
    def info(self, message: str, **kwargs):
        """
        Log info message with optional structured data.
        
        Args:
            message: Log message
            **kwargs: Additional structured data to include in log
        """
        if kwargs:
            message = f"{message} | {', '.join([f'{k}={v}' for k, v in kwargs.items()])}"
        self.logger.info(message)
    
    def error(self, message: str, error: Optional[Exception] = None, **kwargs):
        """
        Log error message with exception details and structured data.
        
        Args:
            message: Error message
            error: Optional exception object for detailed error information
            **kwargs: Additional structured data to include in log
        """
        if error:
            kwargs['error_type'] = type(error).__name__
            kwargs['error_message'] = str(error)
        if kwargs:
            message = f"{message} | {', '.join([f'{k}={v}' for k, v in kwargs.items()])}"
        self.logger.error(message, exc_info=error is not None)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        if kwargs:
            message = f"{message} | {', '.join([f'{k}={v}' for k, v in kwargs.items()])}"
        self.logger.warning(message)
    
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        if kwargs:
            message = f"{message} | {', '.join([f'{k}={v}' for k, v in kwargs.items()])}"
        self.logger.debug(message)
    
    @contextmanager
    def operation(self, operation_name: str, **kwargs):
        """
        Context manager for operation timing and performance monitoring.
        
        Provides automatic timing and logging for operations with start/complete
        messages and duration tracking. Handles exceptions gracefully with
        proper error logging and timing information.
        
        Args:
            operation_name: Name of the operation being monitored
            **kwargs: Additional structured data to include in logs
        """
        start_time = datetime.utcnow()
        self.info(f"Starting {operation_name}", **kwargs)
        
        try:
            yield
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.info(f"Completed {operation_name}", duration_seconds=duration, **kwargs)
        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.error(f"Failed {operation_name}", error=e, duration_seconds=duration, **kwargs)
            raise


def get_logger(name: str) -> PipelineLogger:
    """Get a pipeline logger instance."""
    return PipelineLogger(name)

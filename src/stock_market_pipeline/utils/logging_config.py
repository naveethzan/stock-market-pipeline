"""
Logging configuration loader for the stock market pipeline.
Loads YAML configuration and sets up logging.
"""

import logging
import logging.config
import yaml
from pathlib import Path


def setup_logging():
    """
    Setup logging using YAML configuration.
    
    Initializes enterprise logging configuration from YAML file with automatic
    fallback to basic configuration. Ensures logs directory exists and handles
    configuration errors gracefully for reliable logging across the pipeline.
    """
    try:
        logs_dir = Path('logs')
        logs_dir.mkdir(exist_ok=True)
        
        # Load YAML config
        config_path = Path('config/logging.yaml')
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logging.config.dictConfig(config)
            logging.info("Logging configured from YAML")
        else:
            _setup_basic_logging()
            logging.warning("YAML config not found, using basic configuration")
    except Exception as e:
        print(f"Warning: Failed to load YAML config: {e}. Using basic logging.")
        _setup_basic_logging()


def _setup_basic_logging():
    """
    Setup basic logging configuration as fallback.
    
    Configures basic logging with console and file handlers when YAML
    configuration is not available. Provides reliable logging functionality
    for all pipeline components with proper log directory management.
    """
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/pipeline.log')
        ]
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a standard logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)

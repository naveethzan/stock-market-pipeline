"""
Configuration loader utilities for the streaming pipeline.
"""
import os
import yaml
import logging
import logging.config
from pathlib import Path
from typing import Optional

from .settings import ConfigManager


def load_logging_config(config_path: Optional[str] = None) -> None:
    """
    Load logging configuration from YAML file.
    
    Args:
        config_path: Path to logging configuration file
    """
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'config', 'logging.yaml')
    
    config_path = Path(config_path)
    
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Create logs directory if it doesn't exist
        logs_dir = Path('logs')
        logs_dir.mkdir(exist_ok=True)
        
        logging.config.dictConfig(config)
        logging.info(f"Logging configuration loaded from {config_path}")
    else:
        # Fallback to basic configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logging.warning(f"Logging config file not found at {config_path}, using basic configuration")


def load_environment_file(env_file: Optional[str] = None) -> None:
    """
    Load environment variables from .env file.
    
    Args:
        env_file: Path to environment file
    """
    if env_file is None:
        env_file = '.env'
    
    env_path = Path(env_file)
    
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())
        
        logging.info(f"Environment variables loaded from {env_file}")
    else:
        logging.warning(f"Environment file not found at {env_file}")


def validate_configuration(config: ConfigManager) -> bool:
    """Validate essential configuration settings."""
    logger = logging.getLogger(__name__)
    
    # Essential validations only
    if not config.kafka.bootstrap_servers:
        logger.error("Kafka bootstrap servers not configured")
        return False
    
    if not config.stock_symbols:
        logger.error("No stock symbols configured")
        return False
    
    # Skip API key validation in mock mode
    if not config.alpha_vantage.mock_mode and not config.alpha_vantage.api_key:
        logger.error("Alpha Vantage API key required in production mode")
        return False
    
    logger.info("Configuration validation passed")
    return True




def create_directories() -> None:
    """Create essential directories for the streaming pipeline."""
    # Only create essential directories
    Path('logs').mkdir(exist_ok=True)
    Path('checkpoints').mkdir(exist_ok=True)
    
    logging.info("Created essential directories")


def initialize_configuration(env_file: Optional[str] = None, 
                           logging_config: Optional[str] = None) -> ConfigManager:
    """
    Initialize the complete configuration for the streaming pipeline.
    
    Args:
        env_file: Path to environment file
        logging_config: Path to logging configuration file
        
    Returns:
        Configured ConfigManager instance
    """
    # Load environment variables
    load_environment_file(env_file)
    
    # Load logging configuration
    load_logging_config(logging_config)
    
    # Create necessary directories
    create_directories()
    
    # Initialize configuration manager
    config = ConfigManager()
    
    # Validate configuration
    if not validate_configuration(config):
        raise ValueError("Configuration validation failed. Please check your environment variables.")
    
    return config
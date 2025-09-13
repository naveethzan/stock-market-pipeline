"""
Configuration loader utilities for the streaming pipeline.
"""
import os
import yaml
import logging
import logging.config
from pathlib import Path
from typing import Dict, Any, Optional

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
    """
    Validate the configuration for required settings.
    
    Args:
        config: Configuration manager instance
        
    Returns:
        True if configuration is valid, False otherwise
    """
    logger = logging.getLogger(__name__)
    is_valid = True
    
    # Validate Alpha Vantage configuration
    if not config.alpha_vantage.api_key or config.alpha_vantage.api_key == "your_alpha_vantage_api_key_here":
        logger.error("Alpha Vantage API key is not configured")
        is_valid = False
    
    # Validate Kafka configuration
    if not config.kafka.bootstrap_servers:
        logger.error("Kafka bootstrap servers are not configured")
        is_valid = False
    
    # Validate Redshift configuration
    required_redshift_fields = [
        config.redshift.endpoint,
        config.redshift.database,
        config.redshift.user,
        config.redshift.password
    ]
    
    if any(not field or str(field).startswith("your_") or str(field).startswith("mock_") for field in required_redshift_fields):
        logger.error("Redshift configuration is incomplete")
        is_valid = False
    
    # Validate stock symbols
    if not config.stock_symbols:
        logger.error("No stock symbols configured")
        is_valid = False
    
    if is_valid:
        logger.info("Configuration validation passed")
    else:
        logger.error("Configuration validation failed")
    
    return is_valid


def get_spark_config_dict(config: ConfigManager) -> Dict[str, Any]:
    """
    Get Spark configuration as a dictionary.
    
    Args:
        config: Configuration manager instance
        
    Returns:
        Dictionary of Spark configuration settings
    """
    return {
        "spark.app.name": config.spark.app_name,
        "spark.master": config.spark.master,
        "spark.sql.adaptive.enabled": str(config.spark.sql_adaptive_enabled).lower(),
        "spark.sql.adaptive.coalescePartitions.enabled": str(config.spark.sql_adaptive_coalescePartitions_enabled).lower(),
        "spark.serializer": config.spark.serializer,
        "spark.driver.memory": config.spark.driver_memory,
        "spark.executor.memory": config.spark.executor_memory,
        "spark.executor.cores": str(config.spark.executor_cores),
        "spark.driver.maxResultSize": config.spark.max_result_size,
        
        # Streaming specific configurations
        "spark.sql.streaming.checkpointLocation": config.spark.checkpoint_location,
        "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        
        # Kafka specific configurations
        "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "false",
        
        # Performance tuning
        "spark.sql.streaming.metricsEnabled": "true",
        "spark.sql.streaming.numRecentProgressUpdates": "100",
        
        # Kryo serialization for better performance
        "spark.kryo.registrationRequired": "false",
        "spark.kryo.unsafe": "true",
        
        # Dynamic allocation (disabled for streaming)
        "spark.dynamicAllocation.enabled": "false",
        
        # Garbage collection tuning
        "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1PrintRegionRememberedSetInfo",
        "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1PrintRegionRememberedSetInfo"
    }


def create_directories() -> None:
    """Create necessary directories for the streaming pipeline."""
    directories = [
        'logs',
        'checkpoints',
        'data/processed',
        'data/quarantine',
        'data/archive'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    
    logging.info("Created necessary directories")


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
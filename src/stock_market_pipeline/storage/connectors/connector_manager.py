"""
Enhanced Kafka Connect Connector Management

This module provides a comprehensive connector management system for the stock market pipeline.
It handles all three connectors: Bronze S3, Silver S3, and Redshift connectors.

Features:
- Connector lifecycle management (create, update, delete, restart)
- Health monitoring and status checking
- Configuration validation
- Error handling with proper exception hierarchy
- Integration with the new logging system
"""

import json
import os
import requests
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone

from stock_market_pipeline.core.exceptions import StorageError, ConnectorError
from stock_market_pipeline.utils import PipelineLogger


class ConnectorManager:
    """
    Comprehensive Kafka Connect connector management system.
    
    Manages the complete lifecycle of Kafka Connect connectors including
    Bronze S3, Silver S3, and Redshift connectors. Provides health monitoring,
    configuration management, and operational controls with comprehensive
    error handling and logging for the medallion architecture pipeline.
    """
    
    def __init__(self, connect_url: str = "http://localhost:8083", logger: Optional[PipelineLogger] = None):
        """
        Initialize the connector manager.
        
        Args:
            connect_url: Kafka Connect REST API URL
            logger: Optional logger instance
        """
        self.connect_url = connect_url.rstrip('/')
        self.logger = logger or PipelineLogger(__name__)
        
        # Create session with proper headers
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Connector configuration paths
        self.config_paths = {
            'bronze_s3': 'scripts/connectors/configs/bronze/bronze-s3-connector.json',
            'silver_s3': 'scripts/connectors/configs/silver/silver-s3-connector.json',
            'redshift': 'scripts/connectors/configs/gold/redshift-streaming-connector.json'
        }
        
        self.logger.info(
            "Connector manager initialized",
            connect_url=self.connect_url,
            config_paths=list(self.config_paths.values())
        )
    
    def wait_for_connect(self, timeout: int = 120) -> bool:
        """
        Wait for Kafka Connect to be ready.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if Connect is ready, False if timeout
        """
        self.logger.info(f"Waiting for Kafka Connect at {self.connect_url}")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.session.get(f"{self.connect_url}/")
                if response.status_code == 200:
                    self.logger.info("Kafka Connect is ready!")
                    return True
            except requests.RequestException as e:
                self.logger.debug(f"Connect not ready: {e}")
            
            time.sleep(5)
        
        self.logger.error(f"Timeout waiting for Kafka Connect after {timeout} seconds")
        return False
    
    def list_connectors(self) -> List[str]:
        """
        List all connectors.
        
        Returns:
            List of connector names
        """
        try:
            response = self.session.get(f"{self.connect_url}/connectors")
            response.raise_for_status()
            connectors = response.json()
            
            self.logger.info(f"Retrieved {len(connectors)} connectors")
            return connectors
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to list connectors: {e}")
            raise ConnectorError(f"Failed to list connectors: {str(e)}")
    
    def get_connector_status(self, connector_name: str) -> Dict[str, Any]:
        """
        Get connector status and health information.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Connector status dictionary
        """
        try:
            response = self.session.get(f"{self.connect_url}/connectors/{connector_name}/status")
            response.raise_for_status()
            status = response.json()
            
            self.logger.debug(f"Retrieved status for connector {connector_name}")
            return status
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to get status for connector {connector_name}: {e}")
            raise ConnectorError(f"Failed to get status for connector {connector_name}: {str(e)}")
    
    def get_connector_config(self, connector_name: str) -> Dict[str, Any]:
        """
        Get connector configuration.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Connector configuration dictionary
        """
        try:
            response = self.session.get(f"{self.connect_url}/connectors/{connector_name}/config")
            response.raise_for_status()
            config = response.json()
            
            self.logger.debug(f"Retrieved config for connector {connector_name}")
            return config
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to get config for connector {connector_name}: {e}")
            raise ConnectorError(f"Failed to get config for connector {connector_name}: {str(e)}")
    
    def create_connector(self, connector_config: Dict[str, Any]) -> bool:
        """
        Create a new connector.
        
        Args:
            connector_config: Connector configuration dictionary
            
        Returns:
            True if successful, False otherwise
        """
        connector_name = connector_config.get('name')
        if not connector_name:
            raise ConnectorError("Connector configuration must include 'name' field")
        
        try:
            response = self.session.post(
                f"{self.connect_url}/connectors",
                json=connector_config
            )
            
            if response.status_code == 201:
                self.logger.info(f"Connector '{connector_name}' created successfully")
                return True
            elif response.status_code == 409:
                self.logger.warning(f"Connector '{connector_name}' already exists")
                return True
            else:
                error_msg = f"Error creating connector: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                raise ConnectorError(error_msg)
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to create connector {connector_name}: {e}")
            raise ConnectorError(f"Failed to create connector {connector_name}: {str(e)}")
    
    def create_connector_from_file(self, config_file_path: str) -> bool:
        """
        Create connector from configuration file.
        
        Args:
            config_file_path: Path to connector configuration JSON file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(config_file_path, 'r') as f:
                connector_config = json.load(f)
            
            self.logger.info(f"Loading connector config from {config_file_path}")
            return self.create_connector(connector_config)
            
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {config_file_path}")
            raise ConnectorError(f"Configuration file not found: {config_file_path}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in configuration file {config_file_path}: {e}")
            raise ConnectorError(f"Invalid JSON in configuration file {config_file_path}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Failed to create connector from file {config_file_path}: {e}")
            raise ConnectorError(f"Failed to create connector from file {config_file_path}: {str(e)}")
    
    def update_connector(self, connector_name: str, connector_config: Dict[str, Any]) -> bool:
        """
        Update an existing connector.
        
        Args:
            connector_name: Name of the connector
            connector_config: New connector configuration
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.session.put(
                f"{self.connect_url}/connectors/{connector_name}/config",
                json=connector_config.get('config', {})
            )
            
            if response.status_code == 200:
                self.logger.info(f"Connector '{connector_name}' updated successfully")
                return True
            else:
                error_msg = f"Error updating connector: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                raise ConnectorError(error_msg)
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to update connector {connector_name}: {e}")
            raise ConnectorError(f"Failed to update connector {connector_name}: {str(e)}")
    
    def delete_connector(self, connector_name: str) -> bool:
        """
        Delete a connector.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.session.delete(f"{self.connect_url}/connectors/{connector_name}")
            
            if response.status_code == 204:
                self.logger.info(f"Connector '{connector_name}' deleted successfully")
                return True
            elif response.status_code == 404:
                self.logger.warning(f"Connector '{connector_name}' not found")
                return True
            else:
                error_msg = f"Error deleting connector: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                raise ConnectorError(error_msg)
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to delete connector {connector_name}: {e}")
            raise ConnectorError(f"Failed to delete connector {connector_name}: {str(e)}")
    
    def restart_connector(self, connector_name: str) -> bool:
        """
        Restart a connector.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.session.post(f"{self.connect_url}/connectors/{connector_name}/restart")
            
            if response.status_code == 204:
                self.logger.info(f"Connector '{connector_name}' restarted successfully")
                return True
            else:
                error_msg = f"Error restarting connector: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                raise ConnectorError(error_msg)
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to restart connector {connector_name}: {e}")
            raise ConnectorError(f"Failed to restart connector {connector_name}: {str(e)}")
    
    def pause_connector(self, connector_name: str) -> bool:
        """
        Pause a connector.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.session.put(f"{self.connect_url}/connectors/{connector_name}/pause")
            
            if response.status_code == 204:
                self.logger.info(f"Connector '{connector_name}' paused successfully")
                return True
            else:
                error_msg = f"Error pausing connector: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                raise ConnectorError(error_msg)
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to pause connector {connector_name}: {e}")
            raise ConnectorError(f"Failed to pause connector {connector_name}: {str(e)}")
    
    def resume_connector(self, connector_name: str) -> bool:
        """
        Resume a connector.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.session.put(f"{self.connect_url}/connectors/{connector_name}/resume")
            
            if response.status_code == 204:
                self.logger.info(f"Connector '{connector_name}' resumed successfully")
                return True
            else:
                error_msg = f"Error resuming connector: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                raise ConnectorError(error_msg)
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to resume connector {connector_name}: {e}")
            raise ConnectorError(f"Failed to resume connector {connector_name}: {str(e)}")
    
    def get_connector_health(self, connector_name: str) -> Dict[str, Any]:
        """
        Get comprehensive health information for a connector.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Health information dictionary
        """
        try:
            status = self.get_connector_status(connector_name)
            
            health_info = {
                'connector_name': connector_name,
                'status': status.get('connector', {}).get('state', 'UNKNOWN'),
                'tasks': len(status.get('tasks', [])),
                'healthy_tasks': sum(1 for task in status.get('tasks', []) if task.get('state') == 'RUNNING'),
                'failed_tasks': sum(1 for task in status.get('tasks', []) if task.get('state') == 'FAILED'),
                'last_check': datetime.now(timezone.utc).isoformat(),
                'connector_worker_id': status.get('connector', {}).get('worker_id'),
                'connector_trace': status.get('connector', {}).get('trace'),
                'task_details': status.get('tasks', [])
            }
            
            self.logger.debug(f"Retrieved health info for connector {connector_name}")
            return health_info
            
        except Exception as e:
            self.logger.error(f"Failed to get health info for connector {connector_name}: {e}")
            raise ConnectorError(f"Failed to get health info for connector {connector_name}: {str(e)}")
    
    def get_all_connectors_health(self) -> Dict[str, Any]:
        """
        Get health information for all connectors.
        
        Returns:
            Dictionary mapping connector names to health information
        """
        try:
            connectors = self.list_connectors()
            health_info = {}
            
            for connector_name in connectors:
                try:
                    health_info[connector_name] = self.get_connector_health(connector_name)
                except Exception as e:
                    self.logger.warning(f"Failed to get health for connector {connector_name}: {e}")
                    health_info[connector_name] = {
                        'connector_name': connector_name,
                        'status': 'ERROR',
                        'error': str(e),
                        'last_check': datetime.now(timezone.utc).isoformat()
                    }
            
            self.logger.info(f"Retrieved health info for {len(connectors)} connectors")
            return health_info
            
        except Exception as e:
            self.logger.error(f"Failed to get health info for all connectors: {e}")
            raise ConnectorError(f"Failed to get health info for all connectors: {str(e)}")
    
    def create_all_connectors(self) -> Dict[str, bool]:
        """
        Create all three connectors from configuration files.
        
        Deploys the complete medallion architecture by creating Bronze S3,
        Silver S3, and Redshift connectors using their respective configuration
        files with proper error handling and status reporting.
        
        Returns:
            Dictionary mapping connector types to success status
        """
        results = {}
        
        for connector_type, config_path in self.config_paths.items():
            try:
                self.logger.info(f"Creating {connector_type} connector from {config_path}")
                success = self.create_connector_from_file(config_path)
                results[connector_type] = success
                
                if success:
                    self.logger.info(f"Successfully created {connector_type} connector")
                else:
                    self.logger.error(f"Failed to create {connector_type} connector")
                    
            except Exception as e:
                self.logger.error(f"Error creating {connector_type} connector: {e}")
                results[connector_type] = False
        
        return results
    
    def delete_all_connectors(self) -> Dict[str, bool]:
        """
        Delete all three connectors.
        
        Returns:
            Dictionary mapping connector names to success status
        """
        results = {}
        
        for connector_type, config_path in self.config_paths.items():
            try:
                # Extract connector name from config file
                with open(config_path, 'r') as f:
                    config = json.load(f)
                connector_name = config.get('name')
                
                if connector_name:
                    self.logger.info(f"Deleting {connector_name} connector")
                    success = self.delete_connector(connector_name)
                    results[connector_type] = success
                    
                    if success:
                        self.logger.info(f"Successfully deleted {connector_name} connector")
                    else:
                        self.logger.error(f"Failed to delete {connector_name} connector")
                else:
                    self.logger.error(f"No connector name found in {config_path}")
                    results[connector_type] = False
                    
            except Exception as e:
                self.logger.error(f"Error deleting {connector_type} connector: {e}")
                results[connector_type] = False
        
        return results
    
    def is_healthy(self) -> bool:
        """
        Check if the connector manager is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            response = self.session.get(f"{self.connect_url}/")
            return response.status_code == 200
        except Exception:
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get connector manager metrics.
        
        Returns:
            Metrics dictionary
        """
        try:
            connectors = self.list_connectors()
            health_info = self.get_all_connectors_health()
            
            total_connectors = len(connectors)
            healthy_connectors = sum(1 for info in health_info.values() if info.get('status') == 'RUNNING')
            failed_connectors = sum(1 for info in health_info.values() if info.get('status') == 'FAILED')
            
            return {
                'total_connectors': total_connectors,
                'healthy_connectors': healthy_connectors,
                'failed_connectors': failed_connectors,
                'health_percentage': (healthy_connectors / total_connectors * 100) if total_connectors > 0 else 0,
                'last_check': datetime.now(timezone.utc).isoformat(),
                'connect_url': self.connect_url
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get metrics: {e}")
            return {
                'total_connectors': 0,
                'healthy_connectors': 0,
                'failed_connectors': 0,
                'health_percentage': 0,
                'last_check': datetime.now(timezone.utc).isoformat(),
                'connect_url': self.connect_url,
                'error': str(e)
            }

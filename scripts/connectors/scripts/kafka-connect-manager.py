#!/usr/bin/env python3
"""
Kafka Connect Management Script for Streaming Pipeline

This script provides utilities to manage Kafka Connect connectors for the
medallion architecture implementation.
"""

import json
import requests
import time
import argparse
import sys
from typing import Dict, List, Optional


class KafkaConnectManager:
    """Manager class for Kafka Connect operations"""
    
    def __init__(self, connect_url: str = "http://localhost:8083"):
        self.connect_url = connect_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def wait_for_connect(self, timeout: int = 120) -> bool:
        """Wait for Kafka Connect to be ready"""
        print(f"Waiting for Kafka Connect at {self.connect_url}...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.session.get(f"{self.connect_url}/")
                if response.status_code == 200:
                    print("Kafka Connect is ready!")
                    return True
            except requests.exceptions.RequestException:
                pass
            
            print("Kafka Connect not ready, waiting...")
            time.sleep(5)
        
        print(f"Timeout waiting for Kafka Connect after {timeout} seconds")
        return False
    
    def list_connectors(self) -> List[str]:
        """List all connectors"""
        try:
            response = self.session.get(f"{self.connect_url}/connectors")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error listing connectors: {e}")
            return []
    
    def get_connector_status(self, connector_name: str) -> Optional[Dict]:
        """Get connector status"""
        try:
            response = self.session.get(f"{self.connect_url}/connectors/{connector_name}/status")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error getting connector status: {e}")
            return None
    
    def create_connector(self, connector_config: Dict) -> bool:
        """Create a new connector"""
        connector_name = connector_config.get('name')
        if not connector_name:
            print("Connector configuration must include 'name' field")
            return False
        
        try:
            response = self.session.post(
                f"{self.connect_url}/connectors",
                json=connector_config
            )
            
            if response.status_code == 201:
                print(f"Connector '{connector_name}' created successfully")
                return True
            elif response.status_code == 409:
                print(f"Connector '{connector_name}' already exists")
                return True
            else:
                print(f"Error creating connector: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Error creating connector: {e}")
            return False
    
    def update_connector(self, connector_name: str, connector_config: Dict) -> bool:
        """Update an existing connector"""
        try:
            response = self.session.put(
                f"{self.connect_url}/connectors/{connector_name}/config",
                json=connector_config.get('config', {})
            )
            
            if response.status_code == 200:
                print(f"Connector '{connector_name}' updated successfully")
                return True
            else:
                print(f"Error updating connector: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Error updating connector: {e}")
            return False
    
    def delete_connector(self, connector_name: str) -> bool:
        """Delete a connector"""
        try:
            response = self.session.delete(f"{self.connect_url}/connectors/{connector_name}")
            
            if response.status_code == 204:
                print(f"Connector '{connector_name}' deleted successfully")
                return True
            elif response.status_code == 404:
                print(f"Connector '{connector_name}' not found")
                return True
            else:
                print(f"Error deleting connector: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Error deleting connector: {e}")
            return False
    
    def restart_connector(self, connector_name: str) -> bool:
        """Restart a connector"""
        try:
            response = self.session.post(f"{self.connect_url}/connectors/{connector_name}/restart")
            
            if response.status_code == 204:
                print(f"Connector '{connector_name}' restarted successfully")
                return True
            else:
                print(f"Error restarting connector: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Error restarting connector: {e}")
            return False
    
    def pause_connector(self, connector_name: str) -> bool:
        """Pause a connector"""
        try:
            response = self.session.put(f"{self.connect_url}/connectors/{connector_name}/pause")
            
            if response.status_code == 202:
                print(f"Connector '{connector_name}' paused successfully")
                return True
            else:
                print(f"Error pausing connector: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Error pausing connector: {e}")
            return False
    
    def resume_connector(self, connector_name: str) -> bool:
        """Resume a connector"""
        try:
            response = self.session.put(f"{self.connect_url}/connectors/{connector_name}/resume")
            
            if response.status_code == 202:
                print(f"Connector '{connector_name}' resumed successfully")
                return True
            else:
                print(f"Error resuming connector: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Error resuming connector: {e}")
            return False
    
    def get_connector_plugins(self) -> List[Dict]:
        """Get available connector plugins"""
        try:
            response = self.session.get(f"{self.connect_url}/connector-plugins")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error getting connector plugins: {e}")
            return []


def main():
    parser = argparse.ArgumentParser(description="Kafka Connect Manager")
    parser.add_argument("--connect-url", default="http://localhost:8083",
                       help="Kafka Connect REST API URL")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # List connectors
    subparsers.add_parser("list", help="List all connectors")
    
    # Get connector status
    status_parser = subparsers.add_parser("status", help="Get connector status")
    status_parser.add_argument("connector_name", help="Connector name")
    
    # Create connector
    create_parser = subparsers.add_parser("create", help="Create connector")
    create_parser.add_argument("config_file", help="Connector configuration file (JSON)")
    
    # Update connector
    update_parser = subparsers.add_parser("update", help="Update connector")
    update_parser.add_argument("connector_name", help="Connector name")
    update_parser.add_argument("config_file", help="Connector configuration file (JSON)")
    
    # Delete connector
    delete_parser = subparsers.add_parser("delete", help="Delete connector")
    delete_parser.add_argument("connector_name", help="Connector name")
    
    # Restart connector
    restart_parser = subparsers.add_parser("restart", help="Restart connector")
    restart_parser.add_argument("connector_name", help="Connector name")
    
    # Pause connector
    pause_parser = subparsers.add_parser("pause", help="Pause connector")
    pause_parser.add_argument("connector_name", help="Connector name")
    
    # Resume connector
    resume_parser = subparsers.add_parser("resume", help="Resume connector")
    resume_parser.add_argument("connector_name", help="Connector name")
    
    # List plugins
    subparsers.add_parser("plugins", help="List available connector plugins")
    
    # Wait for connect
    wait_parser = subparsers.add_parser("wait", help="Wait for Kafka Connect to be ready")
    wait_parser.add_argument("--timeout", type=int, default=120, help="Timeout in seconds")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    manager = KafkaConnectManager(args.connect_url)
    
    if args.command == "wait":
        success = manager.wait_for_connect(args.timeout)
        return 0 if success else 1
    
    elif args.command == "list":
        connectors = manager.list_connectors()
        if connectors:
            print("Active connectors:")
            for connector in connectors:
                print(f"  - {connector}")
        else:
            print("No connectors found")
    
    elif args.command == "status":
        status = manager.get_connector_status(args.connector_name)
        if status:
            print(json.dumps(status, indent=2))
        else:
            return 1
    
    elif args.command == "create":
        try:
            with open(args.config_file, 'r') as f:
                config = json.load(f)
            success = manager.create_connector(config)
            return 0 if success else 1
        except FileNotFoundError:
            print(f"Configuration file not found: {args.config_file}")
            return 1
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in configuration file: {e}")
            return 1
    
    elif args.command == "update":
        try:
            with open(args.config_file, 'r') as f:
                config = json.load(f)
            success = manager.update_connector(args.connector_name, config)
            return 0 if success else 1
        except FileNotFoundError:
            print(f"Configuration file not found: {args.config_file}")
            return 1
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in configuration file: {e}")
            return 1
    
    elif args.command == "delete":
        success = manager.delete_connector(args.connector_name)
        return 0 if success else 1
    
    elif args.command == "restart":
        success = manager.restart_connector(args.connector_name)
        return 0 if success else 1
    
    elif args.command == "pause":
        success = manager.pause_connector(args.connector_name)
        return 0 if success else 1
    
    elif args.command == "resume":
        success = manager.resume_connector(args.connector_name)
        return 0 if success else 1
    
    elif args.command == "plugins":
        plugins = manager.get_connector_plugins()
        if plugins:
            print("Available connector plugins:")
            for plugin in plugins:
                print(f"  - {plugin['class']} (version: {plugin.get('version', 'unknown')})")
        else:
            print("No connector plugins found")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
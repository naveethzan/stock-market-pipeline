#!/usr/bin/env python3
"""
Gold Snowflake Connector Diagnostic Script

This script performs comprehensive diagnostics for the Gold layer Snowflake connector:
- Infrastructure availability
- Configuration validation
- Connectivity tests
- Common issue detection
- Resolution recommendations

Usage: python3 scripts/diagnose-gold-connector.py
"""

import os
import sys
import json
import requests
import time
from typing import Dict, Any, List
from pathlib import Path
import subprocess
from urllib.parse import urlparse

# Optional Snowflake import
try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_success(message: str):
    print(f"{Colors.GREEN}✅ {message}{Colors.ENDC}")


def print_error(message: str):
    print(f"{Colors.RED}❌ {message}{Colors.ENDC}")


def print_warning(message: str):
    print(f"{Colors.YELLOW}⚠️  {message}{Colors.ENDC}")


def print_info(message: str):
    print(f"{Colors.BLUE}ℹ️  {message}{Colors.ENDC}")


def print_header(message: str):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.BLUE}{message.center(60)}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.ENDC}")


class GoldConnectorDiagnostic:
    def __init__(self):
        self.connect_url = "http://localhost:8083"
        self.issues_found = []
        self.recommendations = []
        
    def check_infrastructure(self) -> bool:
        """Check if required infrastructure is running"""
        print_header("INFRASTRUCTURE CHECK")
        
        services_ok = True
        
        # Check Kafka Connect
        try:
            response = requests.get(f"{self.connect_url}", timeout=5)
            if response.status_code == 200:
                print_success("Kafka Connect is running")
            else:
                print_error(f"Kafka Connect returned status {response.status_code}")
                services_ok = False
                self.issues_found.append("Kafka Connect not responding properly")
        except requests.RequestException:
            print_error("Kafka Connect is not available")
            services_ok = False
            self.issues_found.append("Kafka Connect service is not running")
            self.recommendations.append("Start services with: make start-mock or make start")
        
        # Check Schema Registry
        try:
            schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8085')
            parsed_url = urlparse(schema_registry_url)
            registry_url = f"http://{parsed_url.netloc}"
            
            response = requests.get(f"{registry_url}/subjects", timeout=5)
            if response.status_code == 200:
                print_success("Schema Registry is running")
            else:
                print_warning(f"Schema Registry returned status {response.status_code}")
        except requests.RequestException:
            print_warning("Schema Registry is not available")
            self.issues_found.append("Schema Registry might not be running")
        
        # Check Docker services
        try:
            result = subprocess.run(
                ["docker", "ps", "--format", "table {{.Names}}\t{{.Status}}", "--filter", "name=kafka"],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0 and "kafka" in result.stdout:
                print_success("Kafka Docker services are running")
            else:
                print_error("Kafka Docker services are not running")
                services_ok = False
                self.issues_found.append("Docker services need to be started")
        except subprocess.TimeoutExpired:
            print_error("Docker command timed out")
            services_ok = False
        except FileNotFoundError:
            print_error("Docker is not available")
            services_ok = False
        
        return services_ok
    
    def check_configuration(self) -> bool:
        """Check Gold connector configuration"""
        print_header("CONFIGURATION CHECK")
        
        config_ok = True
        
        # Check connector config file
        config_path = Path("config/kafka-connect/connectors/gold-snowflake-connector.json")
        if config_path.exists():
            print_success("Gold connector config file exists")
            
            try:
                with open(config_path) as f:
                    config = json.load(f)
                    
                # Check key configurations
                connector_config = config.get('config', {})
                
                # Check connector class
                connector_class = connector_config.get('connector.class')
                if connector_class == "com.snowflake.kafka.connector.SnowflakeSinkConnector":
                    print_success("Correct Snowflake connector class")
                else:
                    print_error(f"Invalid connector class: {connector_class}")
                    config_ok = False
                
                # Check value converter
                value_converter = connector_config.get('value.converter')
                if value_converter == "io.confluent.connect.avro.AvroConverter":
                    print_success("Using AvroConverter (matches Silver layer)")
                elif value_converter == "com.snowflake.kafka.connector.records.SnowflakeJsonConverter":
                    print_warning("Using SnowflakeJsonConverter (consider Avro for consistency)")
                    self.recommendations.append("Consider switching to AvroConverter for consistency with Silver layer")
                else:
                    print_error(f"Unexpected value converter: {value_converter}")
                    config_ok = False
                
                # Check topics
                topics = connector_config.get('topics', '')
                expected_topics = ["processed-stock-prices", "processed-trading-volume", "processed-technical-indicators"]
                for topic in expected_topics:
                    if topic in topics:
                        print_success(f"Topic configured: {topic}")
                    else:
                        print_error(f"Missing topic: {topic}")
                        config_ok = False
                
                # Check topic-to-table mapping
                topic_map = connector_config.get('snowflake.topic2table.map', '')
                if topic_map:
                    print_success("Topic to table mapping configured")
                else:
                    print_error("Missing topic to table mapping")
                    config_ok = False
                    
            except json.JSONDecodeError as e:
                print_error(f"Invalid JSON in config file: {e}")
                config_ok = False
        else:
            print_error("Gold connector config file not found")
            config_ok = False
            self.issues_found.append("Gold connector configuration file missing")
        
        return config_ok
    
    def check_environment_variables(self) -> bool:
        """Check required environment variables"""
        print_header("ENVIRONMENT VARIABLES CHECK")
        
        env_ok = True
        
        required_vars = [
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER', 
            'SNOWFLAKE_PASSWORD',
            'SNOWFLAKE_DATABASE',
            'SNOWFLAKE_SCHEMA',
            'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_ROLE'
        ]
        
        for var in required_vars:
            value = os.getenv(var)
            if value:
                print_success(f"{var}: Set")
            else:
                print_error(f"{var}: Not set")
                env_ok = False
                self.issues_found.append(f"Missing environment variable: {var}")
        
        # Check optional but recommended variables
        optional_vars = ['SNOWFLAKE_URL', 'SCHEMA_REGISTRY_URL']
        for var in optional_vars:
            value = os.getenv(var)
            if value:
                print_success(f"{var}: Set ({value})")
            else:
                print_warning(f"{var}: Not set (will use default)")
        
        return env_ok
    
    def test_snowflake_connection(self) -> bool:
        """Test direct Snowflake connectivity"""
        print_header("SNOWFLAKE CONNECTION TEST")
        
        if not SNOWFLAKE_AVAILABLE:
            print_warning("Snowflake connector not installed - skipping connection test")
            self.recommendations.append("Install Snowflake connector: pip install snowflake-connector-python")
            return False
        
        try:
            account = os.getenv('SNOWFLAKE_ACCOUNT')
            user = os.getenv('SNOWFLAKE_USER')
            password = os.getenv('SNOWFLAKE_PASSWORD')
            warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
            database = os.getenv('SNOWFLAKE_DATABASE')
            schema = os.getenv('SNOWFLAKE_SCHEMA')
            role = os.getenv('SNOWFLAKE_ROLE')
            
            if not all([account, user, password, warehouse, database, schema]):
                print_error("Missing required Snowflake credentials")
                return False
            
            print_info("Testing Snowflake connection...")
            
            conn = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                warehouse=warehouse,
                database=database,
                schema=schema,
                role=role,
                timeout=30
            )
            
            cursor = conn.cursor()
            
            # Test basic connectivity
            cursor.execute("SELECT CURRENT_TIMESTAMP()")
            result = cursor.fetchone()
            print_success(f"Snowflake connection successful - Current time: {result[0]}")
            
            # Check if staging tables exist
            tables = ['FACT_STOCK_PRICES_STAGING', 'FACT_TRADING_VOLUME_STAGING', 'TECHNICAL_INDICATORS_STAGING']
            for table in tables:
                cursor.execute(f"SHOW TABLES LIKE '{table}'")
                if cursor.fetchone():
                    print_success(f"Table exists: {table}")
                else:
                    print_warning(f"Table will be auto-created: {table}")
            
            # Check warehouse state
            cursor.execute(f"SHOW WAREHOUSES LIKE '{warehouse}'")
            wh_info = cursor.fetchone()
            if wh_info:
                wh_state = wh_info[3]  # State is typically the 4th column
                if wh_state == 'STARTED':
                    print_success(f"Warehouse {warehouse} is running")
                else:
                    print_warning(f"Warehouse {warehouse} state: {wh_state}")
                    self.recommendations.append(f"Start warehouse: ALTER WAREHOUSE {warehouse} RESUME;")
            
            conn.close()
            return True
            
        except Exception as e:
            print_error(f"Snowflake connection failed: {e}")
            self.issues_found.append(f"Snowflake connectivity issue: {str(e)}")
            return False
    
    def check_existing_connector(self) -> Dict[str, Any]:
        """Check if Gold connector already exists and its status"""
        print_header("EXISTING CONNECTOR CHECK")
        
        try:
            # List all connectors
            response = requests.get(f"{self.connect_url}/connectors", timeout=10)
            if response.status_code == 200:
                connectors = response.json()
                print_info(f"Found {len(connectors)} connectors: {', '.join(connectors)}")
                
                if 'gold-snowflake-sink-connector' in connectors:
                    # Get connector status
                    status_response = requests.get(
                        f"{self.connect_url}/connectors/gold-snowflake-sink-connector/status",
                        timeout=10
                    )
                    
                    if status_response.status_code == 200:
                        status = status_response.json()
                        connector_state = status.get('connector', {}).get('state')
                        
                        if connector_state == 'RUNNING':
                            print_success("Gold connector exists and is RUNNING")
                        elif connector_state == 'FAILED':
                            print_error("Gold connector exists but is FAILED")
                            tasks = status.get('tasks', [])
                            for task in tasks:
                                if 'trace' in task:
                                    print_error(f"Task error: {task['trace'][:200]}...")
                            self.issues_found.append("Gold connector is in FAILED state")
                        else:
                            print_warning(f"Gold connector state: {connector_state}")
                        
                        return status
                    else:
                        print_error(f"Could not get connector status: {status_response.status_code}")
                else:
                    print_info("Gold connector does not exist yet")
                    self.recommendations.append("Deploy Gold connector with: ./scripts/deploy-gold-connector.sh")
            
        except requests.RequestException as e:
            print_error(f"Could not check connectors: {e}")
        
        return {}
    
    def generate_resolution_plan(self):
        """Generate resolution plan based on issues found"""
        print_header("RESOLUTION PLAN")
        
        if not self.issues_found:
            print_success("No issues found! Gold connector should work correctly.")
            return
        
        print_error(f"Found {len(self.issues_found)} issues:")
        for i, issue in enumerate(self.issues_found, 1):
            print(f"  {i}. {issue}")
        
        print_info("\nRecommended Actions:")
        for i, rec in enumerate(self.recommendations, 1):
            print(f"  {i}. {rec}")
        
        # Additional common resolutions
        print_info("\nCommon Resolution Steps:")
        print("  1. Start services: make start-mock")
        print("  2. Check environment: source config/.env")
        print("  3. Deploy connector: ./scripts/deploy-gold-connector.sh")
        print("  4. Monitor status: curl http://localhost:8083/connectors/gold-snowflake-sink-connector/status")
        print("  5. Check logs: docker logs kafka-connect")
    
    def run_full_diagnostic(self):
        """Run complete diagnostic suite"""
        print(f"{Colors.BOLD}{Colors.BLUE}")
        print("╔══════════════════════════════════════════════════════════════╗")
        print("║               GOLD SNOWFLAKE CONNECTOR                       ║")
        print("║                     DIAGNOSTIC TOOL                         ║")
        print("╚══════════════════════════════════════════════════════════════╝")
        print(f"{Colors.ENDC}")
        
        # Load environment variables from .env if exists
        env_path = Path("config/.env")
        if env_path.exists():
            print_info("Loading environment variables from config/.env")
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ.setdefault(key.strip(), value.strip())
        
        # Run all checks
        infra_ok = self.check_infrastructure()
        config_ok = self.check_configuration()
        env_ok = self.check_environment_variables()
        
        # Only test Snowflake if basic checks pass
        if env_ok:
            snowflake_ok = self.test_snowflake_connection()
        else:
            print_warning("Skipping Snowflake test due to missing environment variables")
            snowflake_ok = False
        
        # Check existing connector if infrastructure is up
        if infra_ok:
            self.check_existing_connector()
        
        # Generate resolution plan
        self.generate_resolution_plan()
        
        # Final summary
        print_header("DIAGNOSTIC SUMMARY")
        total_checks = 4
        passed_checks = sum([infra_ok, config_ok, env_ok, snowflake_ok])
        
        if passed_checks == total_checks:
            print_success("All checks passed! Gold connector should work correctly.")
        else:
            print_warning(f"Passed {passed_checks}/{total_checks} checks. See resolution plan above.")


def main():
    """Main diagnostic function"""
    diagnostic = GoldConnectorDiagnostic()
    diagnostic.run_full_diagnostic()


if __name__ == "__main__":
    main()
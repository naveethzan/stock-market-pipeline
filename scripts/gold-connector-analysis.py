#!/usr/bin/env python3
"""
Gold Snowflake Connector Issues Analysis

This script analyzes Gold Snowflake connector issues and provides resolution guidance.
"""

import os
import sys
import json
import requests
import subprocess
from pathlib import Path
from urllib.parse import urlparse


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


def main():
    """Analyze Gold Snowflake connector issues"""
    
    print(f"{Colors.BOLD}{Colors.BLUE}")
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║               GOLD SNOWFLAKE CONNECTOR                       ║")
    print("║                     ISSUES ANALYSIS                         ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"{Colors.ENDC}")
    
    issues_found = []
    recommendations = []
    
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
    
    # 1. Check Infrastructure
    print_header("INFRASTRUCTURE STATUS")
    
    # Check Docker services
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "table {{.Names}}\t{{.Status}}", "--filter", "name=kafka"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0 and "kafka" in result.stdout:
            print_success("Docker services are available")
            
            # Check specific services
            services = ["kafka", "kafka-connect", "schema-registry", "zookeeper"]
            running_services = []
            for service in services:
                if service in result.stdout:
                    running_services.append(service)
                    print_success(f"Service running: {service}")
                else:
                    print_error(f"Service not running: {service}")
                    issues_found.append(f"{service} service is not running")
            
            if not running_services:
                print_error("No Kafka services are running")
                issues_found.append("Infrastructure services are not started")
                recommendations.append("Start services with: make start-mock")
        else:
            print_error("No Kafka Docker services are running")
            issues_found.append("Docker infrastructure is not started")
            recommendations.append("Start services with: make start-mock or make start")
            
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print_error("Docker is not available or not responding")
        issues_found.append("Docker is not available")
        recommendations.append("Ensure Docker Desktop is running")
    
    # 2. Check Kafka Connect
    print_header("KAFKA CONNECT STATUS")
    
    connect_url = "http://localhost:8083"
    try:
        response = requests.get(connect_url, timeout=5)
        if response.status_code == 200:
            print_success("Kafka Connect is available")
            
            # Check existing connectors
            try:
                conn_response = requests.get(f"{connect_url}/connectors", timeout=5)
                if conn_response.status_code == 200:
                    connectors = conn_response.json()
                    print_info(f"Existing connectors: {', '.join(connectors) if connectors else 'None'}")
                    
                    if 'gold-snowflake-sink-connector' in connectors:
                        # Get connector status
                        status_response = requests.get(
                            f"{connect_url}/connectors/gold-snowflake-sink-connector/status",
                            timeout=5
                        )
                        if status_response.status_code == 200:
                            status = status_response.json()
                            state = status.get('connector', {}).get('state', 'UNKNOWN')
                            print_info(f"Gold connector state: {state}")
                            
                            if state == 'FAILED':
                                print_error("Gold connector is in FAILED state")
                                issues_found.append("Gold connector failed")
                                recommendations.append("Check connector logs: docker logs kafka-connect")
                                recommendations.append("Restart connector or redeploy")
                            elif state == 'RUNNING':
                                print_success("Gold connector is running")
                        else:
                            print_warning("Could not get connector status")
                    else:
                        print_info("Gold connector not deployed yet")
                        recommendations.append("Deploy Gold connector: ./scripts/deploy-gold-connector.sh")
                        
            except requests.RequestException:
                print_warning("Could not check connector status")
        else:
            print_error(f"Kafka Connect returned status {response.status_code}")
            issues_found.append("Kafka Connect is not healthy")
    except requests.RequestException:
        print_error("Kafka Connect is not available")
        issues_found.append("Kafka Connect service is not accessible")
        recommendations.append("Verify Kafka Connect is running in Docker")
    
    # 3. Check Configuration Files
    print_header("CONFIGURATION CHECK")
    
    # Check Gold connector config
    config_path = Path("config/kafka-connect/connectors/gold-snowflake-connector.json")
    if config_path.exists():
        print_success("Gold connector config file exists")
        
        try:
            with open(config_path) as f:
                config = json.load(f)
                
            connector_config = config.get('config', {})
            
            # Check value converter
            value_converter = connector_config.get('value.converter')
            if value_converter == "io.confluent.connect.avro.AvroConverter":
                print_success("Using AvroConverter (consistent with Silver layer)")
            elif value_converter == "com.snowflake.kafka.connector.records.SnowflakeJsonConverter":
                print_warning("Using SnowflakeJsonConverter")
                recommendations.append("Consider switching to AvroConverter for consistency")
            else:
                print_error(f"Unexpected value converter: {value_converter}")
                issues_found.append("Invalid value converter configuration")
            
            # Check topics
            topics = connector_config.get('topics', '')
            expected_topics = ["processed-stock-prices", "processed-trading-volume", "processed-technical-indicators"]
            missing_topics = []
            for topic in expected_topics:
                if topic in topics:
                    print_success(f"Topic configured: {topic}")
                else:
                    missing_topics.append(topic)
                    print_error(f"Missing topic: {topic}")
            
            if missing_topics:
                issues_found.append(f"Missing topics in configuration: {', '.join(missing_topics)}")
                
        except json.JSONDecodeError as e:
            print_error(f"Invalid JSON in config file: {e}")
            issues_found.append("Gold connector config has invalid JSON")
            
    else:
        print_error("Gold connector config file not found")
        issues_found.append("Gold connector configuration file missing")
    
    # 4. Check Environment Variables
    print_header("ENVIRONMENT VARIABLES")
    
    required_vars = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_ROLE'
    ]
    
    missing_vars = []
    for var in required_vars:
        if os.getenv(var):
            print_success(f"{var}: Set")
        else:
            missing_vars.append(var)
            print_error(f"{var}: Not set")
    
    if missing_vars:
        issues_found.append(f"Missing environment variables: {', '.join(missing_vars)}")
        recommendations.append("Set missing environment variables in config/.env")
    
    # Check optional variables
    optional_vars = ['SNOWFLAKE_URL', 'SCHEMA_REGISTRY_URL']
    for var in optional_vars:
        if os.getenv(var):
            print_success(f"{var}: Set")
        else:
            print_warning(f"{var}: Not set (will use default)")
    
    # 5. Generate Resolution Plan
    print_header("ISSUES SUMMARY & RESOLUTION PLAN")
    
    if not issues_found:
        print_success("🎉 No issues found! Gold connector should work correctly.")
        print_info("To deploy the connector, run: ./scripts/deploy-gold-connector.sh")
    else:
        print_error(f"Found {len(issues_found)} issues:")
        for i, issue in enumerate(issues_found, 1):
            print(f"  {i}. {issue}")
        
        print_info(f"\n📋 Recommended Actions ({len(recommendations)} steps):")
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")
        
        print_info("\n🛠️  Common Resolution Steps:")
        print("  1. Start services: make start-mock")
        print("  2. Verify environment: cat config/.env")
        print("  3. Wait for services: sleep 30")
        print("  4. Deploy connector: ./scripts/deploy-gold-connector.sh")
        print("  5. Check status: curl http://localhost:8083/connectors/gold-snowflake-sink-connector/status")
        print("  6. Monitor logs: docker logs kafka-connect")
        
        print_info("\n🔧 If connector fails:")
        print("  • Check Snowflake credentials and network connectivity")
        print("  • Verify Snowflake warehouse is running and not suspended")
        print("  • Check if processed topics have data: docker logs streaming-processor")
        print("  • Restart connector: curl -X POST http://localhost:8083/connectors/gold-snowflake-sink-connector/restart")
    
    print_header("DIAGNOSTIC COMPLETE")
    
    if issues_found:
        print_warning("Gold connector has issues that need to be resolved.")
        return 1
    else:
        print_success("Gold connector configuration looks good!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
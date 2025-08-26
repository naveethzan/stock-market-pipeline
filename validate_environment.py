#!/usr/bin/env python3
"""
Environment Validation Script for Stock Market Streaming Pipeline

This script validates all prerequisites before running the pipeline:
- System requirements
- Required software
- Environment variables
- Port availability
- API connectivity

Usage: python validate_environment.py
"""

import os
import sys
import subprocess
import socket
import requests
from typing import List, Dict, Any
import json
from pathlib import Path

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
    print(f"{Colors.BOLD}{Colors.BLUE}{message}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.ENDC}")

def run_command(command: str) -> tuple:
    """Run command and return (success, output)"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=10)
        return result.returncode == 0, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)

def check_software_requirements() -> List[Dict[str, Any]]:
    """Check required software dependencies"""
    print_header("SOFTWARE REQUIREMENTS")
    
    results = []
    requirements = [
        ("Docker", "docker --version"),
        ("Docker Compose", "docker-compose --version"),
        ("Python", "python --version"),
        ("Make", "make --version"),
        ("Java (JDK)", "java -version"),
        ("Git", "git --version")
    ]
    
    for name, command in requirements:
        success, output = run_command(command)
        if success:
            print_success(f"{name}: {output.split()[0] if output else 'Found'}")
            results.append({"name": name, "status": "pass", "details": output})
        else:
            print_error(f"{name}: Not found or not working")
            results.append({"name": name, "status": "fail", "details": output})
    
    return results

def check_port_availability() -> List[Dict[str, Any]]:
    """Check if required ports are available"""
    print_header("PORT AVAILABILITY")
    
    results = []
    required_ports = [
        (2181, "Zookeeper"),
        (9092, "Kafka"),
        (8081, "Producer Health Check"),
        (8082, "Processor Health Check"),
        (8083, "Kafka Connect"),
        (8085, "Schema Registry"),
        (8090, "Kafka UI"),
        (9090, "Prometheus"),
        (3000, "Grafana"),
        (18080, "Spark Master UI")
    ]
    
    for port, service in required_ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('localhost', port))
        sock.close()
        
        if result != 0:  # Port is free
            print_success(f"Port {port} ({service}): Available")
            results.append({"port": port, "service": service, "status": "available"})
        else:  # Port is in use
            print_warning(f"Port {port} ({service}): In use (will need to stop existing service)")
            results.append({"port": port, "service": service, "status": "in_use"})
    
    return results

def check_environment_variables() -> List[Dict[str, Any]]:
    """Check required environment variables"""
    print_header("ENVIRONMENT VARIABLES")
    
    results = []
    
    # Required variables
    required_vars = [
        ("ALPHA_VANTAGE_API_KEY", True, "Alpha Vantage API key for stock data")
    ]
    
    # Optional variables
    optional_vars = [
        ("SNOWFLAKE_ACCOUNT", False, "Snowflake account (optional)"),
        ("SNOWFLAKE_USER", False, "Snowflake user (optional)"),
        ("SNOWFLAKE_PASSWORD", False, "Snowflake password (optional)"),
        ("AWS_ACCESS_KEY_ID", False, "AWS access key for S3 (optional)"),
        ("AWS_SECRET_ACCESS_KEY", False, "AWS secret key for S3 (optional)"),
        ("S3_BUCKET_NAME", False, "S3 bucket name (optional)")
    ]
    
    print_info("Required Environment Variables:")
    for var_name, required, description in required_vars:
        value = os.getenv(var_name)
        if value:
            print_success(f"{var_name}: Set (***hidden***)")
            results.append({"name": var_name, "status": "set", "required": required})
        else:
            print_error(f"{var_name}: Not set - {description}")
            results.append({"name": var_name, "status": "missing", "required": required})
    
    print_info("\nOptional Environment Variables:")
    for var_name, required, description in optional_vars:
        value = os.getenv(var_name)
        if value:
            print_success(f"{var_name}: Set (***hidden***)")
            results.append({"name": var_name, "status": "set", "required": required})
        else:
            print_warning(f"{var_name}: Not set - {description}")
            results.append({"name": var_name, "status": "missing", "required": required})
    
    return results

def check_api_connectivity() -> List[Dict[str, Any]]:
    """Check API connectivity"""
    print_header("API CONNECTIVITY")
    
    results = []
    
    # Test Alpha Vantage API
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if api_key:
        try:
            print_info("Testing Alpha Vantage API connectivity...")
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey={api_key}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if "Global Quote" in data:
                    print_success("Alpha Vantage API: Connected successfully")
                    results.append({"api": "Alpha Vantage", "status": "success", "details": "Valid response"})
                elif "Error Message" in data:
                    print_error(f"Alpha Vantage API: Error - {data['Error Message']}")
                    results.append({"api": "Alpha Vantage", "status": "error", "details": data["Error Message"]})
                elif "Note" in data:
                    print_warning(f"Alpha Vantage API: Rate limited - {data['Note']}")
                    results.append({"api": "Alpha Vantage", "status": "rate_limited", "details": data["Note"]})
                else:
                    print_warning(f"Alpha Vantage API: Unexpected response - {data}")
                    results.append({"api": "Alpha Vantage", "status": "unexpected", "details": str(data)})
            else:
                print_error(f"Alpha Vantage API: HTTP {response.status_code}")
                results.append({"api": "Alpha Vantage", "status": "http_error", "details": f"HTTP {response.status_code}"})
                
        except requests.exceptions.RequestException as e:
            print_error(f"Alpha Vantage API: Connection failed - {str(e)}")
            results.append({"api": "Alpha Vantage", "status": "connection_error", "details": str(e)})
    else:
        print_error("Alpha Vantage API: API key not set, cannot test connectivity")
        results.append({"api": "Alpha Vantage", "status": "no_key", "details": "API key not set"})
    
    return results

def check_docker_resources() -> List[Dict[str, Any]]:
    """Check Docker resources"""
    print_header("DOCKER RESOURCES")
    
    results = []
    
    # Check if Docker is running
    success, output = run_command("docker info")
    if not success:
        print_error("Docker is not running or not accessible")
        results.append({"check": "docker_running", "status": "fail", "details": output})
        return results
    
    print_success("Docker is running")
    results.append({"check": "docker_running", "status": "pass", "details": "Docker daemon accessible"})
    
    # Check Docker memory allocation (if possible)
    try:
        success, output = run_command("docker system info --format '{{.MemTotal}}'")
        if success and output:
            memory_bytes = int(output)
            memory_gb = memory_bytes / (1024**3)
            if memory_gb >= 6:
                print_success(f"Docker Memory: {memory_gb:.1f}GB (sufficient)")
                results.append({"check": "docker_memory", "status": "pass", "details": f"{memory_gb:.1f}GB"})
            else:
                print_warning(f"Docker Memory: {memory_gb:.1f}GB (recommended 6GB+)")
                results.append({"check": "docker_memory", "status": "warning", "details": f"{memory_gb:.1f}GB"})
        else:
            print_info("Docker Memory: Could not determine allocation")
            results.append({"check": "docker_memory", "status": "unknown", "details": "Could not determine"})
    except Exception as e:
        print_info(f"Docker Memory: Could not check - {str(e)}")
        results.append({"check": "docker_memory", "status": "unknown", "details": str(e)})
    
    return results

def check_file_structure() -> List[Dict[str, Any]]:
    """Check project file structure"""
    print_header("PROJECT FILE STRUCTURE")
    
    results = []
    required_files = [
        "docker-compose.yaml",
        "requirements-streaming.txt",
        "src/streaming_pipeline/producers/alpha_vantage_app.py",
        "src/streaming_pipeline/processors/spark_processor.py",
        "src/streaming_pipeline/warehouse/snowflake_client.py",
        "config/kafka-connect/connectors/bronze-s3-connector.json",
        "config/kafka-connect/connectors/gold-snowflake-connector.json"
    ]
    
    for file_path in required_files:
        if Path(file_path).exists():
            print_success(f"File exists: {file_path}")
            results.append({"file": file_path, "status": "exists"})
        else:
            print_error(f"File missing: {file_path}")
            results.append({"file": file_path, "status": "missing"})
    
    # Check for .env file
    env_files = ["config/.env", ".env"]
    env_found = False
    for env_file in env_files:
        if Path(env_file).exists():
            print_success(f"Environment file found: {env_file}")
            env_found = True
            results.append({"file": env_file, "status": "exists"})
            break
    
    if not env_found:
        print_warning("No .env file found - you'll need to set environment variables manually")
        results.append({"file": ".env", "status": "missing"})
    
    return results

def generate_report(all_results: Dict[str, List[Dict[str, Any]]]) -> None:
    """Generate a summary report"""
    print_header("VALIDATION SUMMARY")
    
    total_checks = 0
    passed_checks = 0
    failed_checks = 0
    warnings = 0
    
    for category, results in all_results.items():
        for result in results:
            total_checks += 1
            status = result.get("status", "unknown")
            if status in ["pass", "set", "success", "exists", "available"]:
                passed_checks += 1
            elif status in ["fail", "missing", "error", "connection_error", "http_error", "no_key"]:
                failed_checks += 1
            else:
                warnings += 1
    
    print(f"\n📊 Overall Results:")
    print(f"   Total Checks: {total_checks}")
    print_success(f"Passed: {passed_checks}")
    if warnings > 0:
        print_warning(f"Warnings: {warnings}")
    if failed_checks > 0:
        print_error(f"Failed: {failed_checks}")
    
    # Determine overall status
    if failed_checks == 0:
        if warnings == 0:
            print_success("\n🎉 All checks passed! Your environment is ready to run the pipeline.")
        else:
            print_warning("\n⚠️  All critical checks passed with some warnings. You can proceed but review the warnings.")
    else:
        print_error(f"\n🛑 {failed_checks} critical check(s) failed. Please fix these issues before running the pipeline.")
    
    # Required actions
    print_info("\n📋 Next Steps:")
    if failed_checks == 0:
        print("   1. Run: make docker-build")
        print("   2. Run: make docker-up")
        print("   3. Monitor: curl http://localhost:8081/health")
    else:
        print("   1. Fix the failed checks above")
        print("   2. Re-run this validation script")
        print("   3. When all checks pass, start the pipeline")

def main():
    """Main validation function"""
    print(f"{Colors.BOLD}{Colors.BLUE}")
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║        Stock Market Streaming Pipeline - Environment        ║")
    print("║                      Validation Script                      ║") 
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"{Colors.ENDC}")
    
    all_results = {}
    
    # Run all validation checks
    all_results["software"] = check_software_requirements()
    all_results["ports"] = check_port_availability()
    all_results["environment"] = check_environment_variables()
    all_results["api"] = check_api_connectivity()
    all_results["docker"] = check_docker_resources()
    all_results["files"] = check_file_structure()
    
    # Generate summary report
    generate_report(all_results)
    
    # Save detailed results to file
    try:
        with open("validation_results.json", "w") as f:
            json.dump(all_results, f, indent=2, default=str)
        print_info(f"\n💾 Detailed results saved to: validation_results.json")
    except Exception as e:
        print_warning(f"Could not save results to file: {str(e)}")

if __name__ == "__main__":
    main()
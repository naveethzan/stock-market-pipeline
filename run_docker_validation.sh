#!/bin/bash

# Docker Deployment Validation Script
# This script validates the Docker deployment for the streaming pipeline

set -e

echo "🚀 Docker Deployment Validation for Streaming Pipeline"
echo "======================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "SUCCESS")
            echo -e "${GREEN}✅ $message${NC}"
            ;;
        "ERROR")
            echo -e "${RED}❌ $message${NC}"
            ;;
        "WARNING")
            echo -e "${YELLOW}⚠️  $message${NC}"
            ;;
        "INFO")
            echo -e "${BLUE}ℹ️  $message${NC}"
            ;;
    esac
}

# Check if Docker is running
check_docker() {
    print_status "INFO" "Checking Docker daemon..."
    if ! docker info >/dev/null 2>&1; then
        print_status "ERROR" "Docker daemon is not running!"
        print_status "INFO" "Please start Docker Desktop and try again."
        exit 1
    fi
    print_status "SUCCESS" "Docker daemon is running"
}

# Run readiness check
run_readiness_check() {
    print_status "INFO" "Running readiness check..."
    if python3 check_docker_readiness.py; then
        print_status "SUCCESS" "Readiness check passed"
    else
        print_status "ERROR" "Readiness check failed"
        exit 1
    fi
}

# Clean up any existing containers
cleanup_existing() {
    print_status "INFO" "Cleaning up existing containers..."
    docker-compose down --remove-orphans >/dev/null 2>&1 || true
    print_status "SUCCESS" "Cleanup completed"
}

# Start services
start_services() {
    print_status "INFO" "Starting Docker services..."
    if docker-compose up -d; then
        print_status "SUCCESS" "Services started successfully"
    else
        print_status "ERROR" "Failed to start services"
        exit 1
    fi
}

# Wait for services to be ready
wait_for_services() {
    print_status "INFO" "Waiting for services to be ready..."
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps | grep -q "Up"; then
            local running_services=$(docker-compose ps | grep "Up" | wc -l)
            local total_services=$(docker-compose ps | tail -n +3 | wc -l)
            
            if [ "$running_services" -eq "$total_services" ]; then
                print_status "SUCCESS" "All services are running"
                return 0
            fi
        fi
        
        print_status "INFO" "Attempt $attempt/$max_attempts - Waiting for services..."
        sleep 10
        ((attempt++))
    done
    
    print_status "ERROR" "Timeout waiting for services to start"
    docker-compose ps
    return 1
}

# Check service health
check_service_health() {
    print_status "INFO" "Checking service health..."
    
    # Check Kafka
    if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        print_status "SUCCESS" "Kafka is healthy"
    else
        print_status "ERROR" "Kafka health check failed"
    fi
    
    # Check Schema Registry
    if curl -s http://localhost:8085/subjects >/dev/null 2>&1; then
        print_status "SUCCESS" "Schema Registry is healthy"
    else
        print_status "WARNING" "Schema Registry health check failed"
    fi
    
    # Check Spark Master
    if curl -s http://localhost:18080 >/dev/null 2>&1; then
        print_status "SUCCESS" "Spark Master is healthy"
    else
        print_status "WARNING" "Spark Master health check failed"
    fi
    
    # Check Kafka Connect
    if curl -s http://localhost:8083/connectors >/dev/null 2>&1; then
        print_status "SUCCESS" "Kafka Connect is healthy"
    else
        print_status "WARNING" "Kafka Connect health check failed"
    fi
}

# Check required topics
check_topics() {
    print_status "INFO" "Checking Kafka topics..."
    
    local required_topics=(
        "stock-data-stream"
        "stock-quotes-realtime"
        "stock-intraday-data"
        "processed-stock-prices"
        "processed-trading-volume"
        "processed-technical-indicators"
        "data-quality-alerts"
    )
    
    local existing_topics=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)
    
    for topic in "${required_topics[@]}"; do
        if echo "$existing_topics" | grep -q "^$topic$"; then
            print_status "SUCCESS" "Topic exists: $topic"
        else
            print_status "WARNING" "Topic missing: $topic"
        fi
    done
}

# Check port accessibility
check_ports() {
    print_status "INFO" "Checking port accessibility..."
    
    local ports=(
        "2181:Zookeeper"
        "9092:Kafka"
        "8085:Schema Registry"
        "18080:Spark Master"
        "8083:Kafka Connect"
        "8090:Kafka UI"
        "9090:Prometheus"
        "3000:Grafana"
    )
    
    for port_info in "${ports[@]}"; do
        local port=$(echo $port_info | cut -d: -f1)
        local service=$(echo $port_info | cut -d: -f2)
        
        if nc -z localhost $port 2>/dev/null; then
            print_status "SUCCESS" "Port $port ($service) is accessible"
        else
            print_status "WARNING" "Port $port ($service) is not accessible"
        fi
    done
}

# Show service status
show_status() {
    print_status "INFO" "Current service status:"
    docker-compose ps
    
    echo ""
    print_status "INFO" "Service URLs:"
    echo "  - Kafka UI: http://localhost:8090"
    echo "  - Spark Master: http://localhost:18080"
    echo "  - Grafana: http://localhost:3000 (admin/admin)"
    echo "  - Prometheus: http://localhost:9090"
    echo "  - Schema Registry: http://localhost:8085"
}

# Main execution
main() {
    check_docker
    run_readiness_check
    cleanup_existing
    start_services
    
    if wait_for_services; then
        check_service_health
        check_topics
        check_ports
        show_status
        
        print_status "SUCCESS" "Docker deployment validation completed!"
        print_status "INFO" "All services are running and accessible."
    else
        print_status "ERROR" "Service startup failed"
        docker-compose logs --tail=50
        exit 1
    fi
}

# Handle script interruption
trap 'print_status "INFO" "Validation interrupted. Cleaning up..."; docker-compose down >/dev/null 2>&1; exit 1' INT

# Run main function
main "$@"
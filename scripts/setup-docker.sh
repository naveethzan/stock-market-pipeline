#!/bin/bash

# Setup script for Streaming Pipeline Docker environment
# This script helps initialize the Docker environment and validate the setup

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check Docker and Docker Compose
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    if ! command_exists docker; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command_exists docker-compose; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check Docker daemon
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker daemon is not running. Please start Docker first."
        exit 1
    fi
    
    print_success "Prerequisites check passed"
}

# Function to setup environment configuration
setup_environment() {
    print_status "Setting up environment configuration..."
    
    if [ ! -f "config/.env" ]; then
        print_status "Creating .env file from template..."
        cp config/.env.streaming.template config/.env
        print_warning "Please edit config/.env with your actual configuration values"
        print_warning "Especially set your ALPHA_VANTAGE_API_KEY"
    else
        print_success "Environment file already exists"
    fi
    
    # Create necessary directories
    print_status "Creating necessary directories..."
    mkdir -p logs data checkpoints
    
    print_success "Environment setup completed"
}

# Function to build Docker images
build_images() {
    print_status "Building Docker images..."
    
    print_status "Building streaming producer..."
    docker build -f Dockerfile.streaming-producer -t streaming-producer:latest .
    
    print_status "Building streaming processor..."
    docker build -f Dockerfile.streaming-processor -t streaming-processor:latest .
    
    print_success "Docker images built successfully"
}

# Function to start infrastructure services
start_infrastructure() {
    print_status "Starting infrastructure services..."
    
    # Start Kafka, Zookeeper, and Spark
    docker-compose -f docker-compose.yaml up -d zookeeper kafka spark-master spark-worker
    
    # Wait for Kafka to be ready
    print_status "Waiting for Kafka to be ready..."
    sleep 30
    
    # Initialize Kafka topics
    docker-compose -f docker-compose.yaml up kafka-topics-init
    
    print_success "Infrastructure services started"
}

# Function to start streaming services
start_streaming() {
    print_status "Starting streaming services..."
    
    docker-compose -f docker-compose.yaml -f docker-compose.streaming.yaml up -d streaming-producer streaming-processor
    
    print_success "Streaming services started"
}

# Function to validate services
validate_services() {
    print_status "Validating services..."
    
    # Wait for services to start
    print_status "Waiting for services to initialize..."
    sleep 60
    
    # Check service status
    print_status "Checking service status..."
    docker-compose -f docker-compose.yaml -f docker-compose.streaming.yaml ps
    
    # Check health endpoints
    print_status "Checking health endpoints..."
    
    # Producer health check
    if curl -s -f http://localhost:8081/health >/dev/null 2>&1; then
        print_success "Producer health check passed"
    else
        print_warning "Producer health check failed - service may still be starting"
    fi
    
    # Processor health check
    if curl -s -f http://localhost:8082/health >/dev/null 2>&1; then
        print_success "Processor health check passed"
    else
        print_warning "Processor health check failed - service may still be starting"
    fi
    
    print_success "Service validation completed"
}

# Function to show service information
show_info() {
    print_status "Service Information:"
    echo ""
    echo "Producer Health Check: http://localhost:8081/health"
    echo "Producer Metrics:      http://localhost:8081/metrics"
    echo "Processor Health Check: http://localhost:8082/health"
    echo "Processor Queries:     http://localhost:8082/queries"
    echo ""
    echo "Kafka UI:              http://localhost:8090"
    echo "Spark UI:              http://localhost:18080"
    echo "Prometheus:            http://localhost:9090"
    echo "Grafana:               http://localhost:3000 (admin/admin)"
    echo ""
    echo "Useful commands:"
    echo "  View logs:           make -f Makefile.streaming-docker logs"
    echo "  Check health:        make -f Makefile.streaming-docker health"
    echo "  Stop services:       make -f Makefile.streaming-docker down"
    echo "  Restart services:    make -f Makefile.streaming-docker restart"
}

# Function to cleanup (for development)
cleanup() {
    print_status "Cleaning up services..."
    docker-compose -f docker-compose.yaml -f docker-compose.streaming.yaml down -v
    docker system prune -f
    print_success "Cleanup completed"
}

# Main function
main() {
    echo "=========================================="
    echo "Streaming Pipeline Docker Setup"
    echo "=========================================="
    echo ""
    
    case "${1:-setup}" in
        "setup")
            check_prerequisites
            setup_environment
            build_images
            start_infrastructure
            start_streaming
            validate_services
            show_info
            ;;
        "build")
            check_prerequisites
            build_images
            ;;
        "start")
            check_prerequisites
            start_infrastructure
            start_streaming
            ;;
        "validate")
            validate_services
            ;;
        "info")
            show_info
            ;;
        "cleanup")
            cleanup
            ;;
        "help")
            echo "Usage: $0 [command]"
            echo ""
            echo "Commands:"
            echo "  setup     - Full setup (default)"
            echo "  build     - Build Docker images only"
            echo "  start     - Start services only"
            echo "  validate  - Validate running services"
            echo "  info      - Show service information"
            echo "  cleanup   - Clean up all services and volumes"
            echo "  help      - Show this help message"
            ;;
        *)
            print_error "Unknown command: $1"
            echo "Use '$0 help' for usage information"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
#!/usr/bin/env python3
"""
Docker Setup Validation Script

This script validates that the streaming pipeline Docker containers
are running correctly and can communicate with each other.
"""
import json
import requests
import time
import sys
from typing import Dict, Any, Optional


class DockerValidator:
    """Validates Docker container setup for streaming pipeline."""
    
    def __init__(self):
        self.producer_url = "http://localhost:8081"
        self.processor_url = "http://localhost:8082"
        self.kafka_ui_url = "http://localhost:8090"
        self.spark_ui_url = "http://localhost:18080"
        
    def check_service_health(self, service_name: str, url: str, timeout: int = 10) -> Dict[str, Any]:
        """Check health of a service."""
        try:
            response = requests.get(f"{url}/health", timeout=timeout)
            if response.status_code == 200:
                return {
                    "status": "healthy",
                    "response_time": response.elapsed.total_seconds(),
                    "data": response.json()
                }
            else:
                return {
                    "status": "unhealthy",
                    "status_code": response.status_code,
                    "response": response.text[:200]
                }
        except requests.exceptions.RequestException as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    def check_service_availability(self, service_name: str, url: str, timeout: int = 5) -> bool:
        """Check if a service is available."""
        try:
            response = requests.get(url, timeout=timeout)
            return response.status_code < 500
        except requests.exceptions.RequestException:
            return False
    
    def validate_producer(self) -> Dict[str, Any]:
        """Validate producer service."""
        print("🔍 Validating Producer Service...")
        
        result = self.check_service_health("producer", self.producer_url)
        
        if result["status"] == "healthy":
            print("✅ Producer is healthy")
            
            # Check metrics endpoint
            try:
                metrics_response = requests.get(f"{self.producer_url}/metrics", timeout=5)
                if metrics_response.status_code == 200:
                    metrics = metrics_response.json()
                    print(f"   📊 Messages sent: {metrics.get('metrics', {}).get('messages', {}).get('sent', 'N/A')}")
                    print(f"   📊 API requests: {metrics.get('metrics', {}).get('api', {}).get('requests', 'N/A')}")
                    result["metrics"] = metrics
                else:
                    print("⚠️  Metrics endpoint not available")
            except Exception as e:
                print(f"⚠️  Failed to get metrics: {str(e)}")
        else:
            print(f"❌ Producer is not healthy: {result}")
        
        return result
    
    def validate_processor(self) -> Dict[str, Any]:
        """Validate processor service."""
        print("🔍 Validating Processor Service...")
        
        result = self.check_service_health("processor", self.processor_url)
        
        if result["status"] == "healthy":
            print("✅ Processor is healthy")
            
            # Check queries endpoint
            try:
                queries_response = requests.get(f"{self.processor_url}/queries", timeout=5)
                if queries_response.status_code == 200:
                    queries = queries_response.json()
                    print(f"   📊 Active queries: {len(queries)}")
                    for query_name, query_status in queries.items():
                        is_active = query_status.get("is_active", False)
                        status_icon = "✅" if is_active else "❌"
                        print(f"   {status_icon} Query '{query_name}': {'Active' if is_active else 'Inactive'}")
                    result["queries"] = queries
                else:
                    print("⚠️  Queries endpoint not available")
            except Exception as e:
                print(f"⚠️  Failed to get queries: {str(e)}")
        else:
            print(f"❌ Processor is not healthy: {result}")
        
        return result
    
    def validate_infrastructure(self) -> Dict[str, Any]:
        """Validate infrastructure services."""
        print("🔍 Validating Infrastructure Services...")
        
        results = {}
        
        # Check Kafka UI
        if self.check_service_availability("kafka-ui", self.kafka_ui_url):
            print("✅ Kafka UI is available")
            results["kafka_ui"] = "available"
        else:
            print("❌ Kafka UI is not available")
            results["kafka_ui"] = "unavailable"
        
        # Check Spark UI
        if self.check_service_availability("spark-ui", self.spark_ui_url):
            print("✅ Spark UI is available")
            results["spark_ui"] = "available"
        else:
            print("❌ Spark UI is not available")
            results["spark_ui"] = "unavailable"
        
        return results
    
    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """Run comprehensive validation of all services."""
        print("🚀 Starting Comprehensive Docker Validation")
        print("=" * 50)
        
        results = {
            "timestamp": time.time(),
            "overall_status": "unknown"
        }
        
        # Validate producer
        results["producer"] = self.validate_producer()
        print()
        
        # Validate processor
        results["processor"] = self.validate_processor()
        print()
        
        # Validate infrastructure
        results["infrastructure"] = self.validate_infrastructure()
        print()
        
        # Determine overall status
        producer_healthy = results["producer"]["status"] == "healthy"
        processor_healthy = results["processor"]["status"] == "healthy"
        kafka_ui_available = results["infrastructure"].get("kafka_ui") == "available"
        spark_ui_available = results["infrastructure"].get("spark_ui") == "available"
        
        if producer_healthy and processor_healthy:
            results["overall_status"] = "healthy"
            print("🎉 Overall Status: HEALTHY")
            print("✅ All critical services are running correctly")
        elif producer_healthy or processor_healthy:
            results["overall_status"] = "partial"
            print("⚠️  Overall Status: PARTIAL")
            print("⚠️  Some services are not healthy")
        else:
            results["overall_status"] = "unhealthy"
            print("❌ Overall Status: UNHEALTHY")
            print("❌ Critical services are not running")
        
        print()
        print("📋 Summary:")
        print(f"   Producer: {'✅' if producer_healthy else '❌'}")
        print(f"   Processor: {'✅' if processor_healthy else '❌'}")
        print(f"   Kafka UI: {'✅' if kafka_ui_available else '❌'}")
        print(f"   Spark UI: {'✅' if spark_ui_available else '❌'}")
        
        return results
    
    def wait_for_services(self, max_wait_time: int = 300) -> bool:
        """Wait for services to become healthy."""
        print(f"⏳ Waiting for services to become healthy (max {max_wait_time}s)...")
        
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            producer_result = self.check_service_health("producer", self.producer_url, timeout=5)
            processor_result = self.check_service_health("processor", self.processor_url, timeout=5)
            
            if producer_result["status"] == "healthy" and processor_result["status"] == "healthy":
                elapsed = int(time.time() - start_time)
                print(f"✅ Services are healthy after {elapsed}s")
                return True
            
            print("⏳ Services still starting up...")
            time.sleep(10)
        
        print("❌ Services did not become healthy within the timeout period")
        return False


def main():
    """Main validation function."""
    validator = DockerValidator()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--wait":
        # Wait for services to become healthy first
        if not validator.wait_for_services():
            print("❌ Validation failed: Services did not start properly")
            sys.exit(1)
    
    # Run comprehensive validation
    results = validator.run_comprehensive_validation()
    
    # Save results to file
    with open("docker-validation-results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\n📄 Detailed results saved to: docker-validation-results.json")
    
    # Exit with appropriate code
    if results["overall_status"] == "healthy":
        sys.exit(0)
    elif results["overall_status"] == "partial":
        sys.exit(1)
    else:
        sys.exit(2)


if __name__ == "__main__":
    main()
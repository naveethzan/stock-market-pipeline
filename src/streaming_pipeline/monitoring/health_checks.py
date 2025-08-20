"""
Health check implementations for Kafka Connect connectors and pipeline components.

Provides comprehensive health monitoring for:
- Kafka Connect connectors (Bronze, Silver, Gold)
- Pipeline components status
- System dependencies
"""

import requests
import json
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta

from .logger import PipelineLogger, MedallionLayer


class HealthStatus(Enum):
    """Health check status values."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check operation."""
    component: str
    status: HealthStatus
    message: str
    timestamp: str
    details: Dict[str, Any]
    response_time_ms: Optional[float] = None


class KafkaConnectHealthChecker:
    """Health checker for Kafka Connect connectors."""
    
    def __init__(self, connect_url: str = "http://localhost:8083", logger: Optional[PipelineLogger] = None):
        self.connect_url = connect_url.rstrip('/')
        self.logger = logger or PipelineLogger(__name__)
        
        # Medallion architecture connector mappings
        self.medallion_connectors = {
            MedallionLayer.BRONZE: ["bronze-s3-connector"],
            MedallionLayer.SILVER: ["silver-s3-connector"],
            MedallionLayer.GOLD: ["gold-snowflake-connector"]
        }
    
    def check_connector_health(self, connector_name: str) -> HealthCheckResult:
        """Check health of a specific Kafka Connect connector."""
        start_time = time.time()
        
        try:
            # Get connector status
            response = requests.get(
                f"{self.connect_url}/connectors/{connector_name}/status",
                timeout=10
            )
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                status_data = response.json()
                connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                
                # Check task states
                tasks = status_data.get('tasks', [])
                task_states = [task.get('state', 'UNKNOWN') for task in tasks]
                
                # Determine overall health
                if connector_state == 'RUNNING' and all(state == 'RUNNING' for state in task_states):
                    status = HealthStatus.HEALTHY
                    message = f"Connector {connector_name} is running normally"
                elif connector_state == 'RUNNING' and any(state == 'RUNNING' for state in task_states):
                    status = HealthStatus.DEGRADED
                    message = f"Connector {connector_name} is partially running"
                else:
                    status = HealthStatus.UNHEALTHY
                    message = f"Connector {connector_name} is not running properly"
                
                details = {
                    'connector_state': connector_state,
                    'task_count': len(tasks),
                    'task_states': task_states,
                    'raw_status': status_data
                }
                
            elif response.status_code == 404:
                status = HealthStatus.UNHEALTHY
                message = f"Connector {connector_name} not found"
                details = {'error': 'Connector not found'}
                
            else:
                status = HealthStatus.UNHEALTHY
                message = f"Failed to get connector status: HTTP {response.status_code}"
                details = {'http_status': response.status_code, 'response': response.text}
        
        except requests.exceptions.RequestException as e:
            response_time = (time.time() - start_time) * 1000
            status = HealthStatus.UNHEALTHY
            message = f"Connection error: {str(e)}"
            details = {'error_type': type(e).__name__, 'error_message': str(e)}
        
        return HealthCheckResult(
            component=f"kafka-connect-{connector_name}",
            status=status,
            message=message,
            timestamp=datetime.utcnow().isoformat(),
            details=details,
            response_time_ms=response_time
        )
    
    def check_layer_health(self, layer: MedallionLayer) -> List[HealthCheckResult]:
        """Check health of all connectors in a medallion layer."""
        results = []
        connectors = self.medallion_connectors.get(layer, [])
        
        for connector_name in connectors:
            result = self.check_connector_health(connector_name)
            results.append(result)
            
            # Log health check result
            if result.status == HealthStatus.HEALTHY:
                self.logger.info(
                    f"Health check passed for {connector_name}",
                    layer=layer,
                    component="health_checker",
                    operation="connector_health_check",
                    metadata={
                        'connector_name': connector_name,
                        'response_time_ms': result.response_time_ms
                    }
                )
            else:
                self.logger.warning(
                    f"Health check failed for {connector_name}: {result.message}",
                    layer=layer,
                    component="health_checker",
                    operation="connector_health_check",
                    metadata={
                        'connector_name': connector_name,
                        'status': result.status.value,
                        'details': result.details
                    }
                )
        
        return results
    
    def check_all_medallion_connectors(self) -> Dict[str, List[HealthCheckResult]]:
        """Check health of all medallion architecture connectors."""
        results = {}
        
        for layer in [MedallionLayer.BRONZE, MedallionLayer.SILVER, MedallionLayer.GOLD]:
            results[layer.value] = self.check_layer_health(layer)
        
        return results
    
    def get_connector_metrics(self, connector_name: str) -> Dict[str, Any]:
        """Get detailed metrics for a connector."""
        try:
            # Get connector config
            config_response = requests.get(
                f"{self.connect_url}/connectors/{connector_name}/config",
                timeout=10
            )
            
            # Get connector tasks
            tasks_response = requests.get(
                f"{self.connect_url}/connectors/{connector_name}/tasks",
                timeout=10
            )
            
            metrics = {
                'config': config_response.json() if config_response.status_code == 200 else None,
                'tasks': tasks_response.json() if tasks_response.status_code == 200 else None,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return metrics
            
        except requests.exceptions.RequestException as e:
            return {
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }


class PipelineHealthChecker:
    """Comprehensive health checker for the entire streaming pipeline."""
    
    def __init__(self, kafka_connect_url: str = "http://localhost:8083", logger: Optional[PipelineLogger] = None):
        self.kafka_connect_checker = KafkaConnectHealthChecker(kafka_connect_url, logger)
        self.logger = logger or PipelineLogger(__name__)
    
    def check_kafka_connect_cluster(self) -> HealthCheckResult:
        """Check overall Kafka Connect cluster health."""
        start_time = time.time()
        
        try:
            response = requests.get(
                f"{self.kafka_connect_checker.connect_url}/",
                timeout=10
            )
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                cluster_info = response.json()
                status = HealthStatus.HEALTHY
                message = "Kafka Connect cluster is accessible"
                details = cluster_info
            else:
                status = HealthStatus.UNHEALTHY
                message = f"Kafka Connect cluster returned HTTP {response.status_code}"
                details = {'http_status': response.status_code}
        
        except requests.exceptions.RequestException as e:
            response_time = (time.time() - start_time) * 1000
            status = HealthStatus.UNHEALTHY
            message = f"Cannot reach Kafka Connect cluster: {str(e)}"
            details = {'error': str(e)}
        
        return HealthCheckResult(
            component="kafka-connect-cluster",
            status=status,
            message=message,
            timestamp=datetime.utcnow().isoformat(),
            details=details,
            response_time_ms=response_time
        )
    
    def run_comprehensive_health_check(self) -> Dict[str, Any]:
        """Run comprehensive health check across all pipeline components."""
        health_report = {
            'timestamp': datetime.utcnow().isoformat(),
            'overall_status': HealthStatus.HEALTHY.value,
            'components': {}
        }
        
        # Check Kafka Connect cluster
        cluster_health = self.check_kafka_connect_cluster()
        health_report['components']['kafka_connect_cluster'] = cluster_health
        
        # Check medallion connectors
        medallion_health = self.kafka_connect_checker.check_all_medallion_connectors()
        health_report['components']['medallion_connectors'] = medallion_health
        
        # Determine overall status
        all_results = [cluster_health]
        for layer_results in medallion_health.values():
            all_results.extend(layer_results)
        
        unhealthy_count = sum(1 for result in all_results if result.status == HealthStatus.UNHEALTHY)
        degraded_count = sum(1 for result in all_results if result.status == HealthStatus.DEGRADED)
        
        if unhealthy_count > 0:
            health_report['overall_status'] = HealthStatus.UNHEALTHY.value
        elif degraded_count > 0:
            health_report['overall_status'] = HealthStatus.DEGRADED.value
        
        # Add summary statistics
        health_report['summary'] = {
            'total_components': len(all_results),
            'healthy_count': sum(1 for result in all_results if result.status == HealthStatus.HEALTHY),
            'degraded_count': degraded_count,
            'unhealthy_count': unhealthy_count
        }
        
        # Log overall health status
        self.logger.info(
            f"Pipeline health check completed: {health_report['overall_status']}",
            component="pipeline_health_checker",
            operation="comprehensive_health_check",
            metadata=health_report['summary']
        )
        
        return health_report
    
    def monitor_continuously(self, interval_seconds: int = 60, max_iterations: Optional[int] = None):
        """Continuously monitor pipeline health."""
        iteration = 0
        
        while max_iterations is None or iteration < max_iterations:
            try:
                health_report = self.run_comprehensive_health_check()
                
                # Log health status
                if health_report['overall_status'] != HealthStatus.HEALTHY.value:
                    self.logger.warning(
                        f"Pipeline health degraded: {health_report['overall_status']}",
                        component="continuous_monitor",
                        operation="health_monitoring",
                        metadata=health_report['summary']
                    )
                
                time.sleep(interval_seconds)
                iteration += 1
                
            except KeyboardInterrupt:
                self.logger.info(
                    "Health monitoring stopped by user",
                    component="continuous_monitor",
                    operation="health_monitoring"
                )
                break
            except Exception as e:
                self.logger.error(
                    f"Error in continuous health monitoring: {str(e)}",
                    component="continuous_monitor",
                    operation="health_monitoring",
                    error_details={'error': str(e)}
                )
                time.sleep(interval_seconds)
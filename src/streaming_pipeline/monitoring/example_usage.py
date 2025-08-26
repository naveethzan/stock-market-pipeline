"""
Example usage of simplified monitoring components for medallion architecture.

Demonstrates:
- Simple layer-aware logging
- Basic health checks
- Essential data lineage tracking (Bronze → Silver → Gold)
- Simple metrics collection
"""

import time
import uuid
from typing import Dict, Any

from .simple_logger import PipelineLogger, MedallionLayer
from .health_checks import PipelineHealthChecker
from .simple_lineage import get_lineage_tracker
from .simple_metrics import get_metrics_collector


class SimpleMedallionMonitor:
    """Simplified monitoring for medallion architecture pipeline."""
    
    def __init__(self, kafka_connect_url: str = "http://localhost:8083"):
        self.logger = PipelineLogger("medallion_pipeline")
        self.health_checker = PipelineHealthChecker(kafka_connect_url, self.logger)
        self.lineage_tracker = get_lineage_tracker()
        self.metrics_collector = get_metrics_collector()
    
    def process_bronze_layer(self, data: Dict[str, Any]) -> str:
        """Process data in Bronze layer with simplified monitoring."""
        with self.logger.layer_context(
            layer=MedallionLayer.BRONZE,
            component="alpha_vantage_producer",
            operation="ingest_raw_data"
        ) as correlation_id:
            
            try:
                # Simulate processing
                record_count = len(data.get("records", []))
                time.sleep(0.1)  # Simulate processing time
                
                # Record simple metrics
                self.metrics_collector.record_layer_processing(
                    layer=MedallionLayer.BRONZE,
                    record_count=record_count,
                    success=True,
                    component="alpha_vantage_producer"
                )
                
                # Track medallion flow lineage
                self.lineage_tracker.track_medallion_flow(
                    correlation_id=correlation_id,
                    source_layer=MedallionLayer.INGESTION,
                    target_layer=MedallionLayer.BRONZE,
                    transformation="raw_data_ingestion",
                    component="alpha_vantage_producer",
                    record_count=record_count
                )
                
                return correlation_id
                
            except Exception as e:
                self.metrics_collector.record_layer_processing(
                    layer=MedallionLayer.BRONZE,
                    record_count=0,
                    success=False,
                    component="alpha_vantage_producer"
                )
                raise
    
    def process_silver_layer(self, correlation_id: str, data: Dict[str, Any]) -> str:
        """Process data in Silver layer with simplified monitoring."""
        with self.logger.layer_context(
            layer=MedallionLayer.SILVER,
            component="spark_processor",
            operation="transform_data",
            correlation_id=correlation_id
        ) as new_correlation_id:
            
            try:
                # Simulate processing
                record_count = len(data.get("records", []))
                time.sleep(0.2)  # Simulate processing time
                
                # Record simple metrics
                self.metrics_collector.record_layer_processing(
                    layer=MedallionLayer.SILVER,
                    record_count=record_count,
                    success=True,
                    component="spark_processor"
                )
                
                # Track medallion flow
                self.lineage_tracker.track_medallion_flow(
                    correlation_id=new_correlation_id,
                    source_layer=MedallionLayer.BRONZE,
                    target_layer=MedallionLayer.SILVER,
                    transformation="data_cleansing_and_enrichment",
                    component="spark_processor",
                    record_count=record_count
                )
                
                return new_correlation_id
                
            except Exception as e:
                self.metrics_collector.record_layer_processing(
                    layer=MedallionLayer.SILVER,
                    record_count=0,
                    success=False,
                    component="spark_processor"
                )
                raise
    
    def process_gold_layer(self, correlation_id: str, data: Dict[str, Any]) -> str:
        """Process data in Gold layer with simplified monitoring."""
        with self.logger.layer_context(
            layer=MedallionLayer.GOLD,
            component="snowflake_loader",
            operation="load_dimensional_data",
            correlation_id=correlation_id
        ) as new_correlation_id:
            
            try:
                # Simulate processing
                record_count = len(data.get("records", []))
                time.sleep(0.15)  # Simulate processing time
                
                # Record simple metrics
                self.metrics_collector.record_layer_processing(
                    layer=MedallionLayer.GOLD,
                    record_count=record_count,
                    success=True,
                    component="snowflake_loader"
                )
                
                # Track medallion flow
                self.lineage_tracker.track_medallion_flow(
                    correlation_id=new_correlation_id,
                    source_layer=MedallionLayer.SILVER,
                    target_layer=MedallionLayer.GOLD,
                    transformation="dimensional_modeling",
                    component="snowflake_loader",
                    record_count=record_count
                )
                
                return new_correlation_id
                
            except Exception as e:
                self.metrics_collector.record_layer_processing(
                    layer=MedallionLayer.GOLD,
                    record_count=0,
                    success=False,
                    component="snowflake_loader"
                )
                raise
    
    def run_health_checks(self) -> Dict[str, Any]:
        """Run comprehensive health checks."""
        self.logger.info(
            "Starting comprehensive health checks",
            component="pipeline_monitor",
            operation="health_check"
        )
        
        health_report = self.health_checker.run_comprehensive_health_check()
        
        # Log health status
        if health_report['overall_status'] != 'healthy':
            self.logger.warning(
                f"Pipeline health issues detected: {health_report['overall_status']}",
                component="pipeline_monitor",
                operation="health_check",
                metadata=health_report['summary']
            )
        
        return health_report
    
    def get_monitoring_dashboard(self) -> Dict[str, Any]:
        """Get simplified monitoring dashboard data."""
        return {
            'timestamp': time.time(),
            'health_status': self.run_health_checks(),
            'metrics_summary': self.metrics_collector.get_pipeline_summary(),
            'lineage_summary': self.lineage_tracker.get_medallion_summary()
        }
    
    def cleanup(self):
        """Cleanup monitoring resources."""
        # Simple cleanup - clear old data
        self.lineage_tracker.clear_old_flows()
        self.logger.info(
            "Monitoring cleanup completed",
            component="pipeline_monitor",
            operation="cleanup"
        )


def demonstrate_medallion_monitoring():
    """Demonstrate complete medallion architecture monitoring."""
    print("Starting Medallion Architecture Monitoring Demo")
    
    # Initialize monitor
    monitor = MedallionPipelineMonitor()
    
    try:
        # Simulate data processing through all layers
        sample_data = {
            "records": [
                {"symbol": "AAPL", "price": 150.25, "volume": 1000000},
                {"symbol": "GOOGL", "price": 2800.50, "volume": 500000},
                {"symbol": "MSFT", "price": 300.75, "volume": 750000}
            ]
        }
        
        print("\n1. Processing Bronze Layer...")
        bronze_correlation_id = monitor.process_bronze_layer(sample_data)
        
        print("\n2. Processing Silver Layer...")
        silver_correlation_id = monitor.process_silver_layer(bronze_correlation_id, sample_data)
        
        print("\n3. Processing Gold Layer...")
        gold_correlation_id = monitor.process_gold_layer(silver_correlation_id, sample_data)
        
        print("\n4. Running Health Checks...")
        health_report = monitor.run_health_checks()
        print(f"Overall Health Status: {health_report['overall_status']}")
        
        print("\n5. Getting Monitoring Dashboard...")
        dashboard = monitor.get_monitoring_dashboard()
        print(f"Total Lineage Events: {dashboard['lineage_summary']['total_events']}")
        
        print("\n6. Exporting Metrics...")
        prometheus_metrics = monitor.metrics_collector.export_metrics("prometheus")
        print(f"Prometheus metrics exported ({len(prometheus_metrics)} characters)")
        
        print("\nDemo completed successfully!")
        
    except Exception as e:
        print(f"Demo failed: {str(e)}")
        
    finally:
        monitor.cleanup()


if __name__ == "__main__":
    demonstrate_medallion_monitoring()
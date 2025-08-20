"""
Example usage of monitoring components for medallion architecture.

Demonstrates:
- Structured logging with layer tracking
- Health checks for Kafka Connect connectors
- Data lineage tracking across Bronze → Silver → Gold
- Metrics collection and monitoring
"""

import time
import uuid
from typing import Dict, Any

from .logger import PipelineLogger, MedallionLayer
from .health_checks import PipelineHealthChecker
from .lineage import DataLineageTracker, DataAsset
from .metrics import MetricsCollector


class MedallionPipelineMonitor:
    """Comprehensive monitoring for medallion architecture pipeline."""
    
    def __init__(self, kafka_connect_url: str = "http://localhost:8083"):
        self.logger = PipelineLogger("medallion_pipeline")
        self.health_checker = PipelineHealthChecker(kafka_connect_url, self.logger)
        self.lineage_tracker = DataLineageTracker(self.logger)
        self.metrics_collector = MetricsCollector(self.logger)
        
        # Start system monitoring
        self.metrics_collector.start_system_monitoring(interval_seconds=30)
    
    def process_bronze_layer(self, data: Dict[str, Any]) -> str:
        """Process data in Bronze layer with full monitoring."""
        with self.logger.layer_context(
            layer=MedallionLayer.BRONZE,
            component="alpha_vantage_producer",
            operation="ingest_raw_data",
            metadata={"record_count": len(data.get("records", []))}
        ) as correlation_id:
            
            # Start processing timer
            timer_id = self.metrics_collector.start_timer("bronze_processing")
            
            try:
                # Simulate processing
                record_count = len(data.get("records", []))
                time.sleep(0.1)  # Simulate processing time
                
                # Record processing metrics
                processing_time = self.metrics_collector.end_timer(timer_id)
                self.metrics_collector.record_medallion_processing(
                    layer=MedallionLayer.BRONZE,
                    record_count=record_count,
                    processing_time_ms=processing_time,
                    quality_score=0.98
                )
                
                # Track data lineage
                self.lineage_tracker.track_data_flow(
                    correlation_id=correlation_id,
                    source_asset_ids=["alpha_vantage_api"],
                    target_asset_ids=["bronze_stock_quotes"],
                    transformation="raw_data_ingestion",
                    component="alpha_vantage_producer",
                    operation="ingest_raw_data",
                    record_count=record_count,
                    quality_metrics={"completeness": 0.98}
                )
                
                return correlation_id
                
            except Exception as e:
                self.metrics_collector.increment_counter("bronze_errors")
                raise
    
    def process_silver_layer(self, correlation_id: str, data: Dict[str, Any]) -> str:
        """Process data in Silver layer with full monitoring."""
        with self.logger.layer_context(
            layer=MedallionLayer.SILVER,
            component="spark_processor",
            operation="transform_data",
            correlation_id=correlation_id,
            metadata={"record_count": len(data.get("records", []))}
        ) as new_correlation_id:
            
            timer_id = self.metrics_collector.start_timer("silver_processing")
            
            try:
                # Simulate processing
                record_count = len(data.get("records", []))
                time.sleep(0.2)  # Simulate processing time
                
                # Record processing metrics
                processing_time = self.metrics_collector.end_timer(timer_id)
                self.metrics_collector.record_medallion_processing(
                    layer=MedallionLayer.SILVER,
                    record_count=record_count,
                    processing_time_ms=processing_time,
                    quality_score=0.95
                )
                
                # Track medallion flow
                self.lineage_tracker.track_medallion_flow(
                    correlation_id=new_correlation_id,
                    source_layer=MedallionLayer.BRONZE,
                    target_layer=MedallionLayer.SILVER,
                    transformation="data_cleansing_and_enrichment",
                    component="spark_processor",
                    record_count=record_count,
                    quality_metrics={"validity": 0.95, "consistency": 0.97}
                )
                
                return new_correlation_id
                
            except Exception as e:
                self.metrics_collector.increment_counter("silver_errors")
                raise
    
    def process_gold_layer(self, correlation_id: str, data: Dict[str, Any]) -> str:
        """Process data in Gold layer with full monitoring."""
        with self.logger.layer_context(
            layer=MedallionLayer.GOLD,
            component="snowflake_loader",
            operation="load_dimensional_data",
            correlation_id=correlation_id,
            metadata={"record_count": len(data.get("records", []))}
        ) as new_correlation_id:
            
            timer_id = self.metrics_collector.start_timer("gold_processing")
            
            try:
                # Simulate processing
                record_count = len(data.get("records", []))
                time.sleep(0.15)  # Simulate processing time
                
                # Record processing metrics
                processing_time = self.metrics_collector.end_timer(timer_id)
                self.metrics_collector.record_medallion_processing(
                    layer=MedallionLayer.GOLD,
                    record_count=record_count,
                    processing_time_ms=processing_time,
                    quality_score=0.99
                )
                
                # Track medallion flow
                self.lineage_tracker.track_medallion_flow(
                    correlation_id=new_correlation_id,
                    source_layer=MedallionLayer.SILVER,
                    target_layer=MedallionLayer.GOLD,
                    transformation="dimensional_modeling",
                    component="snowflake_loader",
                    record_count=record_count,
                    quality_metrics={"accuracy": 0.99, "completeness": 1.0}
                )
                
                return new_correlation_id
                
            except Exception as e:
                self.metrics_collector.increment_counter("gold_errors")
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
        """Get comprehensive monitoring dashboard data."""
        return {
            'timestamp': time.time(),
            'health_status': self.run_health_checks(),
            'metrics_summary': self.metrics_collector.get_medallion_metrics_summary(),
            'lineage_summary': self.lineage_tracker.get_medallion_lineage_summary(),
            'system_metrics': {
                'cpu_percent': self.metrics_collector._gauges.get('system_cpu_percent', 0),
                'memory_percent': self.metrics_collector._gauges.get('system_memory_percent', 0),
                'disk_percent': self.metrics_collector._gauges.get('system_disk_percent', 0)
            }
        }
    
    def cleanup(self):
        """Cleanup monitoring resources."""
        self.metrics_collector.stop_system_monitoring()
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
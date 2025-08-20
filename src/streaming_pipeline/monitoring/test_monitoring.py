"""
Comprehensive tests for monitoring and testing components.

Tests cover:
- Structured logging with layer tracking
- Health checks for Kafka Connect connectors
- Data lineage tracking across Bronze → Silver → Gold
- Metrics collection and monitoring
"""

import pytest
import json
import time
import uuid
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from .logger import PipelineLogger, LayerTracker, MedallionLayer, LogContext
from .health_checks import KafkaConnectHealthChecker, PipelineHealthChecker, HealthStatus
from .lineage import DataLineageTracker, DataAsset, LineageEvent
from .metrics import MetricsCollector, MetricType


class TestPipelineLogger:
    """Test structured logging with layer tracking."""
    
    def test_layer_tracker_initialization(self):
        """Test LayerTracker initialization."""
        tracker = LayerTracker()
        assert len(tracker.get_active_contexts()) == 0
        assert len(tracker.get_layer_transitions()) == 0
    
    def test_start_layer_processing(self):
        """Test starting layer processing tracking."""
        tracker = LayerTracker()
        
        correlation_id = tracker.start_layer_processing(
            layer=MedallionLayer.BRONZE,
            component="test_component",
            operation="test_operation",
            metadata={"test": "data"}
        )
        
        assert correlation_id is not None
        assert len(tracker.get_active_contexts()) == 1
        assert len(tracker.get_layer_transitions()) == 1
        
        context = tracker.get_active_contexts()[correlation_id]
        assert context.layer == MedallionLayer.BRONZE
        assert context.component == "test_component"
        assert context.operation == "test_operation"
    
    def test_end_layer_processing(self):
        """Test ending layer processing tracking."""
        tracker = LayerTracker()
        
        correlation_id = tracker.start_layer_processing(
            layer=MedallionLayer.SILVER,
            component="test_component",
            operation="test_operation"
        )
        
        tracker.end_layer_processing(correlation_id, success=True)
        
        assert len(tracker.get_active_contexts()) == 0
        assert len(tracker.get_layer_transitions()) == 2  # start + end
    
    def test_pipeline_logger_layer_context(self):
        """Test pipeline logger layer context manager."""
        logger = PipelineLogger("test_logger")
        
        with logger.layer_context(
            layer=MedallionLayer.GOLD,
            component="test_component",
            operation="test_operation",
            metadata={"records": 100}
        ) as correlation_id:
            assert correlation_id is not None
            # Context should be active during processing
            assert len(logger.layer_tracker.get_active_contexts()) == 1
        
        # Context should be cleaned up after processing
        assert len(logger.layer_tracker.get_active_contexts()) == 0
    
    def test_pipeline_logger_error_handling(self):
        """Test pipeline logger error handling in context."""
        logger = PipelineLogger("test_logger")
        
        with pytest.raises(ValueError):
            with logger.layer_context(
                layer=MedallionLayer.BRONZE,
                component="test_component",
                operation="test_operation"
            ) as correlation_id:
                raise ValueError("Test error")
        
        # Context should be cleaned up even after error
        assert len(logger.layer_tracker.get_active_contexts()) == 0
        transitions = logger.layer_tracker.get_layer_transitions()
        assert len(transitions) == 2  # start + end
        assert transitions[-1]['success'] == False
    
    def test_log_data_flow(self):
        """Test data flow logging between layers."""
        logger = PipelineLogger("test_logger")
        
        logger.log_data_flow(
            source_layer=MedallionLayer.BRONZE,
            target_layer=MedallionLayer.SILVER,
            record_count=500,
            correlation_id="test-correlation-id",
            component="test_component",
            metadata={"processing_time": 1.5}
        )
        
        # Should not raise any exceptions
        assert True


class TestKafkaConnectHealthChecker:
    """Test Kafka Connect health checking."""
    
    @patch('requests.get')
    def test_check_connector_health_success(self, mock_get):
        """Test successful connector health check."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'connector': {'state': 'RUNNING'},
            'tasks': [{'state': 'RUNNING'}, {'state': 'RUNNING'}]
        }
        mock_get.return_value = mock_response
        
        checker = KafkaConnectHealthChecker()
        result = checker.check_connector_health("test-connector")
        
        assert result.status == HealthStatus.HEALTHY
        assert result.component == "kafka-connect-test-connector"
        assert "running normally" in result.message.lower()
        assert result.response_time_ms is not None
    
    @patch('requests.get')
    def test_check_connector_health_degraded(self, mock_get):
        """Test degraded connector health check."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'connector': {'state': 'RUNNING'},
            'tasks': [{'state': 'RUNNING'}, {'state': 'FAILED'}]
        }
        mock_get.return_value = mock_response
        
        checker = KafkaConnectHealthChecker()
        result = checker.check_connector_health("test-connector")
        
        assert result.status == HealthStatus.DEGRADED
        assert "partially running" in result.message.lower()
    
    @patch('requests.get')
    def test_check_connector_health_not_found(self, mock_get):
        """Test connector not found scenario."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        checker = KafkaConnectHealthChecker()
        result = checker.check_connector_health("missing-connector")
        
        assert result.status == HealthStatus.UNHEALTHY
        assert "not found" in result.message.lower()
    
    @patch('requests.get')
    def test_check_layer_health(self, mock_get):
        """Test checking health of medallion layer connectors."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'connector': {'state': 'RUNNING'},
            'tasks': [{'state': 'RUNNING'}]
        }
        mock_get.return_value = mock_response
        
        checker = KafkaConnectHealthChecker()
        results = checker.check_layer_health(MedallionLayer.BRONZE)
        
        assert len(results) > 0
        assert all(result.status == HealthStatus.HEALTHY for result in results)
    
    @patch('requests.get')
    def test_check_all_medallion_connectors(self, mock_get):
        """Test checking all medallion connectors."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'connector': {'state': 'RUNNING'},
            'tasks': [{'state': 'RUNNING'}]
        }
        mock_get.return_value = mock_response
        
        checker = KafkaConnectHealthChecker()
        results = checker.check_all_medallion_connectors()
        
        assert 'bronze' in results
        assert 'silver' in results
        assert 'gold' in results


class TestPipelineHealthChecker:
    """Test comprehensive pipeline health checking."""
    
    @patch('requests.get')
    def test_check_kafka_connect_cluster(self, mock_get):
        """Test Kafka Connect cluster health check."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'version': '2.8.0',
            'commit': 'test-commit'
        }
        mock_get.return_value = mock_response
        
        checker = PipelineHealthChecker()
        result = checker.check_kafka_connect_cluster()
        
        assert result.status == HealthStatus.HEALTHY
        assert result.component == "kafka-connect-cluster"
        assert result.response_time_ms is not None
    
    @patch('requests.get')
    def test_run_comprehensive_health_check(self, mock_get):
        """Test comprehensive health check."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'connector': {'state': 'RUNNING'},
            'tasks': [{'state': 'RUNNING'}]
        }
        mock_get.return_value = mock_response
        
        checker = PipelineHealthChecker()
        report = checker.run_comprehensive_health_check()
        
        assert 'timestamp' in report
        assert 'overall_status' in report
        assert 'components' in report
        assert 'summary' in report
        assert report['overall_status'] in [status.value for status in HealthStatus]


class TestDataLineageTracker:
    """Test data lineage tracking."""
    
    def test_initialization(self):
        """Test lineage tracker initialization."""
        tracker = DataLineageTracker()
        
        # Should have pre-registered medallion assets
        assert len(tracker._asset_registry) > 0
        
        # Should have assets for each layer
        bronze_assets = [asset for asset in tracker._asset_registry.values() 
                        if asset.layer == MedallionLayer.BRONZE]
        silver_assets = [asset for asset in tracker._asset_registry.values() 
                        if asset.layer == MedallionLayer.SILVER]
        gold_assets = [asset for asset in tracker._asset_registry.values() 
                      if asset.layer == MedallionLayer.GOLD]
        
        assert len(bronze_assets) > 0
        assert len(silver_assets) > 0
        assert len(gold_assets) > 0
    
    def test_register_asset(self):
        """Test asset registration."""
        tracker = DataLineageTracker()
        
        asset = DataAsset(
            asset_id="test_asset",
            layer=MedallionLayer.BRONZE,
            asset_type="topic",
            name="test-topic",
            location="kafka://test-topic"
        )
        
        tracker.register_asset(asset)
        assert "test_asset" in tracker._asset_registry
        assert tracker._asset_registry["test_asset"] == asset
    
    def test_track_data_flow(self):
        """Test data flow tracking."""
        tracker = DataLineageTracker()
        
        # Get some existing assets
        bronze_assets = [asset_id for asset_id, asset in tracker._asset_registry.items() 
                        if asset.layer == MedallionLayer.BRONZE]
        silver_assets = [asset_id for asset_id, asset in tracker._asset_registry.items() 
                        if asset.layer == MedallionLayer.SILVER]
        
        correlation_id = str(uuid.uuid4())
        event_id = tracker.track_data_flow(
            correlation_id=correlation_id,
            source_asset_ids=bronze_assets[:1],
            target_asset_ids=silver_assets[:1],
            transformation="bronze_to_silver_processing",
            component="spark_processor",
            operation="data_transformation",
            record_count=1000,
            quality_metrics={"completeness": 0.95}
        )
        
        assert event_id is not None
        assert len(tracker._lineage_events) == 1
        
        event = tracker._lineage_events[0]
        assert event.correlation_id == correlation_id
        assert event.record_count == 1000
        assert event.quality_metrics["completeness"] == 0.95
    
    def test_track_medallion_flow(self):
        """Test medallion architecture flow tracking."""
        tracker = DataLineageTracker()
        
        correlation_id = str(uuid.uuid4())
        event_id = tracker.track_medallion_flow(
            correlation_id=correlation_id,
            source_layer=MedallionLayer.BRONZE,
            target_layer=MedallionLayer.SILVER,
            transformation="data_processing",
            component="spark_processor",
            record_count=500
        )
        
        assert event_id is not None
        assert len(tracker._lineage_events) == 1
        
        event = tracker._lineage_events[0]
        assert event.correlation_id == correlation_id
        assert event.record_count == 500
    
    def test_get_lineage_for_asset(self):
        """Test getting lineage for specific asset."""
        tracker = DataLineageTracker()
        
        # Track some flows
        bronze_assets = [asset_id for asset_id, asset in tracker._asset_registry.items() 
                        if asset.layer == MedallionLayer.BRONZE]
        silver_assets = [asset_id for asset_id, asset in tracker._asset_registry.items() 
                        if asset.layer == MedallionLayer.SILVER]
        
        tracker.track_data_flow(
            correlation_id=str(uuid.uuid4()),
            source_asset_ids=bronze_assets[:1],
            target_asset_ids=silver_assets[:1],
            transformation="test_transformation",
            component="test_component",
            operation="test_operation"
        )
        
        # Get lineage for silver asset (should have upstream)
        lineage = tracker.get_lineage_for_asset(silver_assets[0])
        assert len(lineage['upstream']) == 1
        assert len(lineage['downstream']) == 0
        
        # Get lineage for bronze asset (should have downstream)
        lineage = tracker.get_lineage_for_asset(bronze_assets[0])
        assert len(lineage['upstream']) == 0
        assert len(lineage['downstream']) == 1
    
    def test_get_medallion_lineage_summary(self):
        """Test getting medallion lineage summary."""
        tracker = DataLineageTracker()
        
        # Track some flows
        tracker.track_medallion_flow(
            correlation_id=str(uuid.uuid4()),
            source_layer=MedallionLayer.BRONZE,
            target_layer=MedallionLayer.SILVER,
            transformation="bronze_to_silver",
            component="spark_processor"
        )
        
        summary = tracker.get_medallion_lineage_summary()
        
        assert 'timestamp' in summary
        assert 'total_events' in summary
        assert 'layer_flows' in summary
        assert 'assets_by_layer' in summary
        assert summary['total_events'] == 1
    
    def test_validate_lineage_integrity(self):
        """Test lineage integrity validation."""
        tracker = DataLineageTracker()
        
        # Track some flows to create valid lineage
        tracker.track_medallion_flow(
            correlation_id=str(uuid.uuid4()),
            source_layer=MedallionLayer.BRONZE,
            target_layer=MedallionLayer.SILVER,
            transformation="bronze_to_silver",
            component="spark_processor"
        )
        
        validation = tracker.validate_lineage_integrity()
        
        assert 'timestamp' in validation
        assert 'is_valid' in validation
        assert 'issues' in validation
        assert 'statistics' in validation


class TestMetricsCollector:
    """Test metrics collection and monitoring."""
    
    def test_initialization(self):
        """Test metrics collector initialization."""
        collector = MetricsCollector()
        
        # Should have initialized medallion metrics
        assert len(collector._counters) > 0
        assert len(collector._gauges) > 0
        
        # Check for layer-specific metrics
        for layer in MedallionLayer:
            assert f"{layer.value}_records_processed" in collector._counters
            assert f"{layer.value}_errors" in collector._counters
    
    def test_increment_counter(self):
        """Test counter increment."""
        collector = MetricsCollector()
        
        collector.increment_counter("test_counter", 5)
        assert collector._counters["test_counter"] == 5
        
        collector.increment_counter("test_counter", 3)
        assert collector._counters["test_counter"] == 8
        
        # Check metric history
        assert len(collector._metrics["test_counter"]) == 2
    
    def test_set_gauge(self):
        """Test gauge setting."""
        collector = MetricsCollector()
        
        collector.set_gauge("test_gauge", 42.5)
        assert collector._gauges["test_gauge"] == 42.5
        
        collector.set_gauge("test_gauge", 100.0)
        assert collector._gauges["test_gauge"] == 100.0
        
        # Check metric history
        assert len(collector._metrics["test_gauge"]) == 2
    
    def test_timer_functionality(self):
        """Test timer start/end functionality."""
        collector = MetricsCollector()
        
        timer_id = collector.start_timer("test_operation")
        assert timer_id in collector._active_timers
        
        time.sleep(0.1)  # Small delay
        duration_ms = collector.end_timer(timer_id)
        
        assert duration_ms > 0
        assert timer_id not in collector._active_timers
        assert len(collector._metrics["test_operation_duration_ms"]) == 1
    
    def test_record_medallion_processing(self):
        """Test recording medallion processing metrics."""
        collector = MetricsCollector()
        
        collector.record_medallion_processing(
            layer=MedallionLayer.SILVER,
            record_count=1000,
            processing_time_ms=500,
            error_count=5,
            quality_score=0.95
        )
        
        # Check that metrics were recorded
        assert collector._gauges[f"{MedallionLayer.SILVER.value}_throughput_records_per_sec"] == 2000  # 1000 records / 0.5 seconds
        assert collector._gauges[f"{MedallionLayer.SILVER.value}_processing_latency_ms"] == 500
        assert collector._counters[f"{MedallionLayer.SILVER.value}_records_processed"] >= 1000
        assert collector._counters[f"{MedallionLayer.SILVER.value}_errors"] >= 5
        assert collector._gauges[f"{MedallionLayer.SILVER.value}_quality_score"] == 0.95
    
    def test_record_data_quality_metrics(self):
        """Test recording data quality metrics."""
        collector = MetricsCollector()
        
        quality_checks = {
            "completeness": True,
            "validity": False,
            "consistency": True
        }
        
        collector.record_data_quality_metrics(
            layer=MedallionLayer.GOLD,
            total_records=1000,
            valid_records=950,
            invalid_records=50,
            quality_checks=quality_checks
        )
        
        # Check quality score calculation
        expected_score = 950 / 1000
        assert collector._gauges[f"{MedallionLayer.GOLD.value}_quality_score"] == expected_score
        
        # Check individual quality checks
        assert collector._gauges[f"{MedallionLayer.GOLD.value}_quality_check_completeness"] == 1.0
        assert collector._gauges[f"{MedallionLayer.GOLD.value}_quality_check_validity"] == 0.0
        assert collector._gauges[f"{MedallionLayer.GOLD.value}_quality_check_consistency"] == 1.0
    
    def test_get_metric_summary(self):
        """Test getting metric summary."""
        collector = MetricsCollector()
        
        # Record some values
        collector.set_gauge("test_metric", 10)
        collector.set_gauge("test_metric", 20)
        collector.set_gauge("test_metric", 15)
        
        summary = collector.get_metric_summary("test_metric")
        
        assert summary is not None
        assert summary.count == 3
        assert summary.min_value == 10
        assert summary.max_value == 20
        assert summary.avg_value == 15
        assert summary.latest_value == 15
    
    def test_get_medallion_metrics_summary(self):
        """Test getting comprehensive medallion metrics summary."""
        collector = MetricsCollector()
        
        # Record some processing metrics
        collector.record_medallion_processing(
            layer=MedallionLayer.BRONZE,
            record_count=500,
            processing_time_ms=250
        )
        
        summary = collector.get_medallion_metrics_summary()
        
        assert 'timestamp' in summary
        assert 'layers' in summary
        assert 'bronze' in summary['layers']
        assert summary['layers']['bronze']['total_records'] >= 500
    
    def test_export_prometheus_format(self):
        """Test exporting metrics in Prometheus format."""
        collector = MetricsCollector()
        
        collector.increment_counter("test_counter", 42)
        collector.set_gauge("test_gauge", 3.14)
        
        prometheus_output = collector.export_metrics("prometheus")
        
        assert "test_counter" in prometheus_output
        assert "test_gauge" in prometheus_output
        assert "42" in prometheus_output
        assert "3.14" in prometheus_output
    
    def test_export_json_format(self):
        """Test exporting metrics in JSON format."""
        collector = MetricsCollector()
        
        collector.increment_counter("test_counter", 42)
        collector.set_gauge("test_gauge", 3.14)
        
        json_output = collector.export_metrics("json")
        data = json.loads(json_output)
        
        assert 'timestamp' in data
        assert 'metrics' in data
        assert 'test_counter' in data['metrics']
        assert 'test_gauge' in data['metrics']


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
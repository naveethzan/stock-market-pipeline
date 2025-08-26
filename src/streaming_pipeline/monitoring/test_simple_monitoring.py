"""
Tests for simplified monitoring components.

Tests essential monitoring functionality:
- Simple logging with layer awareness
- Basic metrics collection
- Essential data lineage tracking
- Simple health checks
"""

import unittest
import uuid
from unittest.mock import patch

from .simple_logger import SimplePipelineLogger, MedallionLayer, create_logger
from .simple_metrics import SimpleMetricsCollector, get_metrics_collector
from .simple_lineage import SimpleDataLineageTracker, get_lineage_tracker
from .simple_health import HealthStatus


class TestSimplePipelineLogger(unittest.TestCase):
    """Test simplified pipeline logger."""
    
    def setUp(self):
        self.logger = SimplePipelineLogger("test_logger")
    
    def test_logger_initialization(self):
        """Test logger initialization."""
        self.assertEqual(self.logger.logger.name, "test_logger")
        self.assertTrue(len(self.logger.logger.handlers) > 0)
    
    def test_layer_context_logging(self):
        """Test layer context manager."""
        with self.logger.layer_context(
            layer=MedallionLayer.BRONZE,
            component="test_component",
            operation="test_operation"
        ) as correlation_id:
            self.assertIsNotNone(correlation_id)
            self.assertTrue(len(correlation_id) > 0)
    
    def test_formatted_logging(self):
        """Test formatted log messages."""
        correlation_id = str(uuid.uuid4())
        
        # Test info logging with context
        with patch.object(self.logger.logger, 'info') as mock_info:
            self.logger.info(
                "Test message",
                correlation_id=correlation_id,
                layer=MedallionLayer.SILVER,
                component="test_component"
            )
            mock_info.assert_called_once()
            args = mock_info.call_args[0]
            self.assertIn(correlation_id[:8], args[0])
            self.assertIn("[SILVER]", args[0])
            self.assertIn("[test_component]", args[0])


class TestSimpleMetricsCollector(unittest.TestCase):
    """Test simplified metrics collector."""
    
    def setUp(self):
        self.collector = SimpleMetricsCollector()
    
    def test_counter_operations(self):
        """Test counter increment operations."""
        self.collector.increment_counter("test_counter", 5)
        self.assertEqual(self.collector.get_counter("test_counter"), 5)
        
        self.collector.increment_counter("test_counter", 3)
        self.assertEqual(self.collector.get_counter("test_counter"), 8)
    
    def test_gauge_operations(self):
        """Test gauge set operations."""
        self.collector.set_gauge("test_gauge", 42.5)
        self.assertEqual(self.collector.get_gauge("test_gauge"), 42.5)
        
        self.collector.set_gauge("test_gauge", 100.0)
        self.assertEqual(self.collector.get_gauge("test_gauge"), 100.0)
    
    def test_layer_processing_metrics(self):
        """Test medallion layer processing metrics."""
        # Test successful processing
        self.collector.record_layer_processing(
            layer=MedallionLayer.BRONZE,
            record_count=100,
            success=True,
            component="test_processor"
        )
        
        self.assertEqual(self.collector.get_counter("bronze_records_processed"), 100)
        self.assertEqual(self.collector.get_gauge("bronze_status"), 1)
        
        # Test failed processing
        self.collector.record_layer_processing(
            layer=MedallionLayer.BRONZE,
            record_count=0,
            success=False,
            component="test_processor"
        )
        
        self.assertEqual(self.collector.get_counter("bronze_errors"), 1)
        self.assertEqual(self.collector.get_gauge("bronze_status"), -1)
    
    def test_pipeline_summary(self):
        """Test pipeline summary generation."""
        # Record some metrics
        self.collector.record_layer_processing(
            MedallionLayer.BRONZE, 50, True, "producer"
        )
        self.collector.record_layer_processing(
            MedallionLayer.SILVER, 45, True, "processor"
        )
        
        summary = self.collector.get_pipeline_summary()
        
        self.assertIn('timestamp', summary)
        self.assertIn('layers', summary)
        self.assertIn('bronze', summary['layers'])
        self.assertIn('silver', summary['layers'])
        
        bronze_data = summary['layers']['bronze']
        self.assertEqual(bronze_data['records_processed'], 50)
        self.assertEqual(bronze_data['status'], 1)


class TestSimpleDataLineageTracker(unittest.TestCase):
    """Test simplified data lineage tracker."""
    
    def setUp(self):
        self.tracker = SimpleDataLineageTracker()
    
    def test_medallion_flow_tracking(self):
        """Test medallion flow tracking."""
        correlation_id = str(uuid.uuid4())
        
        flow_id = self.tracker.track_medallion_flow(
            correlation_id=correlation_id,
            source_layer=MedallionLayer.BRONZE,
            target_layer=MedallionLayer.SILVER,
            transformation="data_cleansing",
            component="spark_processor",
            record_count=100
        )
        
        self.assertIsNotNone(flow_id)
        self.assertTrue(len(self.tracker._flows) > 0)
        
        # Check the flow was recorded
        flow = self.tracker._flows[-1]
        self.assertEqual(flow.correlation_id, correlation_id)
        self.assertEqual(flow.source_layer, MedallionLayer.BRONZE)
        self.assertEqual(flow.target_layer, MedallionLayer.SILVER)
        self.assertEqual(flow.record_count, 100)
    
    def test_medallion_summary(self):
        """Test medallion summary generation."""
        correlation_id = str(uuid.uuid4())
        
        # Track a few flows
        self.tracker.track_medallion_flow(
            correlation_id, MedallionLayer.BRONZE, MedallionLayer.SILVER,
            "cleansing", "processor", 100
        )
        self.tracker.track_medallion_flow(
            correlation_id, MedallionLayer.SILVER, MedallionLayer.GOLD,
            "modeling", "loader", 95
        )
        
        summary = self.tracker.get_medallion_summary()
        
        self.assertIn('total_flows', summary)
        self.assertIn('layer_transitions', summary)
        self.assertIn('recent_flows', summary)
        
        self.assertEqual(summary['total_flows'], 2)
        self.assertIn('bronze_to_silver', summary['layer_transitions'])
        self.assertIn('silver_to_gold', summary['layer_transitions'])
    
    def test_flows_for_correlation(self):
        """Test getting flows for specific correlation ID."""
        correlation_id = str(uuid.uuid4())
        
        self.tracker.track_medallion_flow(
            correlation_id, MedallionLayer.BRONZE, MedallionLayer.SILVER,
            "test_transform", "test_component", 50
        )
        
        flows = self.tracker.get_flows_for_correlation(correlation_id)
        self.assertEqual(len(flows), 1)
        self.assertEqual(flows[0].correlation_id, correlation_id)


class TestHealthStatus(unittest.TestCase):
    """Test health status functionality."""
    
    def setUp(self):
        self.health_status = HealthStatus()
        # Clear any existing metrics from previous tests
        self.health_status.metrics_collector.clear_metrics()
    
    def test_pipeline_status_management(self):
        """Test pipeline status setting and retrieval."""
        self.health_status.set_pipeline_status("running")
        
        report = self.health_status.get_health_report()
        self.assertEqual(report['pipeline_status'], "running")
        self.assertEqual(report['status'], "healthy")
    
    def test_error_detection(self):
        """Test error detection in health report."""
        # Record some errors
        metrics_collector = self.health_status.metrics_collector
        metrics_collector.record_layer_processing(
            MedallionLayer.BRONZE, 0, False, "test_component"
        )
        
        self.health_status.set_pipeline_status("running")
        report = self.health_status.get_health_report()
        
        self.assertEqual(report['status'], "error")
        self.assertTrue(len(report['issues']) > 0)


class TestGlobalInstances(unittest.TestCase):
    """Test global instance functions."""
    
    def test_global_metrics_collector(self):
        """Test global metrics collector access."""
        collector = get_metrics_collector()
        self.assertIsInstance(collector, SimpleMetricsCollector)
        
        # Should return the same instance
        collector2 = get_metrics_collector()
        self.assertIs(collector, collector2)
    
    def test_global_lineage_tracker(self):
        """Test global lineage tracker access."""
        tracker = get_lineage_tracker()
        self.assertIsInstance(tracker, SimpleDataLineageTracker)
        
        # Should return the same instance
        tracker2 = get_lineage_tracker()
        self.assertIs(tracker, tracker2)
    
    def test_logger_creation(self):
        """Test logger creation function."""
        logger = create_logger("test_logger")
        self.assertIsInstance(logger, SimplePipelineLogger)
        self.assertEqual(logger.logger.name, "test_logger")


if __name__ == '__main__':
    unittest.main()
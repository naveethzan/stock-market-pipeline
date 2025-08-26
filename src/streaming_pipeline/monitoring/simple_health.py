"""
Simple health check endpoint for basic pipeline monitoring.

Provides essential health monitoring without complex infrastructure:
- Basic HTTP health endpoint
- Pipeline status checking
- Simple component health verification
"""

import json
from datetime import datetime
from typing import Dict, Any, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

from .simple_metrics import get_metrics_collector
from .simple_lineage import get_lineage_tracker
from .simple_logger import create_logger


class HealthStatus:
    """Simple health status tracking."""
    
    def __init__(self):
        self.logger = create_logger("health_checker")
        self.metrics_collector = get_metrics_collector()
        self.lineage_tracker = get_lineage_tracker()
        self._pipeline_status = "starting"  # starting, running, stopped, error
    
    def set_pipeline_status(self, status: str):
        """Set pipeline status."""
        self._pipeline_status = status
        self.logger.info(f"Pipeline status changed to: {status}")
    
    def get_health_report(self) -> Dict[str, Any]:
        """Get basic health report."""
        metrics_summary = self.metrics_collector.get_pipeline_summary()
        lineage_summary = self.lineage_tracker.get_medallion_summary()
        
        # Simple health logic
        health_status = "healthy"
        issues = []
        
        # Check if pipeline is running
        if self._pipeline_status != "running":
            health_status = "warning"
            issues.append(f"Pipeline status: {self._pipeline_status}")
        
        # Check for errors in any layer
        for layer_name, layer_data in metrics_summary.get('layers', {}).items():
            if layer_data.get('errors', 0) > 0:
                health_status = "error"
                issues.append(f"{layer_name} layer has {layer_data['errors']} errors")
        
        return {
            'status': health_status,
            'timestamp': datetime.utcnow().isoformat(),
            'pipeline_status': self._pipeline_status,
            'issues': issues,
            'metrics': metrics_summary,
            'lineage': {
                'total_flows': lineage_summary.get('total_flows', 0),
                'layer_transitions': dict(lineage_summary.get('layer_transitions', {}))
            }
        }


class SimpleHealthHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks."""
    
    def __init__(self, *args, health_status: HealthStatus, **kwargs):
        self.health_status = health_status
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/health':
            self._handle_health()
        elif self.path == '/metrics':
            self._handle_metrics()
        elif self.path == '/lineage':
            self._handle_lineage()
        else:
            self._handle_not_found()
    
    def _handle_health(self):
        """Handle health check endpoint."""
        try:
            health_report = self.health_status.get_health_report()
            
            # Set HTTP status based on health
            if health_report['status'] == 'healthy':
                status_code = 200
            elif health_report['status'] == 'warning':
                status_code = 200  # Still responding
            else:
                status_code = 503  # Service unavailable
            
            self._send_json_response(health_report, status_code)
            
        except Exception as e:
            self._send_json_response({'error': str(e)}, 500)
    
    def _handle_metrics(self):
        """Handle metrics endpoint."""
        try:
            metrics = self.health_status.metrics_collector.get_pipeline_summary()
            self._send_json_response(metrics, 200)
        except Exception as e:
            self._send_json_response({'error': str(e)}, 500)
    
    def _handle_lineage(self):
        """Handle lineage endpoint."""
        try:
            lineage = self.health_status.lineage_tracker.get_medallion_summary()
            self._send_json_response(lineage, 200)
        except Exception as e:
            self._send_json_response({'error': str(e)}, 500)
    
    def _handle_not_found(self):
        """Handle 404 responses."""
        self._send_json_response({'error': 'Not found'}, 404)
    
    def _send_json_response(self, data: Dict[str, Any], status_code: int):
        """Send JSON response."""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode())
    
    def log_message(self, format, *args):
        """Override to suppress HTTP logs."""
        pass


class SimpleHealthServer:
    """Simple health check server."""
    
    def __init__(self, port: int = 8080):
        self.port = port
        self.health_status = HealthStatus()
        self.server = None
        self.server_thread = None
        self.logger = create_logger("health_server")
    
    def start(self):
        """Start the health server."""
        try:
            # Create handler with health_status
            def handler(*args, **kwargs):
                return SimpleHealthHandler(*args, health_status=self.health_status, **kwargs)
            
            self.server = HTTPServer(('', self.port), handler)
            self.server_thread = threading.Thread(target=self.server.serve_forever)
            self.server_thread.daemon = True
            self.server_thread.start()
            
            self.logger.info(f"Health server started on port {self.port}")
            
        except Exception as e:
            self.logger.error(f"Failed to start health server: {e}")
            raise
    
    def stop(self):
        """Stop the health server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.logger.info("Health server stopped")
    
    def set_pipeline_status(self, status: str):
        """Set pipeline status."""
        self.health_status.set_pipeline_status(status)


# Global health server instance
_health_server = None


def start_health_server(port: int = 8080) -> SimpleHealthServer:
    """Start global health server."""
    global _health_server
    if _health_server is None:
        _health_server = SimpleHealthServer(port)
        _health_server.start()
    return _health_server


def get_health_server() -> Optional[SimpleHealthServer]:
    """Get global health server."""
    return _health_server


def stop_health_server():
    """Stop global health server."""
    global _health_server
    if _health_server:
        _health_server.stop()
        _health_server = None
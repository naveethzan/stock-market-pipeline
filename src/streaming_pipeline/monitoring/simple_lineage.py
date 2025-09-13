"""
Simplified data lineage tracking for medallion architecture.

Focuses on essential data engineering concepts:
- Bronze → Silver → Gold flow tracking
- Basic asset registry
- Correlation ID tracking for debugging
- Core data governance capabilities
"""

import json
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from collections import defaultdict

from .simple_logger import MedallionLayer


@dataclass
class DataAsset:
    """Represents a data asset in the pipeline."""
    asset_id: str
    layer: MedallionLayer
    asset_type: str  # 'topic', 'table', 'file'
    name: str
    location: Optional[str] = None


@dataclass
class LineageFlow:
    """Represents a data flow between assets."""
    flow_id: str
    timestamp: str
    correlation_id: str
    source_layer: MedallionLayer
    target_layer: MedallionLayer
    transformation: str
    component: str
    record_count: Optional[int] = None


class SimpleDataLineageTracker:
    """Simplified data lineage tracker for medallion architecture."""
    
    def __init__(self):
        self._flows: List[LineageFlow] = []
        self._assets: Dict[str, DataAsset] = {}
        
        # Initialize medallion layer assets
        self._initialize_assets()
    
    def _initialize_assets(self):
        """Initialize basic medallion architecture assets."""
        assets = [
            # Bronze layer (raw data)
            DataAsset("bronze_topics", MedallionLayer.BRONZE, "topic", "stock-data-raw"),
            DataAsset("bronze_s3", MedallionLayer.BRONZE, "file", "s3://bronze/stock-data"),
            
            # Silver layer (processed data)
            DataAsset("silver_topics", MedallionLayer.SILVER, "topic", "stock-data-processed"),
            DataAsset("silver_s3", MedallionLayer.SILVER, "file", "s3://silver/stock-data"),
            
            # Gold layer (analytical data)
            DataAsset("gold_facts", MedallionLayer.GOLD, "table", "fact_stock_prices"),
            DataAsset("gold_dims", MedallionLayer.GOLD, "table", "dim_company")
        ]
        
        for asset in assets:
            self._assets[asset.asset_id] = asset
    
    def track_medallion_flow(
        self,
        correlation_id: str,
        source_layer: MedallionLayer,
        target_layer: MedallionLayer,
        transformation: str,
        component: str,
        record_count: Optional[int] = None
    ) -> str:
        """Track data flow between medallion layers (core data engineering concept)."""
        
        flow_id = f"flow_{int(time.time() * 1000)}_{correlation_id[:8]}"
        
        flow = LineageFlow(
            flow_id=flow_id,
            timestamp=datetime.utcnow().isoformat(),
            correlation_id=correlation_id,
            source_layer=source_layer,
            target_layer=target_layer,
            transformation=transformation,
            component=component,
            record_count=record_count
        )
        
        self._flows.append(flow)
        
        # Keep only last 100 flows for memory management
        if len(self._flows) > 100:
            self._flows.pop(0)
        
        return flow_id
    
    def get_medallion_summary(self) -> Dict[str, Any]:
        """Get summary of medallion layer flows."""
        summary = {
            'timestamp': datetime.utcnow().isoformat(),
            'total_flows': len(self._flows),
            'layer_transitions': defaultdict(int),
            'recent_flows': []
        }
        
        # Count transitions
        for flow in self._flows:
            transition = f"{flow.source_layer.value}_to_{flow.target_layer.value}"
            summary['layer_transitions'][transition] += 1
        
        # Recent flows (last 10)
        for flow in self._flows[-10:]:
            summary['recent_flows'].append({
                'flow_id': flow.flow_id,
                'correlation_id': flow.correlation_id,
                'source_layer': flow.source_layer.value,
                'target_layer': flow.target_layer.value,
                'transformation': flow.transformation,
                'record_count': flow.record_count,
                'timestamp': flow.timestamp
            })
        
        return summary
    
    def get_flows_for_correlation(self, correlation_id: str) -> List[LineageFlow]:
        """Get all flows for a specific correlation ID (debugging support)."""
        return [flow for flow in self._flows if flow.correlation_id == correlation_id]
    
    def export_simple_graph(self) -> Dict[str, Any]:
        """Export basic lineage graph for visualization."""
        nodes = []
        edges = []
        
        # Create nodes for assets
        for asset in self._assets.values():
            nodes.append({
                'id': asset.asset_id,
                'name': asset.name,
                'layer': asset.layer.value,
                'type': asset.asset_type
            })
        
        # Create edges for flows
        for flow in self._flows:
            edges.append({
                'source': f"{flow.source_layer.value}_layer",
                'target': f"{flow.target_layer.value}_layer",
                'transformation': flow.transformation,
                'record_count': flow.record_count
            })
        
        return {
            'nodes': nodes,
            'edges': edges,
            'flow_count': len(self._flows)
        }
    
    def clear_old_flows(self, keep_count: int = 50):
        """Clear old flows to manage memory."""
        if len(self._flows) > keep_count:
            self._flows = self._flows[-keep_count:]


# Global lineage tracker instance
lineage_tracker = SimpleDataLineageTracker()


def get_lineage_tracker() -> SimpleDataLineageTracker:
    """Get the global lineage tracker instance."""
    return lineage_tracker
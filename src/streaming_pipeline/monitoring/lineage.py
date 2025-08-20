"""
Data lineage tracking across Bronze → Silver → Gold medallion architecture.

Provides comprehensive data lineage capabilities including:
- Cross-layer data flow tracking
- Data transformation lineage
- Quality metrics tracking
- Audit trail maintenance
"""

import json
import time
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict

from .logger import PipelineLogger, MedallionLayer


@dataclass
class DataAsset:
    """Represents a data asset in the pipeline."""
    asset_id: str
    layer: MedallionLayer
    asset_type: str  # 'topic', 'table', 'file', 'stream'
    name: str
    schema_version: Optional[str] = None
    location: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class LineageEvent:
    """Represents a data lineage event."""
    event_id: str
    timestamp: str
    correlation_id: str
    source_assets: List[DataAsset]
    target_assets: List[DataAsset]
    transformation: str
    component: str
    operation: str
    record_count: Optional[int] = None
    quality_metrics: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


class DataLineageTracker:
    """Tracks data lineage across the medallion architecture."""
    
    def __init__(self, logger: Optional[PipelineLogger] = None):
        self.logger = logger or PipelineLogger(__name__)
        self._lineage_events: List[LineageEvent] = []
        self._asset_registry: Dict[str, DataAsset] = {}
        self._layer_flows: Dict[str, List[str]] = defaultdict(list)  # layer -> list of correlation_ids
        
        # Initialize medallion layer assets
        self._initialize_medallion_assets()
    
    def _initialize_medallion_assets(self):
        """Initialize known medallion architecture assets."""
        # Bronze layer assets (raw data)
        bronze_assets = [
            DataAsset(
                asset_id="bronze_stock_quotes",
                layer=MedallionLayer.BRONZE,
                asset_type="topic",
                name="stock-quotes-realtime",
                location="kafka://stock-quotes-realtime"
            ),
            DataAsset(
                asset_id="bronze_intraday_data",
                layer=MedallionLayer.BRONZE,
                asset_type="topic",
                name="stock-intraday-data",
                location="kafka://stock-intraday-data"
            ),
            DataAsset(
                asset_id="bronze_s3_raw",
                layer=MedallionLayer.BRONZE,
                asset_type="file",
                name="raw-stock-data",
                location="s3://data-lake/bronze/stock-data/"
            )
        ]
        
        # Silver layer assets (processed data)
        silver_assets = [
            DataAsset(
                asset_id="silver_processed_prices",
                layer=MedallionLayer.SILVER,
                asset_type="topic",
                name="processed-stock-prices",
                location="kafka://processed-stock-prices"
            ),
            DataAsset(
                asset_id="silver_processed_volume",
                layer=MedallionLayer.SILVER,
                asset_type="topic",
                name="processed-trading-volume",
                location="kafka://processed-trading-volume"
            ),
            DataAsset(
                asset_id="silver_s3_processed",
                layer=MedallionLayer.SILVER,
                asset_type="file",
                name="processed-stock-data",
                location="s3://data-lake/silver/stock-data/"
            )
        ]
        
        # Gold layer assets (analytical data)
        gold_assets = [
            DataAsset(
                asset_id="gold_fact_stock_prices",
                layer=MedallionLayer.GOLD,
                asset_type="table",
                name="fact_stock_prices",
                location="snowflake://warehouse/schema/fact_stock_prices"
            ),
            DataAsset(
                asset_id="gold_fact_trading_volume",
                layer=MedallionLayer.GOLD,
                asset_type="table",
                name="fact_trading_volume",
                location="snowflake://warehouse/schema/fact_trading_volume"
            ),
            DataAsset(
                asset_id="gold_dim_company",
                layer=MedallionLayer.GOLD,
                asset_type="table",
                name="dim_company",
                location="snowflake://warehouse/schema/dim_company"
            )
        ]
        
        # Register all assets
        for asset in bronze_assets + silver_assets + gold_assets:
            self._asset_registry[asset.asset_id] = asset
    
    def register_asset(self, asset: DataAsset):
        """Register a new data asset."""
        self._asset_registry[asset.asset_id] = asset
        
        self.logger.info(
            f"Registered data asset: {asset.name}",
            layer=asset.layer,
            component="lineage_tracker",
            operation="asset_registration",
            metadata={
                'asset_id': asset.asset_id,
                'asset_type': asset.asset_type,
                'location': asset.location
            }
        )
    
    def track_data_flow(
        self,
        correlation_id: str,
        source_asset_ids: List[str],
        target_asset_ids: List[str],
        transformation: str,
        component: str,
        operation: str,
        record_count: Optional[int] = None,
        quality_metrics: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Track a data flow event between assets."""
        
        # Resolve assets
        source_assets = []
        target_assets = []
        
        for asset_id in source_asset_ids:
            if asset_id in self._asset_registry:
                source_assets.append(self._asset_registry[asset_id])
            else:
                self.logger.warning(
                    f"Unknown source asset: {asset_id}",
                    component="lineage_tracker",
                    operation="track_data_flow"
                )
        
        for asset_id in target_asset_ids:
            if asset_id in self._asset_registry:
                target_assets.append(self._asset_registry[asset_id])
            else:
                self.logger.warning(
                    f"Unknown target asset: {asset_id}",
                    component="lineage_tracker",
                    operation="track_data_flow"
                )
        
        # Create lineage event
        event_id = f"lineage_{int(time.time() * 1000)}_{correlation_id}"
        lineage_event = LineageEvent(
            event_id=event_id,
            timestamp=datetime.utcnow().isoformat(),
            correlation_id=correlation_id,
            source_assets=source_assets,
            target_assets=target_assets,
            transformation=transformation,
            component=component,
            operation=operation,
            record_count=record_count,
            quality_metrics=quality_metrics,
            metadata=metadata
        )
        
        self._lineage_events.append(lineage_event)
        
        # Track layer flows
        source_layers = {asset.layer.value for asset in source_assets}
        target_layers = {asset.layer.value for asset in target_assets}
        
        for source_layer in source_layers:
            for target_layer in target_layers:
                flow_key = f"{source_layer}_to_{target_layer}"
                self._layer_flows[flow_key].append(correlation_id)
        
        # Log lineage event
        self.logger.info(
            f"Tracked data flow: {transformation}",
            correlation_id=correlation_id,
            component=component,
            operation=operation,
            metadata={
                'event_id': event_id,
                'source_assets': [asset.name for asset in source_assets],
                'target_assets': [asset.name for asset in target_assets],
                'record_count': record_count,
                'quality_metrics': quality_metrics
            }
        )
        
        return event_id
    
    def track_medallion_flow(
        self,
        correlation_id: str,
        source_layer: MedallionLayer,
        target_layer: MedallionLayer,
        transformation: str,
        component: str,
        record_count: Optional[int] = None,
        quality_metrics: Optional[Dict[str, Any]] = None
    ) -> str:
        """Track data flow between medallion layers."""
        
        # Get assets for each layer
        source_assets = [asset.asset_id for asset in self._asset_registry.values() 
                        if asset.layer == source_layer]
        target_assets = [asset.asset_id for asset in self._asset_registry.values() 
                        if asset.layer == target_layer]
        
        return self.track_data_flow(
            correlation_id=correlation_id,
            source_asset_ids=source_assets[:1],  # Use first asset as representative
            target_asset_ids=target_assets[:1],  # Use first asset as representative
            transformation=transformation,
            component=component,
            operation=f"{source_layer.value}_to_{target_layer.value}",
            record_count=record_count,
            quality_metrics=quality_metrics,
            metadata={
                'source_layer': source_layer.value,
                'target_layer': target_layer.value
            }
        )
    
    def get_lineage_for_asset(self, asset_id: str) -> Dict[str, List[LineageEvent]]:
        """Get lineage events for a specific asset."""
        upstream_events = []
        downstream_events = []
        
        for event in self._lineage_events:
            # Check if asset is a target (upstream lineage)
            if any(asset.asset_id == asset_id for asset in event.target_assets):
                upstream_events.append(event)
            
            # Check if asset is a source (downstream lineage)
            if any(asset.asset_id == asset_id for asset in event.source_assets):
                downstream_events.append(event)
        
        return {
            'upstream': upstream_events,
            'downstream': downstream_events
        }
    
    def get_medallion_lineage_summary(self) -> Dict[str, Any]:
        """Get summary of data lineage across medallion layers."""
        summary = {
            'timestamp': datetime.utcnow().isoformat(),
            'total_events': len(self._lineage_events),
            'layer_flows': {},
            'assets_by_layer': {},
            'recent_flows': []
        }
        
        # Count flows between layers
        for flow_key, correlation_ids in self._layer_flows.items():
            summary['layer_flows'][flow_key] = len(correlation_ids)
        
        # Count assets by layer
        for layer in MedallionLayer:
            assets = [asset for asset in self._asset_registry.values() if asset.layer == layer]
            summary['assets_by_layer'][layer.value] = len(assets)
        
        # Get recent flows (last 24 hours)
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        recent_events = [
            event for event in self._lineage_events
            if datetime.fromisoformat(event.timestamp.replace('Z', '+00:00')) > cutoff_time
        ]
        
        summary['recent_flows'] = [
            {
                'event_id': event.event_id,
                'timestamp': event.timestamp,
                'transformation': event.transformation,
                'source_layers': list({asset.layer.value for asset in event.source_assets}),
                'target_layers': list({asset.layer.value for asset in event.target_assets}),
                'record_count': event.record_count
            }
            for event in recent_events[-10:]  # Last 10 events
        ]
        
        return summary
    
    def export_lineage_graph(self) -> Dict[str, Any]:
        """Export lineage as a graph structure for visualization."""
        nodes = []
        edges = []
        
        # Create nodes for assets
        for asset in self._asset_registry.values():
            nodes.append({
                'id': asset.asset_id,
                'label': asset.name,
                'layer': asset.layer.value,
                'type': asset.asset_type,
                'location': asset.location
            })
        
        # Create edges for lineage events
        for event in self._lineage_events:
            for source_asset in event.source_assets:
                for target_asset in event.target_assets:
                    edges.append({
                        'source': source_asset.asset_id,
                        'target': target_asset.asset_id,
                        'transformation': event.transformation,
                        'component': event.component,
                        'timestamp': event.timestamp,
                        'record_count': event.record_count
                    })
        
        return {
            'nodes': nodes,
            'edges': edges,
            'metadata': {
                'generated_at': datetime.utcnow().isoformat(),
                'total_assets': len(nodes),
                'total_flows': len(edges)
            }
        }
    
    def validate_lineage_integrity(self) -> Dict[str, Any]:
        """Validate the integrity of tracked lineage."""
        validation_results = {
            'timestamp': datetime.utcnow().isoformat(),
            'is_valid': True,
            'issues': [],
            'statistics': {}
        }
        
        # Check for orphaned assets (no lineage events)
        assets_with_lineage = set()
        for event in self._lineage_events:
            for asset in event.source_assets + event.target_assets:
                assets_with_lineage.add(asset.asset_id)
        
        orphaned_assets = set(self._asset_registry.keys()) - assets_with_lineage
        if orphaned_assets:
            validation_results['issues'].append({
                'type': 'orphaned_assets',
                'count': len(orphaned_assets),
                'assets': list(orphaned_assets)
            })
            validation_results['is_valid'] = False
        
        # Check for missing medallion flows
        expected_flows = ['bronze_to_silver', 'silver_to_gold']
        missing_flows = [flow for flow in expected_flows if flow not in self._layer_flows]
        if missing_flows:
            validation_results['issues'].append({
                'type': 'missing_medallion_flows',
                'flows': missing_flows
            })
            validation_results['is_valid'] = False
        
        # Add statistics
        validation_results['statistics'] = {
            'total_assets': len(self._asset_registry),
            'total_events': len(self._lineage_events),
            'assets_with_lineage': len(assets_with_lineage),
            'layer_flows': dict(self._layer_flows)
        }
        
        return validation_results
    
    def clear_lineage_history(self, older_than_hours: int = 24):
        """Clear old lineage events to manage memory usage."""
        cutoff_time = datetime.utcnow() - timedelta(hours=older_than_hours)
        
        initial_count = len(self._lineage_events)
        self._lineage_events = [
            event for event in self._lineage_events
            if datetime.fromisoformat(event.timestamp.replace('Z', '+00:00')) > cutoff_time
        ]
        
        cleared_count = initial_count - len(self._lineage_events)
        
        self.logger.info(
            f"Cleared {cleared_count} old lineage events",
            component="lineage_tracker",
            operation="cleanup",
            metadata={
                'cleared_count': cleared_count,
                'remaining_count': len(self._lineage_events),
                'cutoff_hours': older_than_hours
            }
        )
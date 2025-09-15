"""
Enhanced schema management for Avro schemas.
Centralized schema handling with validation and evolution.
"""

from typing import Dict, Any, Optional, List
import json
import avro.schema
import avro.io
import io

from stock_market_pipeline.core.exceptions import SchemaRegistryError, AvroSerializationError
from stock_market_pipeline.utils import PipelineLogger
from stock_market_pipeline.storage.schemas.avro_schemas import get_all_schemas, SCHEMA_REGISTRY_SUBJECTS


class SchemaManager:
    """
    Manages Avro schemas and Schema Registry integration.
    
    Provides centralized schema management including compilation, validation,
    serialization, and deserialization of Avro data with comprehensive
    error handling and registry integration capabilities.
    """
    
    def __init__(self, schema_registry_url: str):
        self.schema_registry_url = schema_registry_url
        self.logger = PipelineLogger(__name__)
        self.schemas = get_all_schemas()
        self.registry_subjects = SCHEMA_REGISTRY_SUBJECTS
        self._compiled_schemas = {}
    
    def get_schema(self, schema_name: str) -> Dict[str, Any]:
        """Get schema by name."""
        if schema_name not in self.schemas:
            raise SchemaRegistryError(f"Schema {schema_name} not found")
        return self.schemas[schema_name]
    
    def get_compiled_schema(self, schema_name: str) -> avro.schema.Schema:
        """Get compiled Avro schema object."""
        if schema_name not in self._compiled_schemas:
            schema_dict = self.get_schema(schema_name)
            self._compiled_schemas[schema_name] = avro.schema.parse(json.dumps(schema_dict))
        return self._compiled_schemas[schema_name]
    
    def get_schema_json(self, schema_name: str) -> str:
        """Get schema as JSON string."""
        schema = self.get_schema(schema_name)
        return json.dumps(schema, indent=2)
    
    def get_registry_subject(self, topic: str) -> str:
        """Get Schema Registry subject for topic."""
        subject_key = f"{topic}-value"
        if subject_key not in self.registry_subjects:
            raise SchemaRegistryError(f"No schema found for topic {topic}")
        return subject_key
    
    def validate_data(self, schema_name: str, data: Dict[str, Any]) -> bool:
        """
        Validate data against Avro schema.
        
        Performs comprehensive validation of data structure, types, and
        constraints against the specified Avro schema using Avro's
        built-in validation mechanisms.
        
        Args:
            schema_name: Name of the schema to validate against
            data: Data dictionary to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        try:
            schema = self.get_compiled_schema(schema_name)
            # Use Avro's validation
            avro.io.validate(schema, data)
            return True
        except Exception as e:
            self.logger.error(f"Schema validation failed for {schema_name}", error=e)
            return False
    
    def serialize_data(self, schema_name: str, data: Dict[str, Any]) -> bytes:
        """Serialize data using Avro schema."""
        try:
            schema = self.get_compiled_schema(schema_name)
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            return bytes_writer.getvalue()
        except Exception as e:
            raise AvroSerializationError(f"Failed to serialize data with schema {schema_name}: {str(e)}")
    
    def deserialize_data(self, schema_name: str, data: bytes) -> Dict[str, Any]:
        """Deserialize data using Avro schema."""
        try:
            schema = self.get_compiled_schema(schema_name)
            reader = avro.io.DatumReader(schema)
            bytes_reader = io.BytesIO(data)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            return reader.read(decoder)
        except Exception as e:
            raise AvroSerializationError(f"Failed to deserialize data with schema {schema_name}: {str(e)}")
    
    def get_available_schemas(self) -> List[str]:
        """Get list of available schema names."""
        return list(self.schemas.keys())
    
    def get_registry_subjects(self) -> List[str]:
        """Get list of Schema Registry subjects."""
        return list(self.registry_subjects.keys())

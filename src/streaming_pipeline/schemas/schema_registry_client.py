"""
Schema Registry client for managing Avro schemas.
Handles schema registration, retrieval, and evolution.
"""
import json
import logging
from typing import Dict, Any, Optional, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .avro_schemas import SCHEMA_REGISTRY_SUBJECTS, get_all_schemas


logger = logging.getLogger(__name__)


class SchemaRegistryError(Exception):
    """Custom exception for Schema Registry errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[Dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class SchemaRegistryClient:
    """
    Client for interacting with Confluent Schema Registry.
    
    Handles schema registration, retrieval, compatibility checking,
    and schema evolution management.
    """
    
    def __init__(self, schema_registry_url: str = "http://localhost:8085"):
        """
        Initialize Schema Registry client.
        
        Args:
            schema_registry_url: URL of the Schema Registry service
        """
        self.base_url = schema_registry_url.rstrip('/')
        self.session = self._create_session()
        
        logger.info(
            "Schema Registry client initialized",
            extra={"schema_registry_url": self.base_url}
        )
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'Content-Type': 'application/vnd.schemaregistry.v1+json',
            'Accept': 'application/vnd.schemaregistry.v1+json'
        })
        
        return session
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make HTTP request to Schema Registry.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request data for POST/PUT requests
            
        Returns:
            Response data
            
        Raises:
            SchemaRegistryError: If request fails
        """
        url = f"{self.base_url}{endpoint}"
        
        # Making request to Schema Registry
        
        try:
            if method.upper() == 'GET':
                response = self.session.get(url, timeout=30)
            elif method.upper() == 'POST':
                response = self.session.post(url, json=data, timeout=30)
            elif method.upper() == 'PUT':
                response = self.session.put(url, json=data, timeout=30)
            elif method.upper() == 'DELETE':
                response = self.session.delete(url, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Check for HTTP errors
            if response.status_code >= 400:
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', f'HTTP {response.status_code}')
                except:
                    error_msg = f'HTTP {response.status_code}: {response.text}'
                
                logger.error(
                    "Schema Registry request failed",
                    extra={
                        "url": url,
                        "status_code": response.status_code,
                        "error": error_msg
                    }
                )
                raise SchemaRegistryError(error_msg, response.status_code, error_data if 'error_data' in locals() else None)
            
            # Parse response
            if response.content:
                return response.json()
            else:
                return {}
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            logger.error(
                "Schema Registry connection error",
                extra={
                    "url": url,
                    "error": error_msg
                }
            )
            raise SchemaRegistryError(error_msg)
    
    def register_schema(self, subject: str, schema: Dict[str, Any]) -> int:
        """
        Register a schema for a subject.
        
        Args:
            subject: Subject name (e.g., 'stock-quotes-realtime-value')
            schema: Avro schema as dictionary
            
        Returns:
            Schema ID assigned by registry
            
        Raises:
            SchemaRegistryError: If registration fails
        """
        logger.info(
            "Registering schema",
            extra={
                "subject": subject,
                "schema_name": schema.get("name", "unknown")
            }
        )
        
        data = {"schema": json.dumps(schema)}
        
        try:
            response = self._make_request('POST', f'/subjects/{subject}/versions', data)
            schema_id = response.get('id')
            
            if schema_id is None:
                raise SchemaRegistryError(f"No schema ID returned for subject {subject}")
            
            logger.info(
                "Schema registered successfully",
                extra={
                    "subject": subject,
                    "schema_id": schema_id,
                    "version": response.get('version')
                }
            )
            
            return schema_id
            
        except SchemaRegistryError:
            raise
        except Exception as e:
            error_msg = f"Unexpected error registering schema for {subject}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise SchemaRegistryError(error_msg)
    
    def get_schema_by_id(self, schema_id: int) -> Dict[str, Any]:
        """
        Get schema by ID.
        
        Args:
            schema_id: Schema ID
            
        Returns:
            Schema data
        """
        # Retrieving schema by ID
        
        response = self._make_request('GET', f'/schemas/ids/{schema_id}')
        schema_str = response.get('schema')
        
        if not schema_str:
            raise SchemaRegistryError(f"No schema found for ID {schema_id}")
        
        return json.loads(schema_str)
    
    def get_latest_schema(self, subject: str) -> Dict[str, Any]:
        """
        Get latest schema version for a subject.
        
        Args:
            subject: Subject name
            
        Returns:
            Schema data with metadata
        """
        # Retrieving latest schema for subject
        
        response = self._make_request('GET', f'/subjects/{subject}/versions/latest')
        
        return {
            'id': response.get('id'),
            'version': response.get('version'),
            'schema': json.loads(response.get('schema', '{}')),
            'subject': response.get('subject')
        }
    
    def list_subjects(self) -> List[str]:
        """
        List all subjects in the registry.
        
        Returns:
            List of subject names
        """
        # Listing all subjects
        
        response = self._make_request('GET', '/subjects')
        return response if isinstance(response, list) else []
    
    def check_compatibility(self, subject: str, schema: Dict[str, Any]) -> bool:
        """
        Check if schema is compatible with latest version.
        
        Args:
            subject: Subject name
            schema: Schema to check
            
        Returns:
            True if compatible, False otherwise
        """
        # Checking compatibility for subject
        
        data = {"schema": json.dumps(schema)}
        
        try:
            response = self._make_request('POST', f'/compatibility/subjects/{subject}/versions/latest', data)
            return response.get('is_compatible', False)
        except SchemaRegistryError as e:
            if e.status_code == 404:
                # Subject doesn't exist yet, so it's compatible
                return True
            raise
    
    def delete_subject(self, subject: str, permanent: bool = False) -> List[int]:
        """
        Delete a subject and all its versions.
        
        Args:
            subject: Subject name
            permanent: If True, permanently delete (cannot be undone)
            
        Returns:
            List of deleted version numbers
        """
        logger.warning(
            f"Deleting subject: {subject}",
            extra={"permanent": permanent}
        )
        
        endpoint = f'/subjects/{subject}'
        if permanent:
            endpoint += '?permanent=true'
        
        response = self._make_request('DELETE', endpoint)
        return response if isinstance(response, list) else []
    
    def register_all_schemas(self) -> Dict[str, int]:
        """
        Register all predefined schemas from avro_schemas.py.
        
        Returns:
            Dictionary mapping subject names to schema IDs
        """
        logger.info("Registering all predefined schemas")
        
        results = {}
        
        for subject, schema in SCHEMA_REGISTRY_SUBJECTS.items():
            try:
                # Check compatibility first
                if not self.check_compatibility(subject, schema):
                    logger.warning(
                        f"Schema for {subject} is not compatible with existing version",
                        extra={"subject": subject}
                    )
                    continue
                
                schema_id = self.register_schema(subject, schema)
                results[subject] = schema_id
                
            except SchemaRegistryError as e:
                logger.error(
                    f"Failed to register schema for {subject}",
                    extra={
                        "subject": subject,
                        "error": str(e)
                    }
                )
                # Continue with other schemas
                continue
        
        logger.info(
            "Schema registration completed",
            extra={
                "registered_count": len(results),
                "total_schemas": len(SCHEMA_REGISTRY_SUBJECTS),
                "subjects": list(results.keys())
            }
        )
        
        return results
    
    def get_registry_status(self) -> Dict[str, Any]:
        """
        Get Schema Registry status and health information.
        
        Returns:
            Status information
        """
        try:
            # Try to list subjects as a health check
            subjects = self.list_subjects()
            
            return {
                "status": "healthy",
                "base_url": self.base_url,
                "subjects_count": len(subjects),
                "subjects": subjects
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "base_url": self.base_url,
                "error": str(e)
            }
    
    def close(self) -> None:
        """Close the HTTP session."""
        if self.session:
            self.session.close()
            logger.info("Schema Registry client session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Convenience function for quick schema registration
def register_schemas(schema_registry_url: str = "http://localhost:8085") -> Dict[str, int]:
    """
    Register all schemas with Schema Registry.
    
    Args:
        schema_registry_url: Schema Registry URL
        
    Returns:
        Dictionary mapping subject names to schema IDs
    """
    with SchemaRegistryClient(schema_registry_url) as client:
        return client.register_all_schemas()
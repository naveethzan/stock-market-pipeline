# Schema Scripts

Scripts for managing Avro schemas and Schema Registry.

## Scripts

### `init-schema-registry.py`
Initializes Schema Registry with Avro schemas.
- Registers all Avro schemas with Schema Registry
- Waits for Schema Registry to be available
- Verifies schema registration
- Provides detailed logging and error handling

**Usage:**
```bash
# Register all schemas
python3 scripts/schemas/init-schema-registry.py

# Verify existing schemas only
python3 scripts/schemas/init-schema-registry.py --verify-only

# Use custom Schema Registry URL
python3 scripts/schemas/init-schema-registry.py --schema-registry-url http://localhost:8085
```

**Features:**
- Automatic retry logic
- Schema validation
- Detailed status reporting
- Command-line options for flexibility

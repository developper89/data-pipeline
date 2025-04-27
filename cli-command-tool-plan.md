# CLI Command Tool for Cache Service

## Overview

This document outlines the plan for implementing a Command Line Interface (CLI) tool for interacting with the Redis cache service. This CLI tool will provide direct access to cached device metadata through the command line, making it ideal for operations, debugging, and automation tasks.

## Purpose

The CLI tool serves several key purposes:

- Provide system administrators with a way to check cache status
- Help developers debug cache-related issues
- Support automation scripts that need to retrieve device metadata
- Enable operations teams to monitor system health
- Allow for quick data retrieval without requiring HTTP APIs

## Technical Architecture

### Directory Structure

```
cache_service/
└── cli/
    ├── __init__.py
    ├── commands.py   # CLI command implementations
    ├── main.py       # CLI entry point
    ├── utils.py      # Helper functions
    └── formatters.py # Output formatters
```

### Technology Stack

- **Click**: Python package for creating beautiful command-line interfaces
- **Rich**: For terminal formatting, tables, and progress bars
- **Redis-py**: Direct Redis access using the same client as the cache service
- **PyYAML**: For YAML output format support

## Core Features

### Command Set

1. **List Devices**

   ```bash
   cache-cli list [--limit N] [--page N] [--filter PATTERN]
   ```

   Lists all device IDs in the cache, with optional pagination and filtering.

2. **Get Device Metadata**

   ```bash
   cache-cli get <device_id>
   ```

   Retrieves and displays the metadata for a specific device in a formatted table.

3. **Cache Statistics**

   ```bash
   cache-cli info
   ```

   Shows statistics about the cache: total devices, memory usage, hit/miss ratio.

4. **Connection Management**

   ```bash
   cache-cli connect --host REDIS_HOST --port REDIS_PORT --password REDIS_PASSWORD
   ```

   Sets up connection parameters for the CLI (can also use environment variables).

5. **Configuration**
   ```bash
   cache-cli config [--set KEY=VALUE] [--get KEY] [--list]
   ```
   Manages CLI configuration settings stored in a config file.

### Output Formats

The CLI will support multiple output formats to accommodate different use cases:

1. **Table** (default)

   ```bash
   cache-cli get <device_id> --format table
   ```

   Displays data in a well-formatted table for human readability.

2. **JSON**

   ```bash
   cache-cli get <device_id> --format json
   ```

   Outputs data in JSON format for programmatic consumption.

3. **YAML**

   ```bash
   cache-cli get <device_id> --format yaml
   ```

   Provides data in YAML format for configuration files or documentation.

4. **CSV**
   ```bash
   cache-cli list --format csv
   ```
   Outputs data as CSV for import into spreadsheets or databases.

### Additional Features

1. **Export to File**

   ```bash
   cache-cli get <device_id> --output file.json
   ```

   Save output to a file instead of displaying on screen.

2. **Verbose Mode**

   ```bash
   cache-cli --verbose get <device_id>
   ```

   Displays detailed information about the operation, useful for debugging.

3. **Quiet Mode**

   ```bash
   cache-cli --quiet get <device_id>
   ```

   Suppresses all output except errors, useful for scripting.

4. **Batch Operations**
   ```bash
   cache-cli batch --file devices.txt
   ```
   Process multiple device IDs from a file.

## Implementation Plan

### Phase 1: Core Functionality

1. Set up the CLI framework using Click
2. Implement Redis connection handling
3. Develop the basic commands (list, get, info)
4. Add table output format
5. Implement error handling

### Phase 2: Enhanced Features

1. Add additional output formats (JSON, YAML, CSV)
2. Implement configuration management
3. Add export to file functionality
4. Implement filtering and pagination
5. Add batch operations

### Phase 3: Polish and Documentation

1. Add colorized output for better readability
2. Implement progress indicators for long-running operations
3. Create comprehensive --help documentation
4. Write a user guide with examples
5. Add automated tests

## Configuration

The CLI tool will support configuration from multiple sources, in order of precedence:

1. Command-line arguments
2. Environment variables
3. Configuration file (~/.cache-cli/config.yaml)
4. Default values

### Environment Variables

- `CACHE_CLI_REDIS_HOST`: Redis host address
- `CACHE_CLI_REDIS_PORT`: Redis port number
- `CACHE_CLI_REDIS_PASSWORD`: Redis password
- `CACHE_CLI_REDIS_DB`: Redis database number
- `CACHE_CLI_OUTPUT_FORMAT`: Default output format

## Distribution

The CLI tool will be packaged for easy installation:

1. **PyPI Package**

   ```bash
   pip install cache-service-cli
   ```

2. **Docker Container**

   ```bash
   docker run --rm cache-service-cli list
   ```

3. **Standalone Binary**
   Created using PyInstaller for users without Python installed.

## Use Cases

### Operational Monitoring

Operations teams can quickly check the status of the cache:

```bash
$ cache-cli info
Cache Status: CONNECTED
Total Devices: 127
Memory Usage: 24.5 MB
Oldest Entry: 2023-07-01 12:34:56 UTC (2 days ago)
Newest Entry: 2023-07-03 09:12:34 UTC (10 minutes ago)
```

### Debugging

Developers can inspect specific device metadata for troubleshooting:

```bash
$ cache-cli get device_123
Device ID: device_123
Last Updated: 2023-07-03 08:45:12 UTC

+---------------+-------------------+
| Field         | Value             |
+---------------+-------------------+
| datatype_id   | temp_sensor_v2    |
| datatype_name | Temperature       |
| datatype_unit | Celsius           |
| persist       | true              |
+---------------+-------------------+
```

### Automation

Scripts can use the CLI for automated tasks:

```bash
#!/bin/bash
# Get all devices and process their metadata
for device in $(cache-cli list --format csv); do
    metadata=$(cache-cli get $device --format json)
    # Process metadata...
done
```

## Security Considerations

1. **Password Handling**

   - Never display passwords in help text or error messages
   - Support password input via environment variables or file

2. **Authentication**

   - Support token-based authentication if talking to the API
   - Implement proper credential storage

3. **Logging**
   - Ensure sensitive data is not logged
   - Provide configurable logging levels

## Conclusion

The CLI Command Tool provides a powerful interface for interacting with the cache service directly from the command line. Its flexibility, ease of use, and scriptability make it an ideal tool for operations teams, developers, and automation tasks.

By implementing this CLI tool, we enable efficient access to cached device metadata without requiring a web interface or custom application, simplifying operations and enhancing productivity.

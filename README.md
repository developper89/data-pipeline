# Dynamic Connector Container Manager

This project implements a dynamic container management system for various protocol connectors to Kafka. It allows you to define connector configurations in YAML and automatically manages Docker containers for each connector.

## Project Structure

```
.
├── docker-compose.yml
├── manager/
│   ├── Dockerfile
│   ├── manager.py
│   ├── connectors_config.yaml
│   └── requirements.txt
└── connector/
    └── mqtt/              # MQTT protocol connector
        ├── Dockerfile
        ├── entrypoint.sh
        ├── requirements.txt
        └── scripts/
            └── mqtt_to_kafka.py
```

## Features

- Protocol-based connector organization
- Dynamic container management based on YAML configuration
- Automatic container creation and removal
- Protocol-specific message forwarding to Kafka
- Configurable connector scripts
- Centralized logging
- Automatic reconnection handling

## Prerequisites

- Docker
- Docker Compose

## Configuration

### Connector Configuration

Edit `manager/connectors_config.yaml` to define your connectors:

```yaml
connectors:
  - connector_id: "mqtt_connector1"
    image: "mqtt-connector:latest"
    env:
      BROKER_HOST: "broker1.example.com"
      BROKER_PORT: "1883"
      KAFKA_BOOTSTRAP: "kafka:9092"
      KAFKA_TOPIC: "topic1"
      MQTT_TOPICS: "sensors/#,devices/#" # Comma-separated list of topics
      CONNECTOR_SCRIPT_PATH: "/app/connector_scripts/mqtt_to_kafka.py"
```

### Adding New Protocol Connectors

1. Create a new directory under `connector/` for your protocol (e.g., `connector/modbus/`)
2. Add the protocol-specific files:
   - Dockerfile
   - requirements.txt
   - entrypoint.sh
   - scripts/
3. Update `connectors_config.yaml` with the new connector configuration

## Usage

1. Build the images:

   ```bash
   docker-compose build
   ```

2. Start the system:

   ```bash
   docker-compose up -d
   ```

3. Monitor the logs:
   ```bash
   docker-compose logs -f
   ```

## Adding New Connectors

1. Choose or create the appropriate protocol directory in `connector/`
2. Add your connector script to the protocol's `scripts/` directory
3. Add the connector configuration to `manager/connectors_config.yaml`
4. The manager will automatically detect the new configuration and create the container

## Removing Connectors

1. Remove the connector configuration from `manager/connectors_config.yaml`
2. The manager will automatically detect the change and remove the container

## Development

To modify a protocol connector:

1. Update the connector code in `connector/<protocol>/scripts/`
2. Rebuild the protocol connector image:
   ```bash
   docker-compose build <protocol>-connector
   ```

To modify the manager:

1. Update the manager code in `manager/`
2. Rebuild the manager image:
   ```bash
   docker-compose build manager
   ```

## Supported Protocols

Currently supported protocols:

- MQTT: Connects to MQTT brokers and forwards messages to Kafka

## Adding New Protocols

To add support for a new protocol:

1. Create a new directory under `connector/` for your protocol
2. Implement the protocol-specific connector logic
3. Add protocol-specific configuration options
4. Update documentation

## Troubleshooting

- Check container logs:

  ```bash
  docker-compose logs -f [service-name]
  ```

- Check connector status:

  ```bash
  docker ps -a | grep connector_container
  ```

- Restart the manager:
  ```bash
  docker-compose restart manager
  ```

## License

MIT

# shared/translation/protobuf/proto_schemas/efento/__init__.py

"""
Protobuf schemas for Efento NB-IoT sensors.

This package contains compiled protobuf modules from Efento's Proto-files repository:
https://github.com/efento/Proto-files

To compile the proto files:
1. Download proto files from Efento's GitHub repository
2. Run: protoc --python_out=. proto_measurements.proto proto_device_info.proto proto_config.proto
3. Place the generated *_pb2.py files in this directory
"""

# Import compiled protobuf modules when they are available
try:
    from . import proto_measurements_pb2
    from . import proto_device_info_pb2
    from . import proto_config_pb2
    
    __all__ = [
        'proto_measurements_pb2',
        'proto_device_info_pb2', 
        'proto_config_pb2'
    ]
except ImportError:
    # Protobuf modules not yet compiled
    __all__ = [] 
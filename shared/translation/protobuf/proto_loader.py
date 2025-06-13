import importlib
from typing import Any

class ProtoModuleLoader:
    """Loads pre-compiled protobuf modules for different manufacturers."""

    @staticmethod
    def load_proto_module(manufacturer: str, module_name: str) -> Any:
        """Load a compiled protobuf module for the given manufacturer."""
        module_path = f"shared.translation.protobuf.proto_schemas.{manufacturer}.{module_name}"
        return importlib.import_module(module_path)

    @staticmethod
    def get_proto_class(manufacturer: str, module_name: str, class_name: str) -> Any:
        """Get a specific protobuf class from a module."""
        module = ProtoModuleLoader.load_proto_module(manufacturer, module_name)
        return getattr(module, class_name) 
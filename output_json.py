#!/usr/bin/env python3
"""
Output decoded payload in JSON format
"""

from protobuf_parser import ProtoMeasurementsParser
import json


def main():
    """Parse the example payload and output as JSON."""
    
    # Example payload from the documentation
    payload_hex = "0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000220e080210b8d0a7c20620722a0200002896d5a7c20640014809820100"
    payload_bytes = bytes.fromhex(payload_hex)
    
    parser = ProtoMeasurementsParser()
    result = parser.parse_complete_message(payload_bytes)
    
    # Output as formatted JSON
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main() 
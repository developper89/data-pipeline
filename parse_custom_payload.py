#!/usr/bin/env python3
"""
Custom Payload Parser Example

This script demonstrates how to use the ProtoMeasurementsParser 
with your own payload data.
"""

from protobuf_parser import ProtoMeasurementsParser
import json


def parse_custom_payload(payload_hex: str):
    """Parse a custom payload provided as hex string."""
    
    print(f"Parsing custom payload: {payload_hex}")
    print(f"Length: {len(payload_hex)//2} bytes\n")
    
    try:
        # Convert hex string to bytes
        payload_bytes = bytes.fromhex(payload_hex)
        
        # Create parser and parse
        parser = ProtoMeasurementsParser()
        result = parser.parse_complete_message(payload_bytes)
        
        # Print human-readable results
        parser.print_parsed_data(result)
        
        # Optionally save to JSON file
        with open('parsed_result.json', 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\n💾 Results saved to 'parsed_result.json'")
        
        return result
        
    except ValueError as e:
        print(f"❌ Invalid hex string: {e}")
        return None
    except Exception as e:
        print(f"❌ Parsing error: {e}")
        return None


def main():
    """Example usage."""
    
    # Example 1: Use the documented payload
    print("=" * 80)
    print("EXAMPLE 1: Documented payload")
    print("=" * 80)
    
    documented_payload = "0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000220e080210b8d0a7c20620722a0200002896d5a7c20640014809820100"
    parse_custom_payload(documented_payload)
    
    # Example 2: Parse custom payload (replace with your own)
    print("\n\n" + "=" * 80)
    print("EXAMPLE 2: Custom payload (replace with your own)")
    print("=" * 80)
    
    # Replace this with your actual payload hex string
    custom_payload = "0a06282c02424eed1001183c"  # Truncated example
    
    print("Note: This is a truncated example payload.")
    print("Replace 'custom_payload' variable with your actual hex string.")
    print(f"Current payload: {custom_payload}")


if __name__ == "__main__":
    main() 
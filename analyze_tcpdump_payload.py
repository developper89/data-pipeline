#!/usr/bin/env python3
"""
Analyze protobuf payload from tcpdump hex dump
"""

def extract_protobuf_from_hex():
    # From the tcpdump output at 13:02:18.889182
    # The CoAP payload starts around offset 0x0020
    hex_lines = [
        "5402 e408 5244 7cbf b16d ff0a 0628 2c02 424e ed10",
        "0118 3c22 0f08 0110 b0e6 aac2 0620 d403",
        "2a02 0001 220e 0802 10b0 e6aa c206 2070",
        "2a02 0000 288e ebaa c206 4001 4809 8201",
        "00"
    ]
    
    # Join and clean up the hex data
    hex_data = " ".join(hex_lines).replace(" ", "")
    
    # Find the protobuf start (0a06 pattern for field 1)
    protobuf_start = hex_data.find("0a06")
    if protobuf_start == -1:
        print("Could not find protobuf start pattern")
        return None
    
    # Extract protobuf data from the start
    protobuf_hex = hex_data[protobuf_start:]
    
    # Convert to bytes
    try:
        protobuf_bytes = bytes.fromhex(protobuf_hex)
        return protobuf_bytes
    except ValueError as e:
        print(f"Error converting hex: {e}")
        return None

def parse_device_id(data):
    """Extract device ID from protobuf field 1"""
    if len(data) < 8:
        return None
    
    # Look for field 1 (0x0a) with length 6 (0x06)
    if data[0] == 0x0a and data[1] == 0x06:
        device_id_bytes = data[2:8]
        device_id = device_id_bytes.hex()
        return device_id
    return None

def analyze_coap_header():
    """Analyze the CoAP header from the hex dump"""
    # CoAP header starts at offset 0x0020 in the UDP payload
    coap_header_hex = "5402e408"
    header_bytes = bytes.fromhex(coap_header_hex)
    
    ver_type_tkl = header_bytes[0]  # 0x54
    code = header_bytes[1]          # 0x02
    message_id = int.from_bytes(header_bytes[2:4], 'big')  # 0xe408
    
    version = (ver_type_tkl >> 6) & 0x3
    msg_type = (ver_type_tkl >> 4) & 0x3
    token_length = ver_type_tkl & 0xF
    
    msg_types = {0: "CON", 1: "NON", 2: "ACK", 3: "RST"}
    codes = {1: "GET", 2: "POST", 3: "PUT", 4: "DELETE"}
    
    print("=== CoAP Header Analysis ===")
    print(f"Version: {version}")
    print(f"Type: {msg_types.get(msg_type, 'Unknown')} ({msg_type})")
    print(f"Token Length: {token_length}")
    print(f"Code: {codes.get(code, f'Unknown ({code})')}")
    print(f"Message ID: {message_id}")
    print()

def main():
    print("=== tcpdump CoAP Traffic Analysis ===")
    print()
    
    # Analyze CoAP header
    analyze_coap_header()
    
    # Extract and analyze protobuf payload
    protobuf_data = extract_protobuf_from_hex()
    if protobuf_data:
        print("=== Protobuf Payload Analysis ===")
        print(f"Protobuf data length: {len(protobuf_data)} bytes")
        print(f"Raw hex: {protobuf_data.hex()}")
        print()
        
        # Extract device ID
        device_id = parse_device_id(protobuf_data)
        if device_id:
            print(f"Device ID: {device_id}")
        else:
            print("Could not extract device ID")
        
        # Show first few bytes for manual analysis
        print("\nFirst 20 bytes breakdown:")
        for i in range(min(20, len(protobuf_data))):
            byte_val = protobuf_data[i]
            print(f"  Byte {i:2d}: 0x{byte_val:02x} ({byte_val:3d}) {'[Field tag]' if i % 2 == 0 and byte_val & 0x07 == 2 else ''}")
    
    print("\n=== Server Response Analysis ===")
    print("Server responded with: 'Direct root access not allowed'")
    print("This suggests:")
    print("1. The CoAP server is receiving the request")
    print("2. Authentication/authorization is failing")
    print("3. The device might need proper credentials or endpoint")

if __name__ == "__main__":
    main() 
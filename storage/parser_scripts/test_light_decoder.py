import struct
from typing import Dict, Any

def decode_light_packet(payload_hex: str) -> Dict[str, Any]:
    """
    Decode a light controller packet from hex string.
    Handles both single and double-encoded hex.
    """
    # Check if this is double-encoded (outer hex layer)
    if len(payload_hex) > 26 and all(c in "0123456789abcdefABCDEF" for c in payload_hex):
        try:
            # Try to decode as double-encoded hex
            inner_hex = bytes.fromhex(payload_hex).decode("ascii")
            if len(inner_hex) >= 26 and all(c in "0123456789abcdefABCDEF" for c in inner_hex):
                print(f"Double-encoded hex detected")
                print(f"Outer hex: {payload_hex}")
                print(f"Inner hex: {inner_hex}")
                payload_hex = inner_hex
        except Exception:
            # Not double-encoded, use as-is
            pass

    # Extract exactly 13 bytes (26 hex chars)
    if len(payload_hex) < 26:
        raise ValueError(f"Invalid packet length: {len(payload_hex)//2} bytes (minimum 13 required)")

    binary_data = bytes.fromhex(payload_hex[:26])

    # Unpack using LITTLE ENDIAN
    id_capteur, epoch, sync_header, sync_alarm = struct.unpack("=IIBI", binary_data)

    # Decode header fields
    header = {
        "task_id": sync_header & 0x0F,
        "reserved": (sync_header >> 4) & 0x01,
        "all": (sync_header >> 5) & 0x01,
        "clear": (sync_header >> 6) & 0x01,
        "get_set": (sync_header >> 7) & 0x01,
    }

    # Decode alarm fields
    alarm = {
        "time_beg": sync_alarm & 0x7FF,
        "duration": (sync_alarm >> 11) & 0x7FF,
        "day": (sync_alarm >> 22) & 0x07,
        "task_id": (sync_alarm >> 25) & 0x0F,
        "channel": (sync_alarm >> 29) & 0x01,
        "active": (sync_alarm >> 30) & 0x01,
        "run_once": (sync_alarm >> 31) & 0x01,
    }

    # Determine operation
    operation_map = {
        (0, 0, 0): "read_task",
        (0, 1, 0): "read_all",
        (1, 0, 0): "create_task",
        (1, 0, 1): "delete_task",
        (1, 1, 1): "delete_all",
    }

    operation_key = (header["get_set"], header["all"], header["clear"])
    operation = operation_map.get(operation_key, "unknown")

    return {
        "id_capteur": id_capteur,
        "epoch": epoch,
        "sync_header": sync_header,
        "sync_alarm": sync_alarm,
        "header": header,
        "alarm": alarm,
        "operation": operation,
    }

# Test with your examples
if __name__ == "__main__":
    test_payloads = [
        "3139323432333030656666343963363730303030303030303030",  # Double-encoded
        "2c5d200017b0816820ff9fff0f",  # Single-encoded
    ]

    for payload in test_payloads:
        print(f"\n{'='*60}")
        print(f"Testing payload: {payload}")
        print(f"{'='*60}")

        try:
            result = decode_light_packet(payload)

            print(f"\nDecoded successfully:")
            print(f"  Device ID: {result['id_capteur']}")
            print(f"  Epoch: {result['epoch']}")
            print(f"  Operation: {result['operation']}")
            print(f"\nHeader:")
            for k, v in result["header"].items():
                print(f"  {k}: {v}")
            print(f"\nAlarm:")
            for k, v in result["alarm"].items():
                print(f"  {k}: {v}")

            if result["operation"] == "read_all":
                print(f"\n*** Number of configured tasks: {result['alarm']['task_id']} ***")

        except Exception as e:
            print(f"Error decoding: {e}")
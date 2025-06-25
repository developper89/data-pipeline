#!/usr/bin/env python3
"""
Debug script to identify available CoAP clients in the container
"""

import subprocess
import os

def check_coap_clients():
    """Check for available CoAP clients"""
    print("🔍 CoAP Client Detection Debug")
    print("=" * 40)
    
    # Check common client names
    clients = [
        "coap-client-notls", 
        "coap-client", 
        "coap", 
        "coap-client-openssl", 
        "coap-client-gnutls",
        "coap-client-mbedtls"
    ]
    
    print("\n1️⃣ Checking common client names:")
    found_clients = []
    
    for client in clients:
        try:
            result = subprocess.run(["which", client], capture_output=True, timeout=5)
            if result.returncode == 0:
                path = result.stdout.decode().strip()
                print(f"   ✅ {client} -> {path}")
                
                # Test if it works
                try:
                    test = subprocess.run([client, "--help"], capture_output=True, timeout=5)
                    if test.returncode == 0:
                        print(f"      🟢 Working (exit code: {test.returncode})")
                        found_clients.append(client)
                    else:
                        print(f"      🔴 Not working (exit code: {test.returncode})")
                except Exception as e:
                    print(f"      🔴 Error testing: {e}")
            else:
                print(f"   ❌ {client} not found")
        except Exception as e:
            print(f"   ❌ {client} error: {e}")
    
    # Search for any coap-related binaries
    print("\n2️⃣ Searching for CoAP-related binaries:")
    search_paths = ["/usr/bin", "/usr/local/bin", "/bin", "/usr/sbin"]
    
    all_coap_binaries = []
    for search_path in search_paths:
        if os.path.exists(search_path):
            try:
                result = subprocess.run(["find", search_path, "-name", "*coap*", "-type", "f"], 
                                      capture_output=True, timeout=10)
                if result.returncode == 0 and result.stdout:
                    binaries = result.stdout.decode().strip().split('\n')
                    for binary in binaries:
                        if binary and binary not in all_coap_binaries:
                            all_coap_binaries.append(binary)
            except:
                pass
    
    if all_coap_binaries:
        print("   Found CoAP-related binaries:")
        for binary in all_coap_binaries:
            print(f"     📄 {binary}")
            
            # Test if it's a client
            if 'client' in os.path.basename(binary).lower():
                binary_name = os.path.basename(binary)
                try:
                    test = subprocess.run([binary_name, "--help"], capture_output=True, timeout=5)
                    if test.returncode == 0:
                        print(f"        🟢 This looks like a working client!")
                        if binary_name not in found_clients:
                            found_clients.append(binary_name)
                    else:
                        print(f"        🔴 Exit code: {test.returncode}")
                except Exception as e:
                    print(f"        🔴 Error: {e}")
    else:
        print("   No CoAP-related binaries found")
    
    # Check package installation
    print("\n3️⃣ Checking package installation:")
    packages = ["libcoap3-bin", "libcoap-bin", "coap-utils"]
    
    for package in packages:
        try:
            result = subprocess.run(["dpkg", "-l", package], capture_output=True, timeout=5)
            if result.returncode == 0:
                print(f"   ✅ {package} is installed")
                
                # List files from package
                files_result = subprocess.run(["dpkg", "-L", package], capture_output=True, timeout=5)
                if files_result.returncode == 0:
                    files = files_result.stdout.decode().strip().split('\n')
                    bin_files = [f for f in files if '/bin/' in f and os.path.isfile(f)]
                    if bin_files:
                        print(f"      Executables from {package}:")
                        for bin_file in bin_files:
                            print(f"        📄 {bin_file}")
            else:
                print(f"   ❌ {package} not installed")
        except:
            print(f"   ❓ Could not check {package}")
    
    # Summary
    print("\n📊 Summary:")
    if found_clients:
        print(f"   ✅ Found {len(found_clients)} working CoAP client(s):")
        for client in found_clients:
            print(f"      🎯 {client}")
        print(f"\n   💡 Recommended: Use '{found_clients[0]}' in your simulation")
    else:
        print("   ❌ No working CoAP clients found")
        print("   💡 Try installing: apt-get update && apt-get install -y libcoap3-bin")
    
    return found_clients

if __name__ == "__main__":
    check_coap_clients() 
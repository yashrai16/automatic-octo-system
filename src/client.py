import requests
import json
import time

class KVClient:
    def __init__(self, nodes_map):
        self.nodes = nodes_map

    def put(self, target_node_id, key, value):
        url = self.nodes.get(target_node_id)
        if not url: return None
        try:
            print(f"Client: PUT '{key}':'{value}' to {target_node_id}")
            response = requests.post(f"{url}/put", json={'key': key, 'value': value}, timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Client: Error PUT to {target_node_id}: {e}")
            return None

    def get(self, target_node_id, key):
        url = self.nodes.get(target_node_id)
        if not url: return None
        try:
            print(f"Client: GET '{key}' from {target_node_id}")
            response = requests.get(f"{url}/get/{key}", timeout=5)
            response.raise_for_status()
            data = response.json()
            print(f"Client: {target_node_id} -> '{key}': '{data.get('value')}', VC: {data.get('vector_clock')}")
            return data
        except requests.exceptions.RequestException as e:
            print(f"Client: Error GET from {target_node_id}: {e}")
            return None

    def get_status(self, target_node_id):
        url = self.nodes.get(target_node_id)
        if not url: return None
        try:
            response = requests.get(f"{url}/status", timeout=5)
            response.raise_for_status()
            data = response.json()
            print(f"Client: Status from {target_node_id}: KV={data.get('kv_store')}, VC={data.get('vector_clock')}, Buffered={data.get('buffered_messages_count')}")
            return data
        except requests.exceptions.RequestException as e:
            print(f"Client: Error status from {target_node_id}: {e}")
            return None

if __name__ == "__main__":
    NODE_CONFIG = {
        'node1': 'http://localhost:8001',
        'node2': 'http://localhost:8002',
        'node3': 'http://localhost:8003',
    }
    client = KVClient(NODE_CONFIG)

    print("\n--- Starting Causal Consistency Test Scenario ---")
    key = "causal_key"

    # Event A: Client writes 'value_A' to Node1
    print("\n--- Step 1: Write 'value_A' to Node1 (Event A) ---")
    client.put('node1', key, 'value_A')
    time.sleep(1)

    # Event B: Client reads 'causal_key' from Node2 (should see 'value_A'). Depends on A.
    print(f"\n--- Step 2: Read '{key}' from Node2 (Event B, depends on A) ---")
    client.get('node2', key)
    time.sleep(1)

    # Event C: Client writes 'value_C' to Node3. Depends on B (which depends on A).
    print(f"\n--- Step 3: Write 'value_C' to Node3 (Event C, depends on B) ---")
    client.put('node3', key, 'value_C')
    time.sleep(1)

    print("\n--- Waiting for replication and buffer processing (5 seconds) ---")
    time.sleep(5)

    print("\n--- Final Status Verification ---")
    # Verify all nodes eventually have 'value_C'
    client.get('node1', key)
    client.get('node2', key)
    client.get('node3', key)

    print("\n--- Causal Consistency Test Scenario Completed ---")
    print("Review node logs for 'Buffering PUT...' and 'Delivering buffered message...'")
import threading
import json
import time
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
import requests

class Node:
    def __init__(self, node_id, port, all_nodes_map):
        self.node_id = node_id
        self.port = port
        self.other_nodes = {nid: url for nid, url in all_nodes_map.items() if nid != self.node_id}

        self.kv_store = {}
        self.vector_clock = {nid: 0 for nid in all_nodes_map.keys()} # Initialize VC for all known nodes
        self.message_buffer = []

        self.lock = threading.Lock() # For thread-safe access

        print(f"Node {self.node_id} initialized on port {self.port}. VC: {self.vector_clock}")

    def _increment_local_vc(self):
        with self.lock:
            self.vector_clock[self.node_id] += 1
            # print(f"Node {self.node_id} local event. VC: {self.vector_clock}")

    def _update_vc_on_receive(self, received_vc):
        with self.lock:
            for node_id, count in received_vc.items():
                self.vector_clock[node_id] = max(self.vector_clock.get(node_id, 0), count)
            self.vector_clock[self.node_id] += 1 # Increment for the receive event itself
            # print(f"Node {self.node_id} received. Updated VC: {self.vector_clock}")

    def _is_causally_ready(self, message_vc, sender_id):
        with self.lock:
            # Condition 1: Sender's component is exactly one greater
            if message_vc.get(sender_id, 0) != self.vector_clock.get(sender_id, 0) + 1:
                return False
            # Condition 2: For all other nodes j, message_vc[j] <= local_vc[j]
            for node_id, count in message_vc.items():
                if node_id != sender_id and count > self.vector_clock.get(node_id, 0):
                    return False
            return True

    def _apply_put_and_advance_vc(self, key, value, received_vc, sender_id):
        with self.lock:
            self.kv_store[key] = value
            self._update_vc_on_receive(received_vc)
        print(f"Node {self.node_id}: Processed PUT {key}: {value} from {sender_id}. KV: {self.kv_store}, VC: {self.vector_clock}")
        self._try_deliver_buffered_messages()

    def process_replication_put(self, key, value, received_vc, sender_id):
        if self._is_causally_ready(received_vc, sender_id):
            print(f"Node {self.node_id}: Causally ready. Processing replication for {key}:{value} from {sender_id}")
            self._apply_put_and_advance_vc(key, value, received_vc, sender_id)
            return True
        else:
            print(f"Node {self.node_id}: Buffering replication for {key}:{value} from {sender_id}. VC mismatch: {received_vc} vs local {self.vector_clock}")
            with self.lock:
                self.message_buffer.append({'type': 'put_replication', 'key': key, 'value': value, 'vc': received_vc, 'sender_id': sender_id})
            return False

    def _try_deliver_buffered_messages(self):
        delivered_something = True
        while delivered_something:
            delivered_something = False
            messages_to_keep = []
            with self.lock:
                for msg in self.message_buffer:
                    if self._is_causally_ready(msg['vc'], msg['sender_id']):
                        print(f"Node {self.node_id}: Delivering buffered message - {msg['type']} {msg['key']}:{msg['value']}")
                        self._apply_put_and_advance_vc(msg['key'], msg['value'], msg['vc'], msg['sender_id'])
                        delivered_something = True
                    else:
                        messages_to_keep.append(msg)
                self.message_buffer = messages_to_keep
            if not delivered_something:
                break # No more messages delivered in this pass

    def send_replication_message(self, target_node_id, key, value):
        if target_node_id == self.node_id:
            return

        target_address = self.other_nodes.get(target_node_id)
        if not target_address:
            print(f"Error: Unknown node ID {target_node_id}")
            return

        payload = {
            'type': 'replicate_put',
            'key': key,
            'value': value,
            'vector_clock': self.vector_clock.copy(), # Send a copy of the current VC after local update
            'sender_id': self.node_id
        }
        try:
            # print(f"Node {self.node_id} sending replication {key}:{value} to {target_node_id} with VC: {payload['vector_clock']}")
            requests.post(f"{target_address}/replicate", json=payload, timeout=2)
        except requests.exceptions.RequestException as e:
            print(f"Node {self.node_id}: Could not reach {target_address} for replication: {e}")

    def handle_client_put(self, key, value):
        self._increment_local_vc() # Local event: client PUT
        with self.lock:
            self.kv_store[key] = value
        print(f"Node {self.node_id}: Client PUT {key}: {value}. Current KV: {self.kv_store}, VC: {self.vector_clock}")

        for node_id in self.other_nodes:
            self.send_replication_message(node_id, key, value)

    def handle_client_get(self, key):
        self._increment_local_vc() # Reading is also a local event
        with self.lock:
            value = self.kv_store.get(key)
        # print(f"Node {self.node_id}: Client GET {key} -> {value}. Current VC: {self.vector_clock}")
        return value

node_instance = None # Global instance for the HTTP server

class RequestHandler(BaseHTTPRequestHandler):
    def _set_headers(self, status_code=200):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data.decode('utf-8'))
        response_payload = {}
        status_code = 200

        if self.path == '/put':
            key, value = request_data.get('key'), request_data.get('value')
            if key is not None and value is not None:
                node_instance.handle_client_put(key, value)
                response_payload = {'status': 'success'}
            else: status_code = 400
        elif self.path == '/replicate':
            op_type, key, value, received_vc, sender_id = request_data.get('type'), request_data.get('key'), request_data.get('value'), request_data.get('vector_clock'), request_data.get('sender_id')
            if op_type == 'replicate_put' and all(arg is not None for arg in [key, value, received_vc, sender_id]):
                if not node_instance.process_replication_put(key, value, received_vc, sender_id):
                    response_payload = {'status': 'buffered'}
                else: response_payload = {'status': 'success'}
            else: status_code = 400
        else: status_code = 404
        self._set_headers(status_code)
        self.wfile.write(json.dumps(response_payload).encode('utf-8'))

    def do_GET(self):
        status_code = 200
        response_payload = {}
        if self.path.startswith('/get/'):
            key = self.path.split('/get/')[1]
            value = node_instance.handle_client_get(key)
            if value is not None: response_payload = {'status': 'success', 'key': key, 'value': value, 'vector_clock': node_instance.vector_clock}
            else: status_code = 404
        elif self.path == '/status':
            with node_instance.lock:
                response_payload = {'node_id': node_instance.node_id, 'kv_store': node_instance.kv_store, 'vector_clock': node_instance.vector_clock, 'buffered_messages_count': len(node_instance.message_buffer)}
        else: status_code = 404
        self._set_headers(status_code)
        self.wfile.write(json.dumps(response_payload).encode('utf-8'))

def run_server(node_obj, host='0.0.0.0'):
    global node_instance
    node_instance = node_obj
    server_address = (host, node_obj.port)
    httpd = HTTPServer(server_address, RequestHandler)
    print(f"Starting httpd server on {host}:{node_obj.port}")
    httpd.serve_forever()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python node.py <node_id> <port> <all_nodes_json_string>")
        sys.exit(1)
    my_node_id = sys.argv[1]
    my_port = int(sys.argv[2])
    all_nodes_map = json.loads(sys.argv[3])
    node = Node(my_node_id, my_port, all_nodes_map)
    run_server(node)
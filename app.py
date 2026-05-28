import os
import json
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from raft_engine import RaftEngine
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import threading

# Node configuration
NODE_NAME = os.environ.get('NODE_NAME', 'Unknown-Node')
NODE_TYPE = os.environ.get('NODE_TYPE', 'leaf') # 'leaf', 'regional', or 'cloud'
PEERS_ENV = os.environ.get('PEERS', '')
PEERS = PEERS_ENV.split(',') if PEERS_ENV else []

# Upstream configuration
UPSTREAM_URLS_ENV = os.environ.get('UPSTREAM_URLS', '')
UPSTREAM_URLS = UPSTREAM_URLS_ENV.split(',') if UPSTREAM_URLS_ENV else []


# Sync data from downstream nodes
def sync_upstream_action(logs_to_sync):
    urls_to_try = list(UPSTREAM_URLS)

    while urls_to_try:
        url = urls_to_try.pop(0)
        try:
            proxy = xmlrpc.client.ServerProxy(url, allow_none=True)
            response = proxy.sync_data(NODE_NAME, logs_to_sync)

            # If target node is leader, return True
            if response is True:
                return True 
            
            # If target node is not leader, return leader hint
            elif isinstance(response, dict) and response.get("leader_hint"):
                hint = response.get("leader_hint")
                for u in UPSTREAM_URLS:
                    if hint in u:
                        if u in urls_to_try:
                            urls_to_try.remove(u)
                        urls_to_try.insert(0, u) 
                        break 
        except Exception:
            pass
            
    return False 

# Initialize Engine based on whether it has an upstream
if UPSTREAM_URLS:
    print(f"[{NODE_NAME}] Started as {NODE_TYPE.upper()} NODE. Syncing upwards.")
    raft = RaftEngine(NODE_NAME, PEERS, on_leader_action=sync_upstream_action)
else:
    print(f"[{NODE_NAME}] Started as {NODE_TYPE.upper()} NODE (Top of Hierarchy). Awaiting data.")
    raft = RaftEngine(NODE_NAME, PEERS, on_leader_action=None)

# RPC methods

# RPC method to handle vote requests from other nodes
def rpc_request_vote(candidate_term, candidate_id):
    return raft.handle_vote_request(candidate_term, candidate_id)

# RPC method to handle append entries requests from other nodes
def rpc_append_entries(leader_term, leader_id, entries, prev_log_index, prev_log_term, leader_synced_upstream=None):
    success = raft.handle_append_entries(leader_term, leader_id, entries, prev_log_index, prev_log_term, leader_synced_upstream)
    if success and NODE_TYPE == 'leaf' and entries:
        print(f"[{NODE_NAME}] Edge Sync: Caught up {len(entries)} logs from Leader {leader_id}")
    return {"success": success, "conflict_index": len(raft.log)}

# RPC method to sync data from lower-tier nodes to higher-tier nodes
def rpc_sync_data(sender_leader, logs_received):
    if NODE_TYPE not in ['regional', 'cloud']:
        return False
        
    if raft.state != 'LEADER':
        return {"leader_hint": raft.leader_id}

    for log_entry in logs_received:
        payload = log_entry.get('payload', 'Unknown Data')
        raft.receive_client_payload(payload)

    print(f"[{NODE_TYPE.upper()} CLUSTER] Leader {NODE_NAME} saved {len(logs_received)} logs from {sender_leader}!")
    return True


# Start RPC server
def start_rpc_server():
    server = SimpleXMLRPCServer(("0.0.0.0", 9000), allow_none=True, logRequests=False)
    server.register_function(rpc_request_vote, "request_vote")
    server.register_function(rpc_append_entries, "append_entries")
    server.register_function(rpc_sync_data, "sync_data")
    server.serve_forever()

threading.Thread(target=start_rpc_server, daemon=True).start()

# HTTP Server
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    # HTTP POST method to handle data ingestion from sensors
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode('utf-8'))

        if self.path == '/data' and NODE_TYPE == 'leaf':
            payload = data.get('payload')
            result = raft.receive_client_payload(payload)

            if result.get("success"):
                print(f"[{NODE_NAME}] External Data Ingested: '{payload}' (Log Index {result['index']})")
                self.send_response(200)
            else:
                self.send_response(307)
                
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == '__main__':
    if raft:
        raft.start()
        
    server_address = ('', 8080)
    httpd = HTTPServer(server_address, SimpleHTTPRequestHandler)
    httpd.serve_forever()
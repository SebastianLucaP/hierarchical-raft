import time
import random
import threading
import json
import urllib.request
import os
import xmlrpc.client

class RaftEngine:
    def __init__(self, node_name, peers, on_leader_action=None):
        self.node_name = node_name
        self.peers = peers
        self.state = 'FOLLOWER'
        
        # Persistent state variables
        self.current_term = 0
        self.voted_for = None
        self.log = [] 
        self.last_synced_upstream = 0
        
        self.state_file = f"data/state_{self.node_name}.json"
        self.load_state()

        # Leader-only volatile state: for each peer, index of the next log entry to send
        self.next_index = {}
        self.leader_id = None
        self.on_leader_action = on_leader_action

        # Randomized election timeout (3-5s)
        self.timeout = random.uniform(3.0, 5.0)
        self.last_heartbeat = time.time()

    # Load state from file
    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    self.current_term = data.get('current_term', 0)
                    self.voted_for = data.get('voted_for', None)
                    self.log = data.get('log', [])
                    self.last_synced_upstream = data.get('last_synced_upstream', 0)
                    print(f"[{self.node_name}] Recovered state: Term {self.current_term}, Log Length {len(self.log)}")
            except Exception as e:
                print(f"[{self.node_name}] Failed to read state file: {e}")
    
    # Save state to file
    def save_state(self):
        data = {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log,
            'last_synced_upstream': self.last_synced_upstream
        }
        temp_file = f"{self.state_file}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(data, f)
        os.replace(temp_file, self.state_file)

    # Start Raft threads
    def start(self):
        threading.Thread(target=self.run_election_timer, daemon=True).start()
        threading.Thread(target=self.run_heartbeat_job, daemon=True).start()

    # Election timer
    def run_election_timer(self):
        while True:
            time.sleep(0.5)
            if self.state == 'LEADER':
                continue
            if (time.time() - self.last_heartbeat) >= self.timeout:
                self.start_election()

    # Heartbeat job for leader
    def run_heartbeat_job(self):
        while True:
            time.sleep(1.0)
            if self.state == 'LEADER':
                
                # Send heartbeats to followers (Acts as both heartbeats and missing log transmission)
                for peer in self.peers:
                    self.send_append_entries(peer)

                # Forward logs to upstream nodes
                if self.on_leader_action and self.last_synced_upstream < len(self.log):
                    logs_to_sync = self.log[self.last_synced_upstream:]
                    
                    if self.on_leader_action(logs_to_sync):
                        self.last_synced_upstream += len(logs_to_sync)
                        self.save_state()
                        for peer in self.peers:
                            self.send_append_entries(peer)

    # Client payload reception
    def receive_client_payload(self, payload):

        # If a sensor hits a Follower, reject it and provide the Leader's name
        if self.state != 'LEADER':
            return {"success": False, "leader_hint": self.leader_id}
        
        entry = {
            "term": self.current_term,
            "index": len(self.log),
            "payload": payload
        }
        
        self.log.append(entry)
        self.save_state()
        
        # Push to followers
        for peer in self.peers:
            self.send_append_entries(peer)
            
        return {"success": True, "index": entry['index']}
    
    # Start election
    def start_election(self):
        self.state = 'CANDIDATE'
        self.current_term += 1
        self.voted_for = self.node_name
        self.save_state() 
        
        votes_received = 1
        print(f"\n[{self.node_name}] Timer expired! Term {self.current_term} starting.")
        self.last_heartbeat = time.time()
        self.timeout = random.uniform(3.0, 5.0)

        for peer in self.peers:
            if self.request_vote_from_peer(peer):
                votes_received += 1
        
        # If votes are enough, become leader
        if self.state == 'CANDIDATE' and votes_received > (len(self.peers) + 1) / 2:
            self.state = 'LEADER'
            self.leader_id = self.node_name
            print(f">>> [{self.node_name}] WON THE ELECTION! <<<")

            # Reset next_index for followers
            self.next_index = {peer: len(self.log) for peer in self.peers}

            # Send heartbeats to followers
            for peer in self.peers:
                self.send_append_entries(peer)

    # Request vote from peer
    def request_vote_from_peer(self, peer_name):
        try:
            
            proxy = xmlrpc.client.ServerProxy(f"http://{peer_name}:9000", allow_none=True)
            
            response = proxy.request_vote(self.current_term, self.node_name)
            return response.get('vote_granted', False)
        except Exception:
            return False
        
    # Send append entries to follower
    def send_append_entries(self, peer_name):

        # Determine which logs this specific peer is missing
        next_idx = self.next_index.get(peer_name, len(self.log))
        entries_to_send = self.log[next_idx:]
        
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0

        # Send the entries to the follower (includes last_synced_upstream so followers stay in sync)
        try:
            proxy = xmlrpc.client.ServerProxy(f"http://{peer_name}:9000", allow_none=True)
            resp_data = proxy.append_entries(
                self.current_term,
                self.node_name,
                entries_to_send,
                prev_log_index,
                prev_log_term,
                self.last_synced_upstream
            )
            
            # If the follower accepted the logs, update the next_index
            if resp_data.get('success'):
                self.next_index[peer_name] = next_idx + len(entries_to_send)
            
            # If the follower rejected the logs, modify the next_index to be the conflict index
            else:
                conflict_index = resp_data.get('conflict_index')
                if conflict_index is not None:
                    self.next_index[peer_name] = conflict_index
                
                # If no conflict index is provided, decrement the next_index
                else:
                    if self.next_index[peer_name] > 0:
                        self.next_index[peer_name] -= 1
        except Exception:
            pass
        
    # Handle vote requests from other nodes
    def handle_vote_request(self, candidate_term, candidate_id):

        # Reject vote if the candidate's term is smaller than the current term
        if candidate_term < self.current_term:
            return {"vote_granted": False, "term": self.current_term}
            
        # If the candidate's term is greater than the current term, update the current term and state
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.state = 'FOLLOWER'
            self.voted_for = None
            self.save_state()
        
        # Grant vote if the node has not voted yet or has voted for the same candidate
        if self.voted_for is None or self.voted_for == candidate_id:
            self.voted_for = candidate_id
            self.last_heartbeat = time.time()
            self.save_state()
            return {"vote_granted": True, "term": self.current_term}
            
        return {"vote_granted": False, "term": self.current_term}

    # Handle append entries requests from other nodes
    def handle_append_entries(self, leader_term, leader_id, entries, prev_log_index, prev_log_term, leader_synced_upstream=None):
        if leader_term < self.current_term:
            return False
    
        if leader_term > self.current_term:
            self.current_term = leader_term
            self.voted_for = None
            self.save_state() 

        self.state = 'FOLLOWER'
        self.leader_id = leader_id
        self.last_heartbeat = time.time()
        
        # Reject if we don't have the previous log index
        if prev_log_index >= len(self.log):
            return False 

        # Reject if the previous log term doesn't match ours
        if prev_log_index >= 0 and self.log[prev_log_index]['term'] != prev_log_term:
            self.log = self.log[:prev_log_index]
            self.save_state()
            return False

        # If it matches, append all the new entries the leader sent
        if entries:
            for entry in entries:
                if entry['index'] == len(self.log):
                    self.log.append(entry)

        if leader_synced_upstream is not None and leader_synced_upstream > self.last_synced_upstream:
            self.last_synced_upstream = leader_synced_upstream

        self.save_state()
        return True
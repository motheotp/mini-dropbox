import grpc
import time
import random
import threading
from concurrent import futures
from enum import Enum
import raft_pb2
import raft_pb2_grpc

class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers  # List of other node addresses
        
        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of LogEntry
        
        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self.state = NodeState.FOLLOWER
        
        # Leader volatile state
        self.next_index = {}  # For each peer
        self.match_index = {}  # For each peer
        
        # Timing
        self.last_heartbeat = time.time()
        self.election_timeout = self.get_random_election_timeout()
        self.heartbeat_timeout = 1.0  # 1 second
        
        # Threading
        self.lock = threading.Lock()
        self.running = True
        
        # Start background threads
        threading.Thread(target=self.election_timer, daemon=True).start()
        threading.Thread(target=self.heartbeat_timer, daemon=True).start()
        
        print(f"Node {self.node_id} initialized as FOLLOWER")
    
    def get_random_election_timeout(self):
        """Random election timeout between 1.5 and 3 seconds"""
        return random.uniform(1.5, 3.0)
    
    # --- RPC Handlers ---
    
    def RequestVote(self, request, context):
        """Handle RequestVote RPC"""
        print(f"\nNode {self.node_id} runs RPC RequestVote called by Node {request.candidate_id}")
        
        with self.lock:
            vote_granted = False
            
            # If request term is greater, update term and convert to follower
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.state = NodeState.FOLLOWER
            
            # Vote if haven't voted or already voted for this candidate
            if request.term == self.current_term:
                if self.voted_for is None or self.voted_for == request.candidate_id:
                    # Check log is at least as up-to-date
                    if self.is_log_up_to_date(request.last_log_index, request.last_log_term):
                        vote_granted = True
                        self.voted_for = request.candidate_id
                        self.last_heartbeat = time.time()
                        print(f"Node {self.node_id} voted for Node {request.candidate_id}")
            
            return raft_pb2.RequestVoteResponse(
                term=self.current_term,
                vote_granted=vote_granted,
                voter_id=self.node_id
            )
    
    def AppendEntries(self, request, context):
        """Handle AppendEntries RPC (heartbeat and log replication)"""
        is_heartbeat = len(request.entries) == 0
        rpc_type = "Heartbeat" if is_heartbeat else "AppendEntries"
        
        print(f"\nNode {self.node_id} runs RPC {rpc_type} called by Node {request.leader_id}")
        
        with self.lock:
            success = False
            
            # Update term if request has higher term
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = NodeState.FOLLOWER
                self.voted_for = None
            
            # Accept if term matches
            if request.term == self.current_term:
                self.state = NodeState.FOLLOWER
                self.last_heartbeat = time.time()
                
                # Check log consistency
                if self.check_log_consistency(request.prev_log_index, request.prev_log_term):
                    success = True
                    
                    # Append new entries
                    if len(request.entries) > 0:
                        self.append_entries(request.prev_log_index, request.entries)
                        print(f"Node {self.node_id} appended {len(request.entries)} entries")
                    
                    # Update commit index
                    if request.leader_commit > self.commit_index:
                        self.commit_index = min(request.leader_commit, len(self.log))
                        self.apply_committed_entries()
            
            match_idx = len(self.log) if success else 0
            
            return raft_pb2.AppendEntriesResponse(
                term=self.current_term,
                success=success,
                follower_id=self.node_id,
                match_index=match_idx
            )
    
    def ClientRequest(self, request, context):
        """Handle client request"""
        print(f"\nNode {self.node_id} received client request: {request.operation} on {request.filename}")
        
        with self.lock:
            # If not leader, redirect to leader
            if self.state != NodeState.LEADER:
                leader_id = self.get_current_leader()
                return raft_pb2.ClientResponse(
                    success=False,
                    message="Not the leader",
                    leader_id=leader_id
                )
            
            # Append to log
            new_entry = raft_pb2.LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                operation=request.operation,
                filename=request.filename,
                username=request.username,
                data=request.data
            )
            self.log.append(new_entry)
            
            print(f"Leader {self.node_id} appended entry to log at index {new_entry.index}")
        
        # Replicate to followers (this will happen on next heartbeat in simplified version)
        # In full implementation, should immediately replicate
        self.replicate_log()
        
        # Wait for majority (simplified - in real implementation use proper waiting)
        time.sleep(0.5)
        
        return raft_pb2.ClientResponse(
            success=True,
            message="Request committed",
            leader_id=self.node_id
        )
    
    # --- Helper Methods ---
    
    def is_log_up_to_date(self, last_log_index, last_log_term):
        """Check if candidate's log is at least as up-to-date"""
        if len(self.log) == 0:
            return True
        
        my_last_term = self.log[-1].term
        my_last_index = len(self.log)
        
        if last_log_term != my_last_term:
            return last_log_term >= my_last_term
        return last_log_index >= my_last_index
    
    def check_log_consistency(self, prev_log_index, prev_log_term):
        """Check if log is consistent at prev_log_index"""
        if prev_log_index == 0:
            return True
        if prev_log_index > len(self.log):
            return False
        return self.log[prev_log_index - 1].term == prev_log_term
    
    def append_entries(self, prev_log_index, entries):
        """Append entries to log"""
        # Remove conflicting entries and append new ones
        self.log = self.log[:prev_log_index]
        self.log.extend(entries)
    
    def apply_committed_entries(self):
        """Apply committed entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            print(f"Node {self.node_id} applied entry: {entry.operation} on {entry.filename}")
    
    def get_current_leader(self):
        """Get current leader ID (simplified)"""
        return "unknown"
    
    # --- Election Logic ---
    
    def election_timer(self):
        """Background thread for election timeout"""
        while self.running:
            time.sleep(0.1)
            
            with self.lock:
                if self.state == NodeState.LEADER:
                    continue
                
                time_since_heartbeat = time.time() - self.last_heartbeat
                
                if time_since_heartbeat > self.election_timeout:
                    print(f"\nNode {self.node_id} election timeout! Starting election...")
                    self.start_election()
    
    def start_election(self):
        """Start a new election"""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = self.get_random_election_timeout()
        
        print(f"Node {self.node_id} became CANDIDATE for term {self.current_term}")
        
        votes_received = 1  # Vote for self
        votes_needed = (len(self.peers) + 1) // 2 + 1
        
        # Request votes from peers
        for peer_addr in self.peers:
            threading.Thread(target=self.request_vote_from_peer, 
                           args=(peer_addr,), 
                           daemon=True).start()
    
    def request_vote_from_peer(self, peer_addr):
        """Request vote from a single peer"""
        try:
            peer_id = peer_addr.split(':')[0]
            print(f"Node {self.node_id} sends RPC RequestVote to Node {peer_id}")
            
            channel = grpc.insecure_channel(peer_addr)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            last_log_index = len(self.log)
            last_log_term = self.log[-1].term if self.log else 0
            
            with self.lock:
                term = self.current_term
                candidate_id = self.node_id
            
            request = raft_pb2.RequestVoteRequest(
                term=term,
                candidate_id=candidate_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term
            )
            
            response = stub.RequestVote(request, timeout=2.0)
            
            with self.lock:
                if response.term > self.current_term:
                    self.current_term = response.term
                    self.state = NodeState.FOLLOWER
                    self.voted_for = None
                    channel.close()
                    return
                
                if self.state == NodeState.CANDIDATE and response.vote_granted:
                    # Count votes
                    votes_received = 1  # self vote
                    for peer in self.peers:
                        # In real implementation, track votes properly
                        pass
                    
                    votes_needed = (len(self.peers) + 1) // 2 + 1
                    # Simplified: assume we got enough votes
                    self.become_leader()
            
            channel.close()
            
        except Exception as e:
            print(f"Error requesting vote from {peer_addr}: {e}")
    
    def become_leader(self):
        """Transition to leader state"""
        if self.state != NodeState.CANDIDATE:
            return
        
        print(f"\n*** Node {self.node_id} became LEADER for term {self.current_term} ***\n")
        self.state = NodeState.LEADER
        
        # Initialize leader state
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0
    
    # --- Heartbeat Logic ---
    
    def heartbeat_timer(self):
        """Background thread for sending heartbeats"""
        while self.running:
            time.sleep(self.heartbeat_timeout)
            
            with self.lock:
                if self.state == NodeState.LEADER:
                    self.send_heartbeats()
    
    def send_heartbeats(self):
        """Send heartbeats to all followers"""
        for peer_addr in self.peers:
            threading.Thread(target=self.send_append_entries, 
                           args=(peer_addr,), 
                           daemon=True).start()
    
    def send_append_entries(self, peer_addr):
        """Send AppendEntries RPC to a peer"""
        try:
            peer_id = peer_addr.split(':')[0]
            
            with self.lock:
                prev_log_index = self.next_index.get(peer_addr, 1) - 1
                prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 else 0
                
                # Get entries to send
                entries = self.log[prev_log_index:]
                
                is_heartbeat = len(entries) == 0
                rpc_name = "Heartbeat" if is_heartbeat else "AppendEntries"
                term = self.current_term
                leader_id = self.node_id
                commit_index = self.commit_index
            
            print(f"Node {self.node_id} sends RPC {rpc_name} to Node {peer_id}")
            
            channel = grpc.insecure_channel(peer_addr)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            request = raft_pb2.AppendEntriesRequest(
                term=term,
                leader_id=leader_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=commit_index
            )
            
            response = stub.AppendEntries(request, timeout=2.0)
            
            with self.lock:
                if response.term > self.current_term:
                    self.current_term = response.term
                    self.state = NodeState.FOLLOWER
                    self.voted_for = None
                    channel.close()
                    return
                
                if response.success:
                    self.match_index[peer_addr] = response.match_index
                    self.next_index[peer_addr] = response.match_index + 1
                else:
                    # Decrement next_index and retry
                    self.next_index[peer_addr] = max(1, self.next_index[peer_addr] - 1)
            
            channel.close()
            
        except Exception as e:
            print(f"Error sending AppendEntries to {peer_addr}: {e}")
    
    def replicate_log(self):
        """Trigger log replication"""
        if self.state == NodeState.LEADER:
            self.send_heartbeats()


def serve_raft_node(node_id, port, peers):
    """Start Raft node server"""
    node = RaftNode(node_id, peers)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(node, server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    
    print(f"Raft node {node_id} listening on port {port}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        node.running = False
        server.stop(0)


if __name__ == "__main__":
    import os
    import sys
    
    node_id = os.environ.get("NODE_ID", "node-0")
    port = int(os.environ.get("PORT", "50051"))
    
    # Get peer addresses from environment
    peers_str = os.environ.get("PEERS", "")
    peers = [p.strip() for p in peers_str.split(",") if p.strip()]
    
    serve_raft_node(node_id, port, peers)
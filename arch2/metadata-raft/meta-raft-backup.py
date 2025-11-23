import grpc
import time
import random
import threading

import json
from concurrent import futures
from enum import Enum
import os
import raft_pb2
import raft_pb2_grpc

import metadata_pb2
import metadata_pb2_grpc

class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers
        
        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of LogEntry
        
        # Volatile state
        self.commit_index = 0  # Index of highest log entry known to be committed
        self.last_applied = 0  # Index of highest log entry applied to state machine
        self.state = NodeState.FOLLOWER
        self.current_leader = None  # Track who we think the leader is
        
        # Leader volatile state
        self.next_index = {}
        self.match_index = {}
        
        # Timing
        self.last_heartbeat = time.time()
        self.election_timeout = self.get_random_election_timeout()
        self.heartbeat_timeout = 1.0
        
        # Threading
        self.lock = threading.Lock()
        self.running = True
        self.votes_received = 0
        
        # Track acknowledgments for log replication
        self.ack_count = {}  # {log_index: count}
        
        # Start background threads
        threading.Thread(target=self.election_timer, daemon=True).start()
        threading.Thread(target=self.heartbeat_timer, daemon=True).start()
        
        print(f"Node {self.node_id} initialized as FOLLOWER")
        print(f"Node {self.node_id} commit_index=0, last_applied=0")
    
    def get_random_election_timeout(self):
        return random.uniform(1.5, 3.0)
    
    # --- RPC Handlers ---
    
    def RequestVote(self, request, context):
        print(f"\nNode {self.node_id} runs RPC RequestVote called by Node {request.candidate_id}")
        
        with self.lock:
            vote_granted = False
            
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.state = NodeState.FOLLOWER
            
            if request.term == self.current_term:
                if self.voted_for is None or self.voted_for == request.candidate_id:
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
        is_heartbeat = len(request.entries) == 0
        rpc_type = "Heartbeat" if is_heartbeat else "AppendEntries"
        
        print(f"\nNode {self.node_id} runs RPC {rpc_type} called by Node {request.leader_id}")
        
        with self.lock:
            success = False
            
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = NodeState.FOLLOWER
                self.voted_for = None
            
            if request.term == self.current_term:
                self.state = NodeState.FOLLOWER
                self.current_leader = request.leader_id  # Remember who the leader is!
                self.last_heartbeat = time.time()
                
                if self.check_log_consistency(request.prev_log_index, request.prev_log_term):
                    success = True
                    
                    if len(request.entries) > 0:
                        self.append_entries(request.prev_log_index, request.entries)
                        print(f"Node {self.node_id} appended {len(request.entries)} entries to log")
                        print(f"Node {self.node_id} log now has {len(self.log)} entries")
                    
                    # Update commit index based on leader's commit
                    if request.leader_commit > self.commit_index:
                        old_commit = self.commit_index
                        self.commit_index = min(request.leader_commit, len(self.log))
                        print(f"Node {self.node_id} updated commit_index from {old_commit} to {self.commit_index}")
                        self.apply_committed_entries()
            
            match_idx = len(self.log) if success else 0
            
            return raft_pb2.AppendEntriesResponse(
                term=self.current_term,
                success=success,
                follower_id=self.node_id,
                match_index=match_idx
            )
    
    def ClientRequest(self, request, context):
        print(f"\nNode {self.node_id} received client request: {request.operation} on {request.filename}")
        
        # Quick check without lock first
        if self.state != NodeState.LEADER:
            leader_hint = self.get_current_leader()
            print(f"Node {self.node_id} is not leader, redirecting to {leader_hint}")
            return raft_pb2.ClientResponse(
                success=False,
                message="Not the leader",
                leader_id=leader_hint
            )
        
        # Handle test/ping operations immediately
        if request.operation in ["test", "ping", ""]:
            return raft_pb2.ClientResponse(
                success=True,
                message="I am the leader",
                leader_id=self.node_id
            )
        
        with self.lock:
            # Double-check we're still leader
            if self.state != NodeState.LEADER:
                leader_hint = self.get_current_leader()
                return raft_pb2.ClientResponse(
                    success=False,
                    message="Not the leader",
                    leader_id=leader_hint
                )
            
            # Create new log entry
            new_entry = raft_pb2.LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                operation=request.operation,
                filename=request.filename,
                username=request.username,
                data=request.data
            )
            self.log.append(new_entry)
            log_index = len(self.log)
            
            print(f"Leader {self.node_id} appended entry to log at index {log_index}")
            print(f"Leader {self.node_id} log entry: <{request.operation}, term={self.current_term}, index={log_index}>")
            
            # Initialize ack count for this entry
            self.ack_count[log_index] = 1  # Count self
        
        # Replicate to followers immediately
        print(f"Leader {self.node_id} replicating to followers...")
        self.replicate_log()
        
        # Wait for majority acknowledgment
        majority = (len(self.peers) + 1) // 2 + 1
        timeout = 5.0  # Increased timeout
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self.lock:
                if self.ack_count.get(log_index, 1) >= majority:
                    # Got majority, commit the entry
                    old_commit = self.commit_index
                    self.commit_index = log_index
                    print(f"Leader {self.node_id} received majority ACKs ({self.ack_count[log_index]}/{len(self.peers)+1})")
                    print(f"Leader {self.node_id} updated commit_index from {old_commit} to {self.commit_index}")
                    self.apply_committed_entries()
                    
                    return raft_pb2.ClientResponse(
                        success=True,
                        message=f"Request committed at index {log_index}",
                        leader_id=self.node_id
                    )
            time.sleep(0.1)
        
        # Timeout - but still return success as entry is in log
        print(f"Leader {self.node_id} timeout waiting for majority (got {self.ack_count.get(log_index, 1)}/{len(self.peers)+1})")
        return raft_pb2.ClientResponse(
            success=True,
            message=f"Entry added to log at index {log_index}, waiting for replication",
            leader_id=self.node_id
        )
    
    # --- Helper Methods ---
    
    def is_log_up_to_date(self, last_log_index, last_log_term):
        if len(self.log) == 0:
            return True
        my_last_term = self.log[-1].term
        my_last_index = len(self.log)
        if last_log_term != my_last_term:
            return last_log_term >= my_last_term
        return last_log_index >= my_last_index
    
    def check_log_consistency(self, prev_log_index, prev_log_term):
        if prev_log_index == 0:
            return True
        if prev_log_index > len(self.log):
            return False
        return self.log[prev_log_index - 1].term == prev_log_term
    
    def append_entries(self, prev_log_index, entries):
        # Remove conflicting entries and append new ones
        self.log = self.log[:prev_log_index]
        for entry in entries:
            self.log.append(entry)
    
    def apply_committed_entries(self):
        """Apply committed entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            print(f"Node {self.node_id} executed operation: {entry.operation} on {entry.filename} (index={entry.index})")
    
    def get_current_leader(self):
        # Return the leader we've been receiving heartbeats from
        if self.current_leader:
            # Return full address with port
            for peer in self.peers:
                if self.current_leader in peer:
                    return peer
            return f"{self.current_leader}:50051"
        # If we don't know, return first peer as a hint
        if len(self.peers) > 0:
            return self.peers[0]
        return "raft-node-0:50051"
    
    # --- Election Logic ---
    
    def election_timer(self):
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
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = self.get_random_election_timeout()
        self.votes_received = 1
        
        print(f"Node {self.node_id} became CANDIDATE for term {self.current_term}")
        
        for peer_addr in self.peers:
            threading.Thread(target=self.request_vote_from_peer, 
                           args=(peer_addr,), 
                           daemon=True).start()
    
    def request_vote_from_peer(self, peer_addr):
        try:
            peer_id = peer_addr.split(':')[0]
            print(f"Node {self.node_id} sends RPC RequestVote to Node {peer_id}")
            
            channel = grpc.insecure_channel(peer_addr)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            with self.lock:
                last_log_index = len(self.log)
                last_log_term = self.log[-1].term if self.log else 0
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
                    self.votes_received += 1
                    votes_needed = (len(self.peers) + 1) // 2 + 1
                    
                    if self.votes_received >= votes_needed:
                        self.become_leader()
            
            channel.close()
            
        except Exception as e:
            pass  # Peer might be down
    
    def become_leader(self):
        if self.state != NodeState.CANDIDATE:
            return
        
        print(f"\n*** Node {self.node_id} became LEADER for term {self.current_term} ***\n")
        self.state = NodeState.LEADER
        
        # Initialize leader state
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0
        
        # Send immediate heartbeat
        self.send_heartbeats()
    
    # --- Heartbeat Logic ---
    
    def heartbeat_timer(self):
        while self.running:
            time.sleep(self.heartbeat_timeout)
            with self.lock:
                if self.state == NodeState.LEADER:
                    self.send_heartbeats()
    
    def send_heartbeats(self):
        for peer_addr in self.peers:
            threading.Thread(target=self.send_append_entries, 
                           args=(peer_addr,), 
                           daemon=True).start()
    
    def send_append_entries(self, peer_addr):
        try:
            peer_id = peer_addr.split(':')[0]
            
            with self.lock:
                prev_log_index = self.next_index.get(peer_addr, 1) - 1
                prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 and prev_log_index <= len(self.log) else 0
                
                # Get entries to send
                entries = list(self.log[prev_log_index:])
                
                is_heartbeat = len(entries) == 0
                rpc_name = "Heartbeat" if is_heartbeat else "AppendEntries"
                term = self.current_term
                leader_id = self.node_id
                commit_index = self.commit_index
            
            print(f"Node {self.node_id} sends RPC {rpc_name} to Node {peer_id} (commit_index={commit_index})")
            
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
                    
                    # Count ACKs for uncommitted entries
                    for i in range(prev_log_index + 1, response.match_index + 1):
                        if i > self.commit_index:
                            self.ack_count[i] = self.ack_count.get(i, 1) + 1
                else:
                    self.next_index[peer_addr] = max(1, self.next_index.get(peer_addr, 1) - 1)
            
            channel.close()
            
        except Exception as e:
            pass  # Peer might be down
    
    def replicate_log(self):
        if self.state == NodeState.LEADER:
            self.send_heartbeats()


def serve_raft_node(node_id, port, peers):
    node = RaftNode(node_id, peers)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(node, server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    
    print(f"Raft node {node_id} listening on port {port}")
    print(f"Peers: {peers}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        node.running = False
        server.stop(0)

##################################################################################################################################
# Add metadata storage capabilities

class MetadataRaftNode(raft_pb2_grpc.RaftServiceServicer, 
                       metadata_pb2_grpc.MetadataServiceServicer):
    def __init__(self, node_id, peers):
        # Raft state
        self.node_id = node_id
        self.peers = peers
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.state = NodeState.FOLLOWER
        self.current_leader = None
        
        # Metadata storage (in-memory, like your original)
        self.files = {}
        self.users = {}
        
        # ... (rest of Raft initialization)
        
        # Start Raft threads
        threading.Thread(target=self.election_timer, daemon=True).start()
        threading.Thread(target=self.heartbeat_timer, daemon=True).start()
        
        print(f"Metadata-Raft Node {self.node_id} initialized")
    
    # ===== RAFT RPCs (copy from your raft_node.py) =====
    def RequestVote(self, request, context):
        # ... your existing code
        pass
    
    def AppendEntries(self, request, context):
        # ... your existing code
        pass
    
    def ClientRequest(self, request, context):
        # ... your existing code
        pass
    
    # ===== METADATA RPCs (new - these use Raft internally) =====
    
    def AddFile(self, request, context):
        """Add file metadata via Raft consensus"""
        print(f"Node {self.node_id} received AddFile request for {request.filename}")
        
        # If not leader, redirect
        if self.state != NodeState.LEADER:
            return metadata_pb2.AddFileResponse(
                success=False,
                message=f"Not leader, try {self.current_leader}"
            )
        
        # Convert to JSON for Raft log
        file_data = {
            'filename': request.filename,
            'path': request.path,
            'size': request.size,
            'version': request.version,
            'user': request.user,
            'password': request.password
        }
        
        # Submit to Raft
        raft_request = raft_pb2.ClientRequestMsg(
            operation="add_file",
            filename=request.filename,
            username=request.user,
            data=json.dumps(file_data).encode('utf-8')
        )
        
        # Use internal Raft consensus (same logic as ClientRequest)
        success = self.replicate_via_raft(raft_request)
        
        if success:
            return metadata_pb2.AddFileResponse(
                success=True,
                message="File metadata added",
                metadata=metadata_pb2.FileMetadata(**file_data)
            )
        else:
            return metadata_pb2.AddFileResponse(
                success=False,
                message="Failed to replicate"
            )
    
    def GetFile(self, request, context):
        """Get file metadata (read from local state)"""
        if request.filename in self.files:
            file_data = self.files[request.filename]
            return metadata_pb2.GetFileResponse(
                success=True,
                metadata=metadata_pb2.FileMetadata(**file_data)
            )
        else:
            return metadata_pb2.GetFileResponse(success=False)
    
    def DeleteFile(self, request, context):
        """Delete file metadata via Raft consensus"""
        if self.state != NodeState.LEADER:
            return metadata_pb2.DeleteFileResponse(
                success=False,
                message=f"Not leader, try {self.current_leader}"
            )
        
        # Submit to Raft
        raft_request = raft_pb2.ClientRequestMsg(
            operation="delete_file",
            filename=request.filename,
            username="",
            data=b""
        )
        
        success = self.replicate_via_raft(raft_request)
        
        return metadata_pb2.DeleteFileResponse(
            success=success,
            message="Deleted" if success else "Failed"
        )
    
    def ListFiles(self, request, context):
        """List all files (read from local state)"""
        files_list = [
            metadata_pb2.FileMetadata(**file_data)
            for file_data in self.files.values()
        ]
        return metadata_pb2.ListFilesResponse(files=files_list)
    
    def AddUser(self, request, context):
        """Add user via Raft consensus"""
        if self.state != NodeState.LEADER:
            return metadata_pb2.AddUserResponse(
                success=False,
                message=f"Not leader"
            )
        
        user_data = {
            'username': request.username,
            'password': request.password
        }
        
        raft_request = raft_pb2.ClientRequestMsg(
            operation="add_user",
            filename="",
            username=request.username,
            data=json.dumps(user_data).encode('utf-8')
        )
        
        success = self.replicate_via_raft(raft_request)
        
        return metadata_pb2.AddUserResponse(
            success=success,
            message="User added" if success else "Failed"
        )
    
    def GetUser(self, request, context):
        """Get user (read from local state)"""
        if request.username in self.users:
            return metadata_pb2.GetUserResponse(
                success=True,
                username=request.username,
                password=self.users[request.username]
            )
        else:
            return metadata_pb2.GetUserResponse(success=False)
    
    # ===== RAFT STATE MACHINE =====
    
    def apply_committed_entries(self):
        """Apply committed Raft entries to metadata store"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            
            try:
                data = json.loads(entry.data.decode('utf-8')) if entry.data else {}
                
                if entry.operation == "add_file":
                    self.files[entry.filename] = data
                    print(f"Node {self.node_id} applied: ADD_FILE {entry.filename}")
                
                elif entry.operation == "delete_file":
                    if entry.filename in self.files:
                        del self.files[entry.filename]
                    print(f"Node {self.node_id} applied: DELETE_FILE {entry.filename}")
                
                elif entry.operation == "add_user":
                    self.users[entry.username] = data['password']
                    print(f"Node {self.node_id} applied: ADD_USER {entry.username}")
                
            except Exception as e:
                print(f"Error applying entry: {e}")
    
    def replicate_via_raft(self, raft_request):
        """Use Raft consensus to replicate an operation"""
        # This is the same logic as your ClientRequest RPC
        # Returns True if majority ACKed, False otherwise
        
        with self.lock:
            new_entry = raft_pb2.LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                operation=raft_request.operation,
                filename=raft_request.filename,
                username=raft_request.username,
                data=raft_request.data
            )
            self.log.append(new_entry)
            log_index = len(self.log)
            self.ack_count[log_index] = 1
        
        # Replicate
        self.replicate_log()
        
        # Wait for majority
        majority = (len(self.peers) + 1) // 2 + 1
        timeout = 5.0
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self.lock:
                if self.ack_count.get(log_index, 1) >= majority:
                    self.commit_index = log_index
                    self.apply_committed_entries()
                    return True
            time.sleep(0.1)
        
        return False

def serve_metadata_raft_node(node_id, port, peers):
    """Start Metadata-Raft node server"""
    node = MetadataRaftNode(node_id, peers)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Register BOTH services
    raft_pb2_grpc.add_RaftServiceServicer_to_server(node, server)
    metadata_pb2_grpc.add_MetadataServiceServicer_to_server(node, server)
    
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    
    print(f"Metadata-Raft node {node_id} listening on port {port}")
    print(f"Peers: {peers}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        node.running = False
        server.stop(0)


if __name__ == "__main__":
    import os
    
    node_id = os.environ.get("NODE_ID", "metadata-0")
    port = int(os.environ.get("PORT", "50051"))
    
    peers_str = os.environ.get("PEERS", "")
    peers = [p.strip() for p in peers_str.split(",") if p.strip()]
    
    serve_metadata_raft_node(node_id, port, peers)


    ###################################################gateway############################################################

    from flask import Flask, request, jsonify
import grpc
import metadata_pb2
import metadata_pb2_grpc
import os

app = Flask(__name__)

# List of metadata-raft nodes
METADATA_NODES = os.environ.get("METADATA_NODES", "").split(",")
current_node_index = 0

def get_metadata_stub():
    """Get gRPC stub, cycling through nodes to find leader"""
    global current_node_index
    
    for i in range(len(METADATA_NODES)):
        node_addr = METADATA_NODES[current_node_index]
        current_node_index = (current_node_index + 1) % len(METADATA_NODES)
        
        try:
            channel = grpc.insecure_channel(node_addr)
            stub = metadata_pb2_grpc.MetadataServiceStub(channel)
            return stub, channel
        except:
            continue
    
    return None, None

# ---------------- Add / Upload Metadata ----------------
@app.route("/files", methods=["POST"])
def add_file():
    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON body required"}), 400

    filename = data.get("filename")
    if not filename:
        return jsonify({"error": "Filename is required"}), 400

    stub, channel = get_metadata_stub()
    if not stub:
        return jsonify({"error": "Cannot connect to metadata cluster"}), 503

    try:
        grpc_request = metadata_pb2.AddFileRequest(
            filename=filename,
            path=data.get("path", ""),
            size=data.get("size", 0),
            version=data.get("version", 1),
            user=data.get("user", ""),
            password=data.get("password", "")
        )
        
        response = stub.AddFile(grpc_request, timeout=5.0)
        channel.close()
        
        if response.success:
            return jsonify({
                "filename": filename,
                "path": data.get("path"),
                "size": data.get("size"),
                "version": data.get("version", 1),
                "user": data.get("user"),
                "password": data.get("password", "")
            }), 201
        else:
            return jsonify({"error": response.message}), 500
            
    except grpc.RpcError as e:
        channel.close()
        return jsonify({"error": str(e)}), 500

# ---------------- Get Metadata ----------------
@app.route("/files/<filename>", methods=["GET"])
def get_file(filename):
    stub, channel = get_metadata_stub()
    if not stub:
        return jsonify({"error": "Cannot connect to metadata cluster"}), 503

    try:
        grpc_request = metadata_pb2.GetFileRequest(filename=filename)
        response = stub.GetFile(grpc_request, timeout=2.0)
        channel.close()
        
        if response.success:
            return jsonify({
                "filename": response.metadata.filename,
                "path": response.metadata.path,
                "size": response.metadata.size,
                "version": response.metadata.version,
                "user": response.metadata.user,
                "password": response.metadata.password
            }), 200
        else:
            return jsonify({"error": "File not found"}), 404
            
    except grpc.RpcError as e:
        channel.close()
        return jsonify({"error": str(e)}), 500

# ---------------- Delete Metadata ----------------
@app.route("/files/<filename>", methods=["DELETE"])
def delete_file(filename):
    stub, channel = get_metadata_stub()
    if not stub:
        return jsonify({"error": "Cannot connect to metadata cluster"}), 503

    try:
        grpc_request = metadata_pb2.DeleteFileRequest(filename=filename)
        response = stub.DeleteFile(grpc_request, timeout=5.0)
        channel.close()
        
        if response.success:
            return jsonify({"status": "deleted"}), 200
        else:
            return jsonify({"error": response.message}), 500
            
    except grpc.RpcError as e:
        channel.close()
        return jsonify({"error": str(e)}), 500

# ---------------- List All Files ----------------
@app.route("/files", methods=["GET"])
def list_files():
    stub, channel = get_metadata_stub()
    if not stub:
        return jsonify({"error": "Cannot connect to metadata cluster"}), 503

    try:
        grpc_request = metadata_pb2.ListFilesRequest()
        response = stub.ListFiles(grpc_request, timeout=2.0)
        channel.close()
        
        files_list = [
            {
                "filename": f.filename,
                "path": f.path,
                "size": f.size,
                "version": f.version,
                "user": f.user,
                "password": f.password
            }
            for f in response.files
        ]
        
        return jsonify(files_list), 200
            
    except grpc.RpcError as e:
        channel.close()
        return jsonify({"error": str(e)}), 500

# ---------------- User Registration ----------------
@app.route("/users", methods=["POST"])
def add_user():
    data = request.get_json()
    username = data.get("username")
    password = data.get("password")

    print(f"!!! GATEWAY RECEIVED: username={username}")  # debugging
    
    if not username or not password:
        return jsonify({"error": "Missing username or password"}), 400
    
    for i in range(len(METADATA_NODES)):
        stub, channel = get_metadata_stub()
        if not stub:
            continue
    # stub, channel = get_metadata_stub()
    if not stub:
        print(f"!!! GATEWAY: Cannot connect to metadata cluster")  # debugging
        return jsonify({"error": "Cannot connect to metadata cluster"}), 503

    try:
        grpc_request = metadata_pb2.AddUserRequest(
            username=username,
            password=password
        )

        print(f"!!! GATEWAY: Sending gRPC AddUser for {username}")

        response = stub.AddUser(grpc_request, timeout=5.0)
        channel.close()
        
        if response.success:
            return jsonify({"message": "User created"}), 201
        
        else:
            return jsonify({"error": response.message}), 409
            
    except grpc.RpcError as e:
        channel.close()
        print(f"!!! GATEWAY: gRPC error - {e}")  # ADD THIS

        return jsonify({"error": str(e)}), 500

# ---------------- Get User for Login ----------------
@app.route("/users/<username>", methods=["GET"])
def get_user(username):
    stub, channel = get_metadata_stub()
    if not stub:
        return jsonify({"error": "Cannot connect to metadata cluster"}), 503

    try:
        grpc_request = metadata_pb2.GetUserRequest(username=username)
        response = stub.GetUser(grpc_request, timeout=2.0)
        channel.close()
        
        if response.success:
            return jsonify({
                "username": response.username,
                "password": response.password
            }), 200
        else:
            return jsonify({"error": "User not found"}), 404
            
    except grpc.RpcError as e:
        channel.close()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5005, debug=True)
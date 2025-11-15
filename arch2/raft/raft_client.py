# client for testing

import grpc
import sys
import time
import os
import raft_pb2
import raft_pb2_grpc

class RaftClient:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses
        self.current_leader = None
    
    def find_leader(self):
        """Try each node to find the current leader"""
        for addr in self.node_addresses:
            try:
                channel = grpc.insecure_channel(addr)
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                
                # Send a dummy request to check if it's the leader
                request = raft_pb2.ClientRequestMsg(
                    operation="ping",
                    filename="",
                    username="",
                    data=b""
                )
                
                response = stub.ClientRequest(request, timeout=2.0)
                
                if response.success or response.leader_id:
                    self.current_leader = addr if response.success else response.leader_id
                    channel.close()
                    return self.current_leader
                
                channel.close()
                
            except Exception as e:
                continue
        
        return None
    
    def send_request(self, operation, filename, username="testuser", data=b""):
        """Send a client request to the Raft cluster"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            # Find leader if we don't have one
            if not self.current_leader:
                print("Finding leader...")
                self.current_leader = self.find_leader()
                if not self.current_leader:
                    print("Error: Could not find leader")
                    return None
            
            try:
                print(f"\nSending request to {self.current_leader}")
                print(f"Operation: {operation}")
                print(f"Filename: {filename}")
                
                channel = grpc.insecure_channel(self.current_leader)
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                
                request = raft_pb2.ClientRequestMsg(
                    operation=operation,
                    filename=filename,
                    username=username,
                    data=data
                )
                
                response = stub.ClientRequest(request, timeout=10.0)
                
                if response.success:
                    print(f"\n✓ Request successful!")
                    print(f"Message: {response.message}")
                    channel.close()
                    return response
                else:
                    print(f"\n✗ Request failed: {response.message}")
                    if response.leader_id:
                        print(f"Redirected to leader: {response.leader_id}")
                        self.current_leader = response.leader_id
                        retry_count += 1
                        continue
                    channel.close()
                    return None
                
            except grpc.RpcError as e:
                print(f"RPC Error: {e}")
                self.current_leader = None
                retry_count += 1
                time.sleep(1)
            except Exception as e:
                print(f"Error: {e}")
                self.current_leader = None
                retry_count += 1
                time.sleep(1)
        
        print("Max retries reached")
        return None
    
    def upload_file(self, filename):
        """Upload a file"""
        # In real implementation, read file data
        data = f"Content of {filename}".encode('utf-8')
        return self.send_request("upload", filename, data=data)
    
    def download_file(self, filename):
        """Download a file"""
        response = self.send_request("download", filename)
        if response and response.data:
            print(f"Downloaded data: {response.data.decode('utf-8')}")
        return response
    
    def delete_file(self, filename):
        """Delete a file"""
        return self.send_request("delete", filename)
    
    def list_files(self):
        """List all files"""
        return self.send_request("list", "")


def interactive_mode(client):
    """Interactive CLI for testing Raft"""
    print("\n" + "="*50)
    print("Raft Client - Interactive Mode")
    print("="*50)
    print("\nCommands:")
    print("  upload <filename>   - Upload a file")
    print("  download <filename> - Download a file")
    print("  delete <filename>   - Delete a file")
    print("  list                - List all files")
    print("  leader              - Find current leader")
    print("  quit                - Exit")
    print("="*50 + "\n")
    
    while True:
        try:
            cmd = input("raft> ").strip().split()
            
            if not cmd:
                continue
            
            if cmd[0] == "quit":
                break
            
            elif cmd[0] == "upload":
                if len(cmd) < 2:
                    print("Usage: upload <filename>")
                    continue
                client.upload_file(cmd[1])
            
            elif cmd[0] == "download":
                if len(cmd) < 2:
                    print("Usage: download <filename>")
                    continue
                client.download_file(cmd[1])
            
            elif cmd[0] == "delete":
                if len(cmd) < 2:
                    print("Usage: delete <filename>")
                    continue
                client.delete_file(cmd[1])
            
            elif cmd[0] == "list":
                client.list_files()
            
            elif cmd[0] == "leader":
                leader = client.find_leader()
                if leader:
                    print(f"Current leader: {leader}")
                else:
                    print("No leader found")
            
            else:
                print(f"Unknown command: {cmd[0]}")
        
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    # Get node addresses from environment or command line
    nodes_env = os.environ.get("RAFT_NODES", "")
    
    if nodes_env:
        node_addresses = [n.strip() for n in nodes_env.split(",") if n.strip()]
    elif len(sys.argv) > 1:
        node_addresses = sys.argv[1:]
    else:
        # Default addresses
        node_addresses = [
            "raft-node-0:50051",
            "raft-node-1:50051",
            "raft-node-2:50051",
            "raft-node-3:50051",
            "raft-node-4:50051"
        ]
    
    print(f"Connecting to Raft cluster:")
    for addr in node_addresses:
        print(f"  - {addr}")
    
    client = RaftClient(node_addresses)
    
    # Check if command line operation provided
    if len(sys.argv) > 2:
        operation = sys.argv[1]
        filename = sys.argv[2] if len(sys.argv) > 2 else ""
        
        if operation == "upload":
            client.upload_file(filename)
        elif operation == "download":
            client.download_file(filename)
        elif operation == "delete":
            client.delete_file(filename)
        elif operation == "list":
            client.list_files()
        else:
            print(f"Unknown operation: {operation}")
    else:
        # Interactive mode
        interactive_mode(client)


if __name__ == "__main__":
    main()
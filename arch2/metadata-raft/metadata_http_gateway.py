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
            
            # Test the connection with a timeout
            channel_ready = grpc.channel_ready_future(channel)
            channel_ready.result(timeout=1)  # Wait max 1 second
            
            return stub, channel
        except Exception as e:
            print(f"!!! Failed to connect to {node_addr}: {e}")
            if channel:
                channel.close()
            continue
    
    print(f"!!! Could not connect to any metadata node")
    return None, None

# ---------------- Add / Upload Metadata ----------------
@app.route("/files", methods=["POST"])
def add_file():
    print("Attempting to add file to metadata node...")
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
            print("File Successfully added to metadata...")
            return jsonify({
                "filename": filename,
                "path": data.get("path"),
                "size": data.get("size"),
                "version": data.get("version", 1),
                "user": data.get("user"),
                "password": data.get("password", "")
            }), 201
        else:
            print("File Failed to save to metadata...")
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
    print("In gateway delete mode...fetching metadata stub")
    stub, channel = get_metadata_stub()
    if not stub:
        return jsonify({"error": "Cannot connect to metadata cluster"}), 503

    try:
        grpc_request = metadata_pb2.DeleteFileRequest(filename=filename)
        response = stub.DeleteFile(grpc_request, timeout=5.0)
        channel.close()
        
        if response.success:
            print("Delete request successful")
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
    
    
    stub, channel = get_metadata_stub()
    if not stub:
        print(f"!!! GATEWAY: Cannot connect to metadata cluster")  # debugging
        return jsonify({"error": "Cannot connect to metadata cluster"}), 503

    try:
        grpc_request = metadata_pb2.AddUserRequest(
            username=username,
            password=password
        )

        print(f"!!! GATEWAY: Sending gRPC AddUser for {username}")

        response = stub.AddUser(grpc_request, timeout=10.0)
        print(f"!!! GATEWAY: Got response - success={response.success}, message={response.message}")

        if not response.success and hasattr(response, 'leader_id') and response.leader_id:
            # Retry with the actual leader
            print(f"!!! GATEWAY: Not leader, retrying with {response.leader_id}")
            channel.close()
            channel = grpc.insecure_channel(f"{response.leader_id}:50051")
            stub = metadata_pb2_grpc.MetadataServiceStub(channel)
            response = stub.AddUser(grpc_request, timeout=10.0)
            print(f"!!! GATEWAY: Leader response - success={response.success}")
            channel.close()
        
        if response.success:
            return jsonify({"message": "User created"}), 201
        
        else:
            return jsonify({"error": response.message}), 409
            
    except grpc.RpcError as e:
        channel.close()
        print(f"!!! GATEWAY: gRPC error - {e}")  

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
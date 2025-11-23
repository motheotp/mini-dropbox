FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto files and generate code
COPY raft.proto .
COPY metadata.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. metadata.proto

# Copy application code
COPY metadata_raft_node.py .

# Run the node
CMD ["python", "-u", "metadata_raft_node.py"]
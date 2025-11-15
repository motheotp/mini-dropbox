FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto file and generate code
COPY raft.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

# Copy application code
COPY raft_node.py .

# Run raft node
CMD ["python", "-u", "raft_node.py"]
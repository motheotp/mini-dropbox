# ☁️ Mini Dropbox Metadata Service – Raft Implementation

A **fault-tolerant metadata service** for a Dropbox-like system using the **Raft consensus algorithm**. This service manages file metadata across a distributed cluster, ensuring leader election, log replication, and failover for reliability.

---

## 📑 Table of Contents

* [Overview](#overview)
* [Architecture](#architecture)
* [Features](#features)
* [Prerequisites](#prerequisites)
* [Setup and Run](#setup-and-run)
* [Docker Compose](#docker-compose)
* [Running Tests](#running-tests)
* [Project Structure](#project-structure)
* [Leader Election & Failure Handling](#leader-election--failure-handling)
* [Contributing](#contributing)
* [License](#license)

---

## 🧭 Overview

This project implements a distributed **Metadata Service** at the core of the Mini Dropbox system, utilizing the **Raft consensus algorithm** for fault tolerance.

The cluster ensures:

* **Single Leader:** Only one leader manages updates at any time.
* **Automatic Re-election:** Automatic re-election is triggered in case of node failures.
* **Log Replication:** Replication of metadata logs to all followers.
* **High Availability:** Provides fault tolerance for a file storage system.

It is part of the larger Mini Dropbox project, which also includes storage, upload, download, client, and backup services.

---

## 📐 Architecture

The Metadata Service is integrated into the system as follows:

        +-------------------+
      |    Client         |
      +-------------------+
         |          |
         v          v
 +-----------+   +-----------+
 |  Upload   |   | Download  |
 +-----------+   +-----------+
         |          |
         v          v
  +-------------------+
  | Metadata Gateway  |
  +-------------------+
         |
 -------------------------
 |   Raft Metadata Cluster |
 |------------------------|
 | Node0 | Node1 | Node2 |
 | Node3 | Node4 |        |
 -------------------------
         |
       Storage


### Components
* **Metadata Gateway:** Single HTTP entry point for all services.
* **Raft Cluster:** 5-node consensus cluster handling metadata replication with gRPC support.
* **Storage Service:** Persists file contents (separate from metadata).

## ✨ Features

* **Raft Consensus:** Implementation of Raft for **leader election** and log **replication**.
* **Automatic Failover:** Automatic leader failure detection and re-election.
* **Heartbeat Mechanism:** Consistent heartbeat communication to maintain cluster health.
* **Dockerized Deployment:** Services are containerized for easy deployment.
* **Integration Tests:** Fully automated tests to validate core Raft operations.

---

## 🛠️ Prerequisites

* **Docker**  24.0
* **Docker Compose**  2.0
* **Python** 3.11
* **Git**

---

## 🚀 Setup and Run

### 1. Clone the repository

```bash
git clone https://github.com/motheotp/mini-dropbox.git
cd mini-dropbox/arch2

```

### 2. Build all services
```bash
docker-compose -f docker-compose-integrated.yml build --no-cache

##!! be sure to use docker-compose-integrated for raft support on metadata,
## and only use docker-compose if you want to run without raft implementation.

```

### 3. Start all services
```bash

docker-compose -f docker-compose-integrated.yml up -d
```

### 4. Check running services
```bash
docker-compose -f docker-compose-integrated.yml ps
```
### Runnnig the client - Accessing the shell for user interaction:

```bash
docker-compose -f docker-compose-integrated.yml run client sh
```
### Signing up and uploading file
```sh
python cli.py signup first_name last_name
python cli.py login first_name last_name
python cli.py upload file.txt
python cli.py list
python cli.py delete file.txt
```


### Docker Compose
## Compose File
docker-compose-integrated.yml

Service	                        Description	                                                Port
client	                        CLI client	                                                 -
upload	                        Handles file uploads	                                    5003
download	                    Handles file downloads	                                    5004
metadata-gateway	            Gateway to Raft cluster	                                    5005
metadata-raft-0..4	            5-Node Raft cluster nodes	                                50051
storage	                        File storage backend	                                    5006
backup	                        Backup service	                                             -


# Notes:

Node IDs are named metadata-raft-0 to metadata-raft-4.

Container names follow the pattern: arch2-metadata-raft-0-1.

### Running Tests

Automated integration tests are available in metadata-raft/raft_test_suite.py

Ensure all containers are up and running using above docker commands

```bash
python metadata-raft/raft_test_suite.py 1   # Test basic leader election
python metadata-raft/raft_test_suite.py 3   # Test leader failure and re-election
python metadata-raft/raft_test_suite.py     # runs all tests
```

## Key Test Cases
# 1      Basic Leader Election (with random timeout) – Verifies exactly one leader is elected.
#               Includes: Heartbeat Mechanism Verification – Ensures leader sends periodic heartbeats to followers (Every 1 second).

# 2       Log replication to all live nodes


# 3       Leader Failure & Re-election – Simulates a leader crash and validates re-election.


# 4       Network Partition Recovery - Log replication upon recovery


#  5       Multiple failures - Failure of more than one node, cluster continues to be available for client requests.

# Test cases included internally:
#       Client request handling with leader forwarding (all client requests are forwarded to leader.)
#       New node joining cluster ( joins as follower and updates entries missed by following leader instruction)


# Project Structure

arch2/
├── client/
├── services/
│   ├── upload/
│   └── download/
├── metadata-raft/
│   ├── Dockerfile
│   ├── Dockerfile.gateway
│   ├── raft_test_suite.py
│   └── ...                 # Raft implementation files
├── storage/
├── backup/
├── docker-compose-integrated.yml
└── README.md

# Inspecting Logs
        Election messages and heartbeat logs can be inspected via Docker logs
```bash
docker logs <container_name> --tail 50
```

# Contributing

    - Fork the repository.

    - Create a new branch (git checkout -b feature/xyz).

    - Commit changes and push.

    - Submit a pull request.




References:

References
1. Raft Consensus Algorithm (Primary)
Raft Paper (Extended Version): The core document detailing the algorithm, decomposition, and safety properties.
https://raft.github.io/raft.pdf
In Search of an Understandable Consensus Algorithm (Extended Version) by Ongaro & Ousterhout

2. Claude & ChatGpt

3. Youtube: https://www.youtube.com/watch?v=rVe_F_4H_60
The Raft Consensus Algorithm - Zymposium

License
This project is licensed under the MIT License—see LICENSE for details.
#!/usr/bin/env python3
"""
Helper script to capture logs and verify state during Raft testing
Updated for 5-node cluster with docker-compose-integrated.yml
"""

import subprocess
import time
import json
from datetime import datetime

# Docker compose configuration
DOCKER_COMPOSE = ["docker-compose", "-f", "docker-compose-integrated.yml"]

class LogCapture:
    def __init__(self):
        self.nodes = [
            "metadata-raft-0",
            "metadata-raft-1",
            "metadata-raft-2",
            "metadata-raft-3",
            "metadata-raft-4"
        ]
        self.gateway = "metadata-gateway"
        self.log_dir = "test_logs"
        subprocess.run(["mkdir", "-p", self.log_dir], capture_output=True)
    
    def capture_all_logs(self, test_name: str, step: str):
        """Capture logs from all nodes"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print(f"\n📝 Capturing logs for: {test_name} - {step}")
        
        for node in self.nodes:
            filename = f"{self.log_dir}/{test_name}_{step}_{node}_{timestamp}.log"
            
            try:
                result = subprocess.run(
                    ["docker", "logs", "--timestamps", "--tail", "100", node],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                
                with open(filename, 'w') as f:
                    f.write(f"=== Logs for {node} ===\n")
                    f.write(f"Test: {test_name}\n")
                    f.write(f"Step: {step}\n")
                    f.write(f"Captured: {timestamp}\n")
                    f.write("="*80 + "\n\n")
                    f.write(result.stdout)
                    f.write(result.stderr)
                
                print(f"   ✓ {node} logs saved to {filename}")
                
            except Exception as e:
                print(f"   ✗ Failed to capture {node}: {e}")
        
        # Also capture gateway logs
        try:
            filename = f"{self.log_dir}/{test_name}_{step}_{self.gateway}_{timestamp}.log"
            result = subprocess.run(
                ["docker", "logs", "--timestamps", "--tail", "50", self.gateway],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            with open(filename, 'w') as f:
                f.write(f"=== Logs for {self.gateway} ===\n")
                f.write(f"Test: {test_name}\n")
                f.write(f"Step: {step}\n")
                f.write(f"Captured: {timestamp}\n")
                f.write("="*80 + "\n\n")
                f.write(result.stdout)
                f.write(result.stderr)
            
            print(f"   ✓ {self.gateway} logs saved to {filename}")
        except Exception as e:
            print(f"   ✗ Failed to capture gateway: {e}")
    
    def get_cluster_state(self) -> dict:
        """Get current state of all nodes"""
        state = {
            "timestamp": datetime.now().isoformat(),
            "nodes": {}
        }
        
        for node in self.nodes:
            try:
                # Get container status
                status_result = subprocess.run(
                    ["docker", "inspect", "-f", "{{.State.Status}}", node],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                status = status_result.stdout.strip()
                
                # Get recent logs
                logs_result = subprocess.run(
                    ["docker", "logs", "--tail", "50", node],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                logs = logs_result.stdout + logs_result.stderr
                
                # Parse state from logs
                node_state = "UNKNOWN"
                term = 0
                
                if "became LEADER" in logs:
                    # Check most recent
                    for line in reversed(logs.split('\n')):
                        if "became LEADER" in line:
                            node_state = "LEADER"
                            # Try to extract term
                            if "term" in line:
                                try:
                                    term = int(line.split("term")[1].split()[0])
                                except:
                                    pass
                            break
                        elif "became CANDIDATE" in line or "became FOLLOWER" in line:
                            break
                
                if node_state == "UNKNOWN":
                    if "became CANDIDATE" in logs:
                        # Check if most recent state
                        for line in reversed(logs.split('\n')):
                            if "became CANDIDATE" in line:
                                node_state = "CANDIDATE"
                                break
                            elif "became FOLLOWER" in line or "became LEADER" in line:
                                node_state = "FOLLOWER"
                                break
                    else:
                        node_state = "FOLLOWER"
                
                # Count applied operations
                applied_count = logs.count("applied:")
                
                state["nodes"][node] = {
                    "status": status,
                    "state": node_state,
                    "term": term,
                    "applied_operations": applied_count,
                    "running": status == "running"
                }
                
            except Exception as e:
                state["nodes"][node] = {
                    "status": "error",
                    "error": str(e)
                }
        
        return state
    
    def print_cluster_state(self):
        """Print formatted cluster state"""
        state = self.get_cluster_state()
        
        print("\n" + "="*80)
        print("CLUSTER STATE (5 Nodes)".center(80))
        print("="*80)
        print(f"Timestamp: {state['timestamp']}\n")
        
        # Find leader
        leader = None
        for node, info in state["nodes"].items():
            if info.get("state") == "LEADER":
                leader = node
                break
        
        # Count running nodes
        running_count = sum(1 for info in state["nodes"].values() if info.get("running"))
        print(f"Running Nodes: {running_count}/5")
        print(f"Majority Required: 3\n")
        
        if leader:
            print(f"🎯 Leader: {leader} (Term {state['nodes'][leader].get('term', 0)})\n")
        else:
            print("⚠️  No leader found\n")
        
        # Print each node
        for node, info in state["nodes"].items():
            status_emoji = "✓" if info.get("running") else "✗"
            state_emoji = "👑" if info.get("state") == "LEADER" else "🗳️" if info.get("state") == "CANDIDATE" else "👤"
            
            print(f"{status_emoji} {state_emoji} {node}")
            print(f"   Status: {info.get('status', 'unknown')}")
            print(f"   State: {info.get('state', 'unknown')}")
            print(f"   Term: {info.get('term', 0)}")
            print(f"   Applied Ops: {info.get('applied_operations', 0)}")
            print()
        
        print("="*80 + "\n")
        
        return state
    
    def save_cluster_state(self, test_name: str, step: str):
        """Save cluster state to JSON file"""
        state = self.get_cluster_state()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.log_dir}/{test_name}_{step}_state_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(state, f, indent=2)
        
        print(f"💾 Cluster state saved to {filename}")
    
    def verify_consistency(self) -> tuple[bool, str]:
        """Verify log consistency across running nodes"""
        state = self.get_cluster_state()
        
        running_nodes = [
            (node, info) for node, info in state["nodes"].items() 
            if info.get("running")
        ]
        
        if len(running_nodes) < 2:
            return False, "Not enough running nodes to verify consistency"
        
        # Check applied operations
        applied_counts = [info.get("applied_operations", 0) for _, info in running_nodes]
        
        if len(set(applied_counts)) == 1:
            return True, f"All {len(running_nodes)} running nodes have {applied_counts[0]} applied operations"
        else:
            node_ops = {node: info.get("applied_operations", 0) for node, info in running_nodes}
            return False, f"Inconsistent operations: {node_ops}"
    
    def wait_for_stable_leader(self, timeout: int = 10) -> str:
        """Wait for a stable leader to be elected"""
        print(f"⏳ Waiting up to {timeout}s for stable leader...")
        
        start = time.time()
        last_leader = None
        stable_count = 0
        
        while time.time() - start < timeout:
            state = self.get_cluster_state()
            
            current_leader = None
            for node, info in state["nodes"].items():
                if info.get("state") == "LEADER":
                    current_leader = node
                    break
            
            if current_leader:
                if current_leader == last_leader:
                    stable_count += 1
                    if stable_count >= 3:  # Leader stable for 3 checks
                        print(f"✓ Stable leader found: {current_leader}")
                        return current_leader
                else:
                    last_leader = current_leader
                    stable_count = 1
            
            time.sleep(1)
        
        print("✗ No stable leader found within timeout")
        return None
    
    def check_majority_available(self) -> tuple[bool, int]:
        """Check if majority of nodes (3/5) are available"""
        state = self.get_cluster_state()
        running = sum(1 for info in state["nodes"].values() if info.get("running"))
        majority = (len(self.nodes) // 2) + 1
        
        return running >= majority, running

def main():
    """Example usage"""
    import sys
    
    capture = LogCapture()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "state":
            capture.print_cluster_state()
        
        elif command == "save":
            test_name = sys.argv[2] if len(sys.argv) > 2 else "test"
            step = sys.argv[3] if len(sys.argv) > 3 else "step"
            capture.capture_all_logs(test_name, step)
            capture.save_cluster_state(test_name, step)
        
        elif command == "verify":
            consistent, message = capture.verify_consistency()
            print(f"\n{'✓' if consistent else '✗'} {message}\n")
        
        elif command == "wait-leader":
            leader = capture.wait_for_stable_leader()
            if leader:
                print(f"Leader: {leader}")
            else:
                print("No leader found")
                sys.exit(1)
        
        elif command == "majority":
            has_majority, count = capture.check_majority_available()
            print(f"\nRunning nodes: {count}/5")
            print(f"Majority (3): {'✓ Available' if has_majority else '✗ Not available'}\n")
        
        else:
            print(f"Unknown command: {command}")
            print("Usage:")
            print("  python log_helper.py state           - Print current cluster state")
            print("  python log_helper.py save TEST STEP  - Capture logs and state")
            print("  python log_helper.py verify          - Verify log consistency")
            print("  python log_helper.py wait-leader     - Wait for stable leader")
            print("  python log_helper.py majority        - Check if majority available")
    else:
        print("Usage:")
        print("  python log_helper.py state           - Print current cluster state")
        print("  python log_helper.py save TEST STEP  - Capture logs and state")
        print("  python log_helper.py verify          - Verify log consistency")
        print("  python log_helper.py wait-leader     - Wait for stable leader")
        print("  python log_helper.py majority        - Check if majority available")

if __name__ == "__main__":
    main()
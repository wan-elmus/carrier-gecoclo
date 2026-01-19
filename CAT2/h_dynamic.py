import random
import time
import threading

class Node:
    def __init__(self, name, cpu_max, mem_max):
        self.name = name
        self.cpu_max = cpu_max
        self.mem_max = mem_max
        self.cpu_used = 0
        self.mem_used = 0
        self.services = []
        self.failed = False  # failure injection

    @property
    def cpu_avail(self):
        return max(0, self.cpu_max - self.cpu_used)

    @property
    def mem_avail(self):
        return max(0, self.mem_max - self.mem_used)

    def assign_service(self, service, cpu_req, mem_req):
        if self.failed:
            return False
        if self.cpu_avail >= cpu_req and self.mem_avail >= mem_req:
            self.services.append(service)
            self.cpu_used += cpu_req
            self.mem_used += mem_req
            return True
        return False

    def release_service(self, service, cpu_req, mem_req):
        if service in self.services:
            self.services.remove(service)
            self.cpu_used -= cpu_req
            self.mem_used -= mem_req

    def inject_failure(self, fail_prob=0.1):
        self.failed = random.random() < fail_prob
        return self.failed

# Example nodes
nodes = [
    Node("Edge1", 50, 4),
    Node("Edge2", 50, 4.5),
    Node("Core1", 65, 8),
    Node("Core2", 58, 7.5),
    Node("Cloud1", 72, 16)
]

# Services with CPU/memory requirements
services = [
    ("RPC", 10, 1),
    ("EventOrdering", 5, 0.5),
    ("TransactionCommit", 15, 2),
    ("Analytics", 20, 8)
]

def dynamic_allocation():
    """Simulate dynamic process allocation with adaptive migration."""
    for service, cpu_req, mem_req in services:
        # Compute node weights based on available resources
        total_avail = sum((node.cpu_avail/cpu_req + node.mem_avail/mem_req) for node in nodes if not node.failed)
        if total_avail == 0:
            continue
        weights = [(node.cpu_avail/cpu_req + node.mem_avail/mem_req)/total_avail for node in nodes if not node.failed]
        # Probabilistically assign service
        eligible_nodes = [node for node in nodes if not node.failed]
        node = random.choices(eligible_nodes, weights=weights)[0]
        assigned = node.assign_service(service, cpu_req, mem_req)
        if not assigned:
            # Fallback: migrate to any node with enough resources
            for n in eligible_nodes:
                if n.assign_service(service, cpu_req, mem_req):
                    break

def simulate_traffic(duration=10, interval=1):
    """Simulate dynamic traffic, network delays, and failures."""
    for t in range(duration):
        print(f"\nTime step {t+1}")
        # Inject failures
        for node in nodes:
            if node.inject_failure(fail_prob=0.1):
                print(f"{node.name} failed!")
                # Release all services temporarily
                for s, cpu, mem in services:
                    node.release_service(s, cpu, mem)
        # Allocate/reallocate services dynamically
        dynamic_allocation()
        # Print node status
        for node in nodes:
            print(f"{node.name}: Services={node.services}, CPU used={node.cpu_used}, Mem used={node.mem_used}, Failed={node.failed}")
        # Simulate network delay between nodes
        time.sleep(interval * random.uniform(0.5, 1.5))

if __name__ == "__main__":
    simulate_traffic(duration=10, interval=1)

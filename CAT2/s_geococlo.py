import random
from collections import defaultdict

# --- Node and Service Classes ---

class Node:
    def __init__(self, name, layer, cpu_max, mem_max, fail_prob, byzantine=False):
        self.name = name
        self.layer = layer
        self.cpu_max = cpu_max
        self.mem_max = mem_max
        self.cpu_used = 0
        self.mem_used = 0
        self.services = []
        self.failed = False
        self.partitioned = False
        self.fail_prob = fail_prob
        self.byzantine = byzantine

    def inject_failure(self):
        if random.random() < self.fail_prob:
            self.failed = True
            self.services.clear()
            return True
        return False

    def recover(self):
        self.failed = False
        self.partitioned = False

    def add_service(self, service, cpu_req=5, mem_req=1):
        if self.failed or self.partitioned:
            return False
        if self.cpu_used + cpu_req <= self.cpu_max and self.mem_used + mem_req <= self.mem_max:
            self.services.append(service)
            self.cpu_used += cpu_req
            self.mem_used += mem_req
            return True
        return False

    def remove_service(self, service, cpu_req=5, mem_req=1):
        if service in self.services:
            self.services.remove(service)
            self.cpu_used -= cpu_req
            self.mem_used -= mem_req

# --- Initialize Nodes ---
nodes = [
    Node("Edge1", "Edge", 50, 8, 0.1),
    Node("Edge2", "Edge", 50, 8, 0.1),
    Node("Core1", "Core", 80, 12, 0.05, byzantine=True),
    Node("Core2", "Core", 80, 12, 0.05),
    Node("Cloud1", "Cloud", 100, 16, 0.05)
]

# --- Service Definitions ---
service_specs = {
    "RPC": {"layer":"Edge", "replicas":2},
    "EventOrdering": {"layer":"Edge", "replicas":2},
    "TransactionCommit": {"layer":"Core", "replicas":3},
    "Analytics": {"layer":"Cloud", "replicas":3},
    "SharedMemory": {"layer":"Cloud", "replicas":3},
    "Recovery": {"layer":"Core", "replicas":2},
    "DeadlockDetection": {"layer":"Core", "replicas":2}
}

# --- Service Registry ---
service_registry = defaultdict(list)

# --- Deadlock detection ---
def detect_deadlock(nodes):
    # Simple simulation: if two services compete, abort one
    for node in nodes:
        if node.failed:
            continue
        if "TransactionCommit" in node.services and "RPC" in node.services:
            # Simulate abort of one transaction
            node.remove_service("TransactionCommit")
            print(f"Deadlock resolved by aborting TransactionCommit on {node.name}")

# --- Distributed commit simulation ---
def distributed_commit(service, nodes):
    quorum = 0
    for node in nodes:
        if service in node.services and not node.failed:
            quorum += 1
    # PBFT for Byzantine tolerance: quorum > 2/3 of replicas
    return quorum

# --- Service Allocation / Replication ---
def replicate_services():
    for svc, spec in service_specs.items():
        replicas_needed = spec["replicas"]
        allocated = 0
        # Prefer nodes in the same layer
        for node in nodes:
            if node.layer == spec["layer"] and not node.failed and node.add_service(svc):
                service_registry[svc].append(node.name)
                allocated += 1
            if allocated >= replicas_needed:
                break
        # If under-replicated, fill from other layers
        if allocated < replicas_needed:
            for node in nodes:
                if node.layer != spec["layer"] and not node.failed and node.add_service(svc):
                    service_registry[svc].append(node.name)
                    allocated += 1
                if allocated >= replicas_needed:
                    break

# --- Failover & Migration ---
def failover_and_migrate():
    for node in nodes:
        if node.failed or node.partitioned:
            # Migrate services to healthy nodes
            for svc in node.services:
                for target in nodes:
                    if target.failed or target.partitioned:
                        continue
                    if svc not in target.services:
                        target.add_service(svc)

# --- Simulation Loop ---
timesteps = 10
for t in range(1, timesteps+1):
    print(f"\nTime step {t}")
    # Inject random failures
    for node in nodes:
        node.inject_failure()
    
    replicate_services()
    failover_and_migrate()
    detect_deadlock(nodes)
    
    # Print node state
    for node in nodes:
        print(f"{node.name}: Services={node.services}, Failed={node.failed}, Partitioned={node.partitioned}, CPU={node.cpu_used}, Mem={node.mem_used}")


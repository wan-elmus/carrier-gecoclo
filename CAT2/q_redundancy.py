import random
import time

class Node:
    def __init__(self, name, layer, services, cpu_cap, mem_cap):
        self.name = name
        self.layer = layer
        self.services = services[:]
        self.cpu_cap = cpu_cap
        self.mem_cap = mem_cap
        self.failed = False
        self.partitioned = False

    def is_available(self):
        return not self.failed and not self.partitioned


# Initial nodes (from risk analysis in p)
nodes = [
    Node("Edge1", "Edge", ["RPC", "EventOrdering"], 50, 6),
    Node("Edge2", "Edge", ["RPC"], 50, 6),
    Node("Core1", "Core", ["TransactionCommit"], 80, 10),
    Node("Core2", "Core", ["Recovery", "DeadlockDetection"], 70, 9),
    Node("Cloud1", "Cloud", ["Analytics", "SharedMemory"], 90, 20),
]

# Global service registry
service_registry = {}

def register_services():
    service_registry.clear()
    for n in nodes:
        if n.is_available():
            for s in n.services:
                service_registry.setdefault(s, []).append(n.name)

def inject_failures():
    for n in nodes:
        if random.random() < 0.25:
            n.failed = True
        if random.random() < 0.2:
            n.partitioned = True

def recover_nodes():
    for n in nodes:
        if random.random() < 0.3:
            n.failed = False
            n.partitioned = False

def replicate_and_failover():
    for service, holders in list(service_registry.items()):
        if len(holders) < 2:
            candidates = [n for n in nodes if n.is_available() and service not in n.services]
            if candidates:
                target = random.choice(candidates)
                target.services.append(service)

def print_state(t):
    print(f"\nTime step {t}")
    for n in nodes:
        print(
            f"{n.name}: "
            f"Services={n.services}, "
            f"Failed={n.failed}, "
            f"Partitioned={n.partitioned}"
        )

# Simulation
for t in range(1, 11):
    inject_failures()
    register_services()
    replicate_and_failover()
    print_state(t)
    recover_nodes()
    time.sleep(0.5)

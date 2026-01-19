import random
import time

class Node:
    def __init__(self, name, cpu_max, mem_max):
        self.name = name
        self.cpu_max = cpu_max
        self.mem_max = mem_max
        self.cpu_used = 0
        self.mem_used = 0
        self.services = []
        self.failed = False

    @property
    def cpu_avail(self):
        return max(0, self.cpu_max - self.cpu_used)

    @property
    def mem_avail(self):
        return max(0, self.mem_max - self.mem_used)

    def assign_service(self, service, cpu_req, mem_req):
        if self.failed: return False
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

    def inject_failure(self, prob=0.1):
        self.failed = random.random() < prob
        return self.failed

# Nodes
nodes = [
    Node("Edge1", 50, 4), Node("Edge2", 50, 4.5),
    Node("Core1", 65, 8), Node("Core2", 58, 7.5),
    Node("Cloud1", 72, 16)
]

# Services with CPU and memory requirements
services = [
    ("RPC", 10, 1), ("EventOrdering", 5, 0.5),
    ("TransactionCommit", 15, 2), ("Analytics", 20, 8)
]

# Global service registry
service_registry = {s[0]: [] for s in services}

def register_service(node, service):
    service_registry[service].append(node.name)

def dynamic_allocation():
    for service, cpu_req, mem_req in services:
        # Determine eligible nodes
        eligible = [n for n in nodes if not n.failed]
        if not eligible: continue
        total_weight = sum((n.cpu_avail/cpu_req + n.mem_avail/mem_req) for n in eligible)
        weights = [(n.cpu_avail/cpu_req + n.mem_avail/mem_req)/total_weight for n in eligible]
        # Assign service
        node = random.choices(eligible, weights=weights)[0]
        assigned = node.assign_service(service, cpu_req, mem_req)
        if assigned: register_service(node, service)
        else:  # migrate to another replica if first choice fails
            for n in eligible:
                if n.assign_service(service, cpu_req, mem_req):
                    register_service(n, service)
                    break

def simulate_system(duration=10, interval=1):
    for t in range(duration):
        print(f"\nTime step {t+1}")
        # Inject failures
        for node in nodes:
            if node.inject_failure(prob=0.1):
                for s, cpu, mem in services:
                    node.release_service(s, cpu, mem)
        # Allocate/migrate services dynamically
        dynamic_allocation()
        # Print node status
        for n in nodes:
            print(f"{n.name}: Services={n.services}, CPU={n.cpu_used}, Mem={n.mem_used}, Failed={n.failed}")

if __name__ == "__main__":
    simulate_system(duration=10, interval=1)

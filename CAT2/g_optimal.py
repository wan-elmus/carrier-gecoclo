import random

class Node:
    def __init__(self, name, cpu_max, mem_max):
        self.name = name
        self.cpu_max = cpu_max
        self.mem_max = mem_max
        self.cpu_used = 0
        self.mem_used = 0
        self.services = []

    @property
    def cpu_avail(self):
        return self.cpu_max - self.cpu_used

    @property
    def mem_avail(self):
        return self.mem_max - self.mem_used

    def assign_service(self, service, cpu_req, mem_req):
        if self.cpu_avail >= cpu_req and self.mem_avail >= mem_req:
            self.services.append(service)
            self.cpu_used += cpu_req
            self.mem_used += mem_req
            return True
        return False

# Example nodes
nodes = [
    Node("Edge1", cpu_max=50, mem_max=4),
    Node("Edge2", cpu_max=50, mem_max=4.5),
    Node("Core1", cpu_max=65, mem_max=8),
    Node("Core2", cpu_max=58, mem_max=7.5),
    Node("Cloud1", cpu_max=72, mem_max=16)
]

# Services with CPU/memory requirements
services = [
    ("RPC", 10, 1),
    ("EventOrdering", 5, 0.5),
    ("TransactionCommit", 15, 2),
    ("Analytics", 20, 8)
]

# Dynamic allocation based on available CPU/memory
for service, cpu_req, mem_req in services:
    total_avail = sum(node.cpu_avail/cpu_req + node.mem_avail/mem_req for node in nodes)
    # Compute proportional allocation
    probs = [(node.cpu_avail/cpu_req + node.mem_avail/mem_req)/total_avail for node in nodes]
    # Assign service to node probabilistically based on weights
    node = random.choices(nodes, weights=probs)[0]
    assigned = node.assign_service(service, cpu_req, mem_req)
    if not assigned:
        # fallback: find first node with enough resources
        for n in nodes:
            if n.assign_service(service, cpu_req, mem_req):
                break

# Display allocation
for node in nodes:
    print(f"{node.name} allocated services: {node.services}, CPU used: {node.cpu_used}, Mem used: {node.mem_used}")

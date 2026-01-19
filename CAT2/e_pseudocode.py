# Assume nodes = list of Node objects with cpu_capacity, mem_capacity
# incoming_transactions = list of (service_type, arrival_time)

def allocate_transactions(nodes, incoming_transactions):
    for tx in incoming_transactions:
        # Compute node weights based on available CPU and memory
        weights = []
        for node in nodes:
            cpu_avail = node.cpu_capacity - node.cpu_usage
            mem_avail = node.mem_capacity - node.mem_usage
            weight = max(0, cpu_avail) + max(0, mem_avail)
            weights.append(weight)
        # Normalize weights
        total = sum(weights)
        if total == 0:
            continue  # Drop or queue transaction if all nodes saturated
        weights = [w/total for w in weights]
        # Assign transaction probabilistically according to weights
        node = random.choices(nodes, weights)[0]
        node.process(tx)

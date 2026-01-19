import multiprocessing as mp
import queue
import time
import random

class Node:
    def __init__(self, name, layer, services, failure_type, active):
        self.name = name
        self.layer = layer
        self.services = services
        self.failure_type = failure_type
        self.queue = mp.Queue()
        self.active = active  # mp.Value(boolean)

    def process_message(self, msg):
        if not self.active.value:
            return f"{self.name} inactive"

        time.sleep(random.uniform(0.1, 0.5))

        if "RPC" in msg and "RPC Call" in self.services:
            return f"{self.name} handled RPC"

        if "Commit" in msg and "Transaction Commit" in self.services:
            return self.two_pc_vote()

        if "Recovery" in msg:
            self.active.value = True
            return f"{self.name} recovered"

        return f"{self.name} processed message"

    def two_pc_vote(self):
        if self.failure_type == "Byzantine":
            return random.choice(["YES", "NO", "INVALID"])
        return "YES"

def run_node(node):
    while True:
        try:
            msg = node.queue.get(timeout=5)
            result = node.process_message(msg)
            print(result)
        except queue.Empty:
            break

if __name__ == "__main__":
    active_flag = lambda: mp.Value('b', True)

    nodes = [
        Node("Edge1", "Edge", ["RPC Call", "Event Ordering"], "Crash", active_flag()),
        Node("Core1", "Core", ["Transaction Commit"], "Byzantine", active_flag()),
        Node("Cloud1", "Cloud", ["Analytics", "RPC Call"], "Omission", active_flag())
    ]

    processes = [mp.Process(target=run_node, args=(n,)) for n in nodes]
    for p in processes:
        p.start()

    nodes[0].queue.put("RPC")
    nodes[1].queue.put("Commit")

    time.sleep(5)
    for p in processes:
        p.terminate()

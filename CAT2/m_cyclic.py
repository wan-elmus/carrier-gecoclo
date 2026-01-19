import time
import random
from collections import defaultdict, deque

class Transaction:
    def __init__(self, tid, locks):
        self.tid = tid
        self.locks = locks
        self.start_time = time.time()
        self.aborted = False

class DeadlockDetector:
    def __init__(self, timeout=3):
        self.wait_for = defaultdict(set)
        self.transactions = {}
        self.timeout = timeout

    def add_transaction(self, tx):
        self.transactions[tx.tid] = tx

    def add_dependency(self, t1, t2):
        self.wait_for[t1].add(t2)

    def remove_transaction(self, tid):
        self.wait_for.pop(tid, None)
        for deps in self.wait_for.values():
            deps.discard(tid)
        self.transactions.pop(tid, None)

    def detect_cycle(self):
        visited, stack = set(), set()

        def dfs(t):
            if t in stack:
                return True
            if t in visited:
                return False
            visited.add(t)
            stack.add(t)
            for nxt in self.wait_for[t]:
                if dfs(nxt):
                    return True
            stack.remove(t)
            return False

        return any(dfs(t) for t in self.wait_for)

    def detect_timeout(self):
        now = time.time()
        for tx in self.transactions.values():
            if not tx.aborted and now - tx.start_time > self.timeout:
                return tx.tid
        return None

    def resolve(self):
        timeout_tx = self.detect_timeout()
        if timeout_tx:
            victim = timeout_tx
        elif self.detect_cycle():
            victim = random.choice(list(self.wait_for.keys()))
        else:
            return None

        self.transactions[victim].aborted = True
        self.remove_transaction(victim)
        return victim

def simulate():
    detector = DeadlockDetector(timeout=2)

    t1 = Transaction("T1", ["A", "B"])
    t2 = Transaction("T2", ["B", "C"])
    t3 = Transaction("T3", ["C", "A"])

    for t in [t1, t2, t3]:
        detector.add_transaction(t)

    detector.add_dependency("T1", "T2")
    detector.add_dependency("T2", "T3")
    detector.add_dependency("T3", "T1")

    time.sleep(2.5)

    victim = detector.resolve()
    print("Deadlock resolved by aborting:", victim)

if __name__ == "__main__":
    simulate()

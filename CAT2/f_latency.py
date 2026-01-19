import pandas as pd

# Dataset
data = {
    "Node": ["Edge1", "Edge2", "Core1", "Core2", "Cloud1"],
    "Layer": ["Edge", "Edge", "Core", "Core", "Cloud"],
    "Latency_ms": [12, 15, 8, 10, 22],
    "CPU_pct": [45, 50, 65, 58, 72],
    "Memory_GB": [4.0, 4.5, 8.0, 7.5, 16.0],
    "Transactions_per_sec": [120, 100, 250, 230, 300],
    "Locks_pct": [5, 8, 12, 10, 15],
    "P_fail": [0.05, 0.08, 0.10, 0.07, 0.08]  # conditional failure probability
}

df = pd.DataFrame(data)

# Compute effective latency
df["Effective_Latency"] = df["Latency_ms"] * (1 + df["CPU_pct"]/100) * (1 + df["Locks_pct"]/100) * (1 + df["P_fail"])

# Rank nodes by effective latency
df_sorted = df.sort_values("Effective_Latency", ascending=False)

print(df_sorted[["Node", "Layer", "Effective_Latency"]])

import pandas as pd

# Load dataset
df = pd.read_csv("distributed_nodes.csv")

# Convert packet loss to fraction
df["Packet_Loss_frac"] = df["Packet_Loss_pct"] / 100.0

# Bottleneck severity score
df["BottleneckScore"] = (
    df["Latency_ms"]
    * df["CPU_pct"]
    * df["Locks_pct"]
    * (1 + df["Packet_Loss_frac"])
) / (
    df["Throughput_Mbps"]
    * df["Transactions_per_sec"]
)

# Rank nodes (higher score = worse bottleneck)
ranked = df.sort_values("BottleneckScore", ascending=False)

print(ranked[[
    "Node",
    "Layer",
    "BottleneckScore",
    "Latency_ms",
    "CPU_pct",
    "Locks_pct",
    "Throughput_Mbps",
    "Transactions_per_sec"
]])

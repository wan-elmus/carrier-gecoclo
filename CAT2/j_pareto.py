import random
import pandas as pd

nodes = [
    {"name": "Edge", "L": 10, "T": 120, "CPU": 30, "Mem": 3, "MemMax": 8, "Pfail": 0.05},
    {"name": "Core", "L": 15, "T": 250, "CPU": 50, "Mem": 5, "MemMax": 16, "Pfail": 0.03},
    {"name": "Cloud", "L": 25, "T": 400, "CPU": 70, "Mem": 10, "MemMax": 32, "Pfail": 0.02},
]

configs = []

for r_edge in [1, 2, 3]:
    for r_core in [2, 3, 4]:
        for r_cloud in [2, 3, 4]:
            replicas = {"Edge": r_edge, "Core": r_core, "Cloud": r_cloud}

            T_sys = 0
            L_sys = 0

            for n in nodes:
                r = replicas[n["name"]]
                T_sys += r * n["T"] * (1 - n["Pfail"])
                L_sys += (
                    (n["L"] / (1 - n["Pfail"])) *
                    (1 + n["CPU"] / 100) *
                    (1 + n["Mem"] / n["MemMax"])
                )

            configs.append({
                "Edge_r": r_edge,
                "Core_r": r_core,
                "Cloud_r": r_cloud,
                "Throughput": round(T_sys, 2),
                "Latency": round(L_sys, 2),
            })

df = pd.DataFrame(configs)

pareto = []
for i, row in df.iterrows():
    dominated = False
    for _, other in df.iterrows():
        if (other["Throughput"] >= row["Throughput"] and
            other["Latency"] <= row["Latency"] and
            (other["Throughput"] > row["Throughput"] or
             other["Latency"] < row["Latency"])):
            dominated = True
            break
    if not dominated:
        pareto.append(row)

pareto_df = pd.DataFrame(pareto)
print(pareto_df.sort_values("Latency"))

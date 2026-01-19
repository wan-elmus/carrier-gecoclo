import matplotlib.pyplot as plt
import numpy as np

timesteps = np.arange(1, 11)

# CPU usage per layer
cpu_usage = {
    'Edge': [40, 55, 60, 60, 60, 60, 40, 40, 0, 0],
    'Core': [30, 80, 115, 55, 60, 60, 0, 0, 0, 0],
    'Cloud': [10, 20, 45, 75, 75, 80, 80, 80, 80, 80]
}

# Memory usage per layer
mem_usage = {
    'Edge': [8, 11, 12, 12, 12, 12, 8, 8, 0, 0],
    'Core': [6, 16, 23, 11, 12, 12, 12, 12, 0, 0],
    'Cloud': [2, 4, 9, 15, 15, 16, 16, 16, 16, 16]
}

throughput = [2000, 2200, 2100, 1800, 1600, 1500, 1200, 1100, 1000, 900]
latency = [50, 55, 60, 70, 80, 85, 95, 100, 110, 120]
replicas = [5, 6, 7, 8, 9, 10, 10, 10, 10, 10]

# Node failure flags for overlay
failures = {
    'Edge': [0, 0, 0, 0, 0, 0, 1, 1, 1, 1],
    'Core': [0, 0, 0, 0, 0, 0, 1, 1, 1, 1],
    'Cloud': [0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
}

fig, axes = plt.subplots(3, 2, figsize=(14, 12))
fig.suptitle("Edge-Core-Cloud System Metrics with Layer Breakdown and Failover", fontsize=16)

# CPU
for layer, color in zip(['Edge','Core','Cloud'], ['tab:blue','tab:green','tab:cyan']):
    axes[0,0].plot(timesteps, cpu_usage[layer], marker='o', label=layer, color=color)
    for t, fail in enumerate(failures[layer]):
        if fail:
            axes[0,0].scatter(t+1, cpu_usage[layer][t], color='red', s=100, edgecolors='k', zorder=5)
axes[0,0].set_title("CPU Usage per Layer (Red=Failed)")
axes[0,0].set_xlabel("Timestep")
axes[0,0].set_ylabel("CPU (%)")
axes[0,0].legend()

# Memory
for layer, color in zip(['Edge','Core','Cloud'], ['tab:blue','tab:green','tab:cyan']):
    axes[0,1].plot(timesteps, mem_usage[layer], marker='o', label=layer, color=color)
    for t, fail in enumerate(failures[layer]):
        if fail:
            axes[0,1].scatter(t+1, mem_usage[layer][t], color='red', s=100, edgecolors='k', zorder=5)
axes[0,1].set_title("Memory Usage per Layer (Red=Failed)")
axes[0,1].set_xlabel("Timestep")
axes[0,1].set_ylabel("Memory (GB)")
axes[0,1].legend()

# Throughput
axes[1,0].plot(timesteps, throughput, marker='o', color='tab:orange')
axes[1,0].set_title("System Throughput")
axes[1,0].set_xlabel("Timestep")
axes[1,0].set_ylabel("Transactions/sec")

# Latency
axes[1,1].plot(timesteps, latency, marker='o', color='tab:red')
axes[1,1].set_title("Average Latency")
axes[1,1].set_xlabel("Timestep")
axes[1,1].set_ylabel("Latency (ms)")

# Replicas
axes[2,0].plot(timesteps, replicas, marker='o', color='tab:purple')
axes[2,0].set_title("Service Replicas Over Time")
axes[2,0].set_xlabel("Timestep")
axes[2,0].set_ylabel("Replicas Count")

axes[2,1].axis('off')

plt.tight_layout(rect=[0,0.03,1,0.95])
plt.show()

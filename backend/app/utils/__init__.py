"""
Utilities for carrier-grade telecom distributed system
"""

from .logger import setup_logger, TelecomLogger
from .locks import DistributedLock, TransactionLocks, ensure_locks_table
from .clocks import VectorClock, VectorClockManager, init_clock_manager, get_clock_manager
from .heartbeat import HeartbeatManager, start_heartbeat, get_heartbeat_manager
from .network import NetworkClient, simulate_network_delay, get_network_latency

__all__ = [
    # Logger
    "setup_logger",
    "TelecomLogger",
    
    # Locks
    "DistributedLock", 
    "TransactionLocks",
    "ensure_locks_table",
    
    # Clocks
    "VectorClock",
    "VectorClockManager", 
    "init_clock_manager",
    "get_clock_manager",
    
    # Heartbeat
    "HeartbeatManager",
    "start_heartbeat", 
    "get_heartbeat_manager",
    
    # Network
    "NetworkClient",
    "simulate_network_delay",
    "get_network_latency",
]
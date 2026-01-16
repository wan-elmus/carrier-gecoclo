from typing import Dict, List, Optional, Tuple
from pydantic import BaseModel
import json
from datetime import datetime
from app.utils.logger import setup_logger

logger = setup_logger(__name__)

class VectorClock(BaseModel):
    """Vector clock for distributed event ordering"""
    node_id: str
    timestamps: Dict[str, int]  # node_id -> counter
    
    @classmethod
    def create(cls, node_id: str, all_nodes: List[str]) -> 'VectorClock':
        """Create new vector clock initialized with zeros"""
        timestamps = {node: 0 for node in all_nodes}
        return cls(node_id=node_id, timestamps=timestamps)
    
    def increment(self) -> None:
        """Increment this node's counter"""
        current = self.timestamps.get(self.node_id, 0)
        self.timestamps[self.node_id] = current + 1
    
    def update(self, other: 'VectorClock') -> bool:
        """
        Merge with another vector clock (take maximum of each counter)
        Returns True if this clock was updated
        """
        updated = False
        for node, counter in other.timestamps.items():
            current = self.timestamps.get(node, 0)
            if counter > current:
                self.timestamps[node] = counter
                updated = True
        return updated
    
    def compare(self, other: 'VectorClock') -> int:
        """
        Compare two vector clocks
        Returns: -1 if this < other, 0 if concurrent, 1 if this > other
        """
        less = False
        greater = False
        
        all_nodes = set(self.timestamps.keys()) | set(other.timestamps.keys())
        
        for node in all_nodes:
            v1 = self.timestamps.get(node, 0)
            v2 = other.timestamps.get(node, 0)
            
            if v1 < v2:
                less = True
            elif v1 > v2:
                greater = True
        
        if less and not greater:
            return -1  # this < other
        elif greater and not less:
            return 1   # this > other
        else:
            return 0   # concurrent
    
    def to_json(self) -> str:
        """Serialize to JSON string"""
        return json.dumps({
            "node_id": self.node_id,
            "timestamps": self.timestamps
        })
    
    @classmethod
    def from_json(cls, json_str: str) -> 'VectorClock':
        """Deserialize from JSON string"""
        data = json.loads(json_str)
        return cls(**data)
    
    def __str__(self) -> str:
        return f"VectorClock(node={self.node_id}, timestamps={self.timestamps})"

class Event:
    """Event with vector clock timestamp"""
    def __init__(self, event_id: str, data: Dict, clock: VectorClock):
        self.event_id = event_id
        self.data = data
        self.clock = clock
        self.created_at = datetime.utcnow()
    
    def to_dict(self) -> Dict:
        return {
            "event_id": self.event_id,
            "data": self.data,
            "clock": self.clock.to_json(),
            "created_at": self.created_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Event':
        clock = VectorClock.from_json(data['clock'])
        event = cls(
            event_id=data['event_id'],
            data=data['data'],
            clock=clock
        )
        event.created_at = datetime.fromisoformat(data['created_at'])
        return event

class VectorClockManager:
    """Manages vector clocks for a node"""
    def __init__(self, node_id: str, all_nodes: List[str]):
        self.node_id = node_id
        self.all_nodes = all_nodes
        self.clock = VectorClock.create(node_id, all_nodes)
        self.pending_events: List[Event] = []
        self.delivered_events: List[Event] = []
    
    def create_event(self, event_data: Dict) -> Event:
        """Create new event with incremented vector clock"""
        self.clock.increment()
        event_id = f"{self.node_id}_{self.clock.timestamps[self.node_id]}"
        event = Event(event_id, event_data, self.clock.model_copy())
        self.pending_events.append(event)
        logger.debug(f"Created event {event_id}: {self.clock}")
        return event
    
    def receive_event(self, event: Event) -> bool:
        """
        Receive event from another node
        Returns True if event can be delivered immediately
        """
        # Update our clock with received clock
        updated = self.clock.update(event.clock)
        
        if updated:
            logger.debug(f"Clock updated after receiving event {event.event_id}")
        
        # Check if we can deliver this event (all predecessors received)
        can_deliver = self._can_deliver(event)
        
        if can_deliver:
            self._deliver_event(event)
        else:
            self.pending_events.append(event)
            # Try to deliver any pending events that might now be ready
            self._try_deliver_pending()
        
        return can_deliver
    
    def _can_deliver(self, event: Event) -> bool:
        """Check if event can be delivered (causal order satisfied)"""
        for node, counter in event.clock.timestamps.items():
            if node == self.node_id:
                continue
            our_counter = self.clock.timestamps.get(node, 0)
            if counter > our_counter + 1:
                return False
        return True
    
    def _deliver_event(self, event: Event) -> None:
        """Deliver event (causal order satisfied)"""
        # Update our clock to include this event
        self.clock.update(event.clock)
        self.delivered_events.append(event)
        
        # Remove from pending if it was there
        if event in self.pending_events:
            self.pending_events.remove(event)
        
        logger.info(f"Delivered event {event.event_id} from {event.clock.node_id}")
    
    def _try_deliver_pending(self) -> None:
        """Try to deliver any pending events that are now ready"""
        for event in list(self.pending_events):
            if self._can_deliver(event):
                self._deliver_event(event)
    
    def get_state(self) -> Dict:
        """Get current state for debugging/monitoring"""
        return {
            "node_id": self.node_id,
            "clock": self.clock.timestamps,
            "pending_events": len(self.pending_events),
            "delivered_events": len(self.delivered_events)
        }

# Global clock manager instance
_clock_manager: Optional[VectorClockManager] = None

def init_clock_manager(node_id: str, all_nodes: List[str]) -> None:
    """Initialize global clock manager"""
    global _clock_manager
    _clock_manager = VectorClockManager(node_id, all_nodes)
    logger.info(f"Vector clock initialized for {node_id} with nodes {all_nodes}")

def get_clock_manager() -> VectorClockManager:
    """Get global clock manager instance"""
    if _clock_manager is None:
        raise RuntimeError("Clock manager not initialized. Call init_clock_manager first.")
    return _clock_manager
from BPCon.routing import RoutingManager
from BPCon.storage import InMemoryStorage


class StateManager(object):
    def __init__(self, init_state=None):
        self.groups = {}
        self.groups['G0'] = None 
        self.groups['G1'] = RoutingManager()
        self.groups['G2'] = RoutingManager()
        
        self.db = InMemoryStorage()

        self.group_p1_hashval = None 
        self.state = 'normal'     # normal, managing1, managing2, awaiting
        self.state_timer = 0.0
        
        
        # local stats
        self.routing_cache = {}
        self.peer_latencies = (0,0) # (# records, weighted average)
        self.client_latencies = (0,0)
        self.failed_peers = {}
        self.bad_peers = {}


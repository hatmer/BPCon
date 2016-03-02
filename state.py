from BPCon.routing import RoutingManager
from BPCon.storage import InMemoryStorage


class StateManager(object):
    def __init__(self, init_state=None):
        self.G0_keyspace = (0,1)
        self.G1_keyspace = (0,0)
        self.G2_keyspace = (0,0)
        self.G0_peers = RoutingManager() # initlist and keydir
        self.G1_peers = RoutingManager()
        self.G2_peers = RoutingManager()
        
        self.db = InMemoryStorage()

        self.group_p1_lock = None # format: (owner, op)
        self.state = 'normal'     # normal, managing1, managing2, awaiting

        self.routing_cache = {}

    

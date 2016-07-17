from BPCon.routing import GroupManager
from BPCon.storage import InMemoryStorage
from Crypto.Hash import SHA
import pickle
import time

class StateManager:
    def __init__(self, conf): # init_state=None):
        self.log = conf['log']
        self.addr = conf['c_wss']
        self.groups = {}
        self.groups['G0'] = GroupManager(conf) # (members, keyspace)
        self.groups['G1'] = GroupManager(conf) # overwritten by BPCon
        self.groups['G2'] = GroupManager(conf)
        
        self.db = InMemoryStorage()

        self.group_p1_hashval = None 
        self.state = 'normal'     # normal, managing1, managing2, awaiting
        self.timer = 0.0
        
        # local stats
        self.routing_cache = {}
        self.peer_latencies = (0,0) # (# records, weighted average)
        self.failed_peers = {}
        self.bad_peers = {}

    def update(self, val, ballot_num=-1):
        self.log.debug("updating state: ballot #{}, op: {}".format(ballot_num, val))
        if not ',' in val:
            # requires unpackaging
            length, data = val.split('<>')
            val = int(data).to_bytes(int(length), byteorder='little').decode()

        t,k,v = val.split(',')

        
        #DB requests
        if t == 'P':
            self.db.put(k,v)
        elif t == 'D':
            self.db.delete(k)
        if t == 'A': # Adding peer: k is wss, v is key
            self.groups['G1'].add_peer(k,v)
        #Group membership requests
        elif t == "S": # Split: v is initiator wss
            self.log.debug("splitting")
            peers = self.groups['G1'].peers.keys() # array of wss
            l = int(len(peers)/2)
            list_a = peers[:l]
            list_b = peers[l:] # these nodes will be in the new group
            keyspace = self.groups['G1'].keyspace
            diff = (keyspace[1] - keyspace[0]) / 2
            mid = keyspace[0] + diff

            if self.addr in list_a:
                self.groups['G2'].peers = OrderedDict()
                for wss in list_b:
                    self.groups['G2'].peers[wss] = self.groups['G1'].peers[wss]
                    del self.groups['G1'].peers[wss]
                
                self.groups['G1'].keyspace = (keyspace[0],mid)
                self.groups['G2'].keyspace = (mid,keyspace[1])

            elif self.addr in list_b:
                self.groups['G0'].peers = OrderedDict()
                for wss in list_a:
                    self.groups['G0'].peers[wss] = self.groups['G1'].peers[wss]
                    del self.groups['G1'].peers[wss]

                self.groups['G0'].keyspace = (keyspace[0],mid)
                self.groups['G1'].keyspace = (mid,keyspace[1])
            
            else:
                self.log.error("inconsistent state. not participating in split operation!")
        

        # congregate requests
        elif t == 'G': 
            if k == 'lock': #op is lock request
                self.log.debug("attempting to acquire lock")
                elapsed = time.time() - self.timer
                if self.state == 'normal' or (self.state == 'locked' and elapsed > 3):
                    self.group_p1_hashval = v
                    self.state = 'locked'
                    self.timer = time.time()
                    self.log.debug("lock acquired. Locked hashvalue: {}".format(v))

            elif k == "commit" and self.state == 'locked': #op is prepped(locked) group op
                    # verify group op
                    vhash = SHA.new(val.encode()).hexdigest()
                    if vhash != self.group_p1_hashval:
                        self.log.info("locked state, rejecting invalid commit value")
                        return
                    self.group_update(v)
                    # release lock
                    self.state = 'normal'
                    self.log.debug("lock released")
            else:
                self.log.info("got bad group request: ignoring")
    
    def group_update(self, opList):
        # TODO modify for rollbackable
        self.log.info("performing group operations: {}".format(opList))
        try:
            for item in opList.split('<>'): #  keyspace and group membership
                t,g,a,b = item.split(';') # t is type, g is Group#
                if g not in ['G0', 'G1', 'G2']:
                    raise ValueError("key is not a valid group")
                if t == 'A': # Adding peer: a is wss, b is key
                    self.groups[g].add_peer(a,b)
                elif t == 'R': # Removing peer: a is wss, b is placeholder
                    self.groups[g].remove_peer(a)
                elif t == 'K': # Keyspace update: a is lower, b is upper
                    self.groups[g].keyspace = (float(a),float(b))
                elif t == 'M': # Migrate peer: a is wss, b is destination group, b does an add
                    pubkey = self.groups[g].peers[a]
                    self.groups[b].peers[a] = pubkey
                    self.groups[g].remove_peer(a)
                
                else:
                    self.log.info("unrecognized group op: {}".format(item))

        except Exception as e:
            self.log.info("got bad value in group update: {}".format(e))


    def image_state(self):
        # create disc copy of system state 
        try:
            # These saved to data directory
            self.groups['G0'].save(0)
            self.groups['G1'].save(1)
            self.groups['G2'].save(2)
            self.db.save()
        except Exception as e:
            self.log.debug("save state failed: {}".format(e))

    def load_state(self):
        try:
            self.groups['G0'].load(0)
            self.groups['G1'].load(1)
            self.groups['G2'].load(2)
            self.db.load()
        except Exception as e:
            self.log.debug("load state failed: {}".format(e))

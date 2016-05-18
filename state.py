from BPCon.routing import GroupManager
from BPCon.storage import InMemoryStorage
from Crypto.Hash import SHA
import pickle
class StateManager:
    def __init__(self): # init_state=None):
        self.groups = {}
        self.groups['G0'] = None 
        self.groups['G1'] = GroupManager() # (members, keyspace)
        self.groups['G2'] = GroupManager()
        
        self.db = InMemoryStorage()

        self.group_p1_hashval = None 
        self.state = 'normal'     # normal, managing1, managing2, awaiting
        self.timer = 0.0
        
        # local stats
        self.routing_cache = {}
        self.peer_latencies = (0,0) # (# records, weighted average)
        self.failed_peers = {}
        self.bad_peers = {}

        self.backup = None # disc copy of db and routing state

    def update(self, val, ballot_num):
        print("updating state: ballot #{}".format(ballot_num))
        if not ',' in val:
            # requires unpackaging
            length, data = val.split('<>')
            val = int(data).to_bytes(int(length), byteorder='little').decode()

        t,k,v = val.split(',')
        
        if t == 'P':
            self.db.put(k,v)
        elif t == 'D':
            self.db.delete(k)
        elif t == 'G': # congregate requests
            if k == 'locked':
                elapsed = time.time() - self_timer
                if self.state == 'normal' or (self.state == 'locked' and elapsed > 3):
                    self.group_p1_hashval = v
                    self.state = 'locked'
                    self.state_timer = time.time()
                    self.logger.debug("state is now locked. Locked value: {}".format(v))

            elif self.state == 'locked' and k == 'commit':
                vhash = SHA.new(val.encode()).hexdigest()
                if vhash == self.group_p1_hashval:
                    self.group_update(v)
                    self.state = 'normal'
            else:
                self.logger.info("got bad group request: ignoring")
    
    def group_update(self, opList):
        # TODO modify for rollbackable
        self.logger.info("performing group operations: {}".format(opList))
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
                elif t == 'M': # Migrate peer: a is wss, b is destination group
                    pubkey = self.groups[g].peers[a]
                    self.groups[b].peers[a] = pubkey
                    self.groups[g].remove_peer(a)
                else:
                    print("unrecognized group op: {}".format(item))

        except Exception as e:
            self.logger.info("got bad value in group update: {}".format(e))



    def image_state(self):
        # create disc copy of system state
        backupData = [self.bpcon.state.db, self.bpcon.state.groups]
        dataBytes = pickle.dumps(backupData)
        dataInts = int.from_bytes(dataBytes, byteorder='little')

        newBackup = str(int(time.time())) + "_backup.pkli"
        with open(newBackup, 'w') as fh:
            fh.write(dataInts)

        self.backup = newBackup


from BPCon.routing import GroupManager
from BPCon.storage import InMemoryStorage
from Crypto.Hash import SHA
from collections import OrderedDict
import pickle
import time
import zlib

class StateManager:
    """ Provides system state variables and functions """
    def __init__(self, conf):
        self.log = conf['log']

        # Congregate
        self.addr = conf['p_wss']
        self.groups = {}
        self.groups['G0'] = GroupManager(conf) # (members, keyspace)
        self.groups['G1'] = GroupManager(conf) # overwritten by BPCon local group
        self.groups['G2'] = GroupManager(conf)
        self.group_p1_hashval = None 
        self.lock = 'normal'     # normal, locked,  managing1, managing2, awaiting
        self.timer = 0.0
        
        # BPCon
        self.db = InMemoryStorage()

        # local stats
        self.routing_cache = {}
        self.peer_latencies = (0,0) # (# records, weighted average)
        self.failed_peers = {}
        self.bad_peers = {}

    def update_wss(self, wss):
        """ Update websocket port XXX0 to XXX1 """
        return wss[:-1]+"1"

    def update(self, ballot_num=-1, val=None):
        self.log.debug("updating state: ballot #{}, op: {}".format(ballot_num, val))
        if not ',' in val:
            # requires unpackaging
            try:
                length, data = val.split('<>', 1) 
                val = int(data).to_bytes(int(length), byteorder='little').decode()
            except:
                self.log.critical("bad update value received. no changes to state")
                return

        t,k,v = val.split(',', 2)
        self.log.debug("optype: {}, key: {}, value: {}".format(t, k,v))
        
        #DB requests
        if t == 'P':
            self.db.put(k,v)
        elif t == 'D':
            self.db.delete(k)
        if t == 'A': # Adding peer: k is wss, v is key
            self.groups['G1'].add_peer(k,v)
        
        #Group membership requests
        elif t == "S": # Split: v is initiator wss
            self.log.info("splitting")
            peers = sorted(list(self.groups['G1'].peers.keys())) # array of wss
            l = int(len(peers)/2)
            list_a = peers[:l]
            list_b = peers[l:] 
            keyspace = self.groups['G1'].keyspace
            diff = (keyspace[1] - keyspace[0]) / 2
            mid = keyspace[0] + diff
            
            if self.addr in list_a:
                self.log.debug("I am in group a")
                self.groups['G2'].peers = OrderedDict()
                for wss in list_b:
                    self.groups['G2'].peers[self.update_wss(wss)] = self.groups['G1'].peers[wss]
                    del self.groups['G1'].peers[wss]
                self.groups['G1'].keyspace = (keyspace[0],mid)
                self.groups['G2'].keyspace = (mid,keyspace[1])
                return self.db.split(0,mid)

            elif self.addr in list_b:
                self.log.debug("I am in group b")
                self.log.debug(peers)
                self.log.debug(list_b)
                self.groups['G0'].peers = OrderedDict()
                for wss in list_a:
                    self.groups['G0'].peers[self.update_wss(wss)] = self.groups['G1'].peers[wss]
                    del self.groups['G1'].peers[wss]
                self.groups['G0'].keyspace = (keyspace[0],mid)
                self.groups['G1'].keyspace = (mid,keyspace[1])
                return self.db.split(1, mid) 
            
            else:
                self.log.error("inconsistent state. not participating in split operation!")
        

        # congregate requests 
        elif t == 'G': 
            if k == 'lock': #op is lock request
                self.log.debug("attempting to acquire lock")
                elapsed = time.time() - self.timer
                if self.lock == 'normal' or (self.lock == 'locked' and elapsed > 3):
                    self.group_p1_hashval = v
                    self.lock = 'locked'
                    self.timer = time.time()
                    self.log.debug("lock acquired. Locked hashvalue: {}".format(v))

            elif k == "commit" and self.lock == 'locked': #op is prepped(locked) group op
                    # verify group op
                    optype, group, b64_files = v.split(';', 2)
                    optype_and_group = optype + "," + group + ","
                    vhash = SHA.new(optype_and_group.encode()).hexdigest()
                    if vhash != self.group_p1_hashval:
                        self.log.info("locked state, rejecting invalid commit value: {}".format(val))
                        return

                    # make a backup copy of current state
                    self.image_state()
                    
                    # update and ensure rollback in case of update failure
                    try:
                        self.group_update(k,v)
                        self.lock = 'normal'
                        return 0

                    except:
                        self.load_state()
                        self.lock = 'normal'
                        return 1

            else:
                self.log.info("got bad group request: ignoring")


    def group_update(self, group, opList):
        """ atomic state updates inside lock """
        
        self.log.info("performing group operations: {}".format(opList))
        try:
            for item in opList.split('<>'): #  keyspace and group membership #TODO <> in data can break this? remove mult-op suppport?
                
                #opType, group, data = item.split(';')
                opType, group, data = item.split(';', 2)
                
                if group not in ['G0','G2']:
                    raise ValueError("not a valid group")
                
                # reverse groups since views are mirror images
                tg = 'G0' 
                if group == 'G0':
                    tg = 'G2'

                if opType == 'M': # Merge
                    self.log.debug("merging!!!")

                    pubkey = self.groups[tg].peers[a]
                    self.groups[b].peers[a] = pubkey
                    self.groups[tg].remove_peer(a)
                
                #elif t == 'K':
                #    self.groups[g].keyspace = (float(a),float(b))
                
                else:
                    self.log.info("unrecognized group op: {}".format(item))

        except Exception as e:
            self.log.info("got bad value in group update: {}".format(e))

    def get_compressed_state(self):
        self.log.debug("compressing state...")
        toStore = pickle.dumps([self.groups['G0'].get_peers(), self.groups['G1'].get_peers(),
                                self.groups['G2'].get_peers(), self.db])
        compressed = zlib.compress(toStore)
        return compressed

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

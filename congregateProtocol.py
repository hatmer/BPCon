"""
State

Each group maintains up-to-date knowledge of the two neigh-
boring groups that immediately precede and follow it in the
key-space. These consistent lookup links form a global ring
topology, on top of which Scatter layers a best-effort rout-
ing policy based on cached hints.

handles multi-op requests from other groups
makes state-change requests to peers through BPCon

"""
import asyncio
import pickle
import time
from Crypto.Hash import SHA

from BPCon.routing import RoutingManager

class CongregateProtocol:
    def __init__(self, loop, conf, bpcon):
        self.loop = loop
        self.logger = conf['logger']
        self.address = "wss://"+conf['ip_addr']+":"+str(conf['port']+1)
        self.bpcon = bpcon

        self.keyspace = (0,1)
        self.neighbors = [RoutingManager(),RoutingManager()]
        self.routing_cache = {}
        
        self.backup = None # disc copy of db and routing state

    def create_peer_request(self, request_type):
       	"""
        Internal group consensus messages for:
            - split
            - merge
            - migrate
            - keyspace change
            - modify peer group membership

        Message format:  local/other, change type, data

        0: paxos operation
        1: routing
        2: congregate operation

        1: keyspace
        2: group membership
        3: both

        data: keyspace, groupmember sockets with corresponding keys

        """
        msg = "0&"
        if request_type == "split":
            # action: divide group and keyspace in half           
            pass
        elif request_type == "remove_peer": 
            # action: change paxos routing membership
            pass
        elif request_type == "add_peer": 
            # action: change paxos routing membership
            pass
        else: # multigroup operations 
            msg = "2&"              
            if request_type == "merge":
                # ks, peerlist
                pass
            elif request_type == "migrate":
                # peerlist
                pass
            elif request_type == "keyspace":
                # ks
                pass

        return msg
            

    def got_commit_result(self, future):
        if future.done():
            #do cleanup
            if not future.cancelled():
                logger.info("commit result: {}".format(future.result()))
            else:
                logger.info("future cancelled")
        else:    
            logger.info("future not done ???")


    @asyncio.coroutine
    def bpcon_commit(self, msg):
        bpcon_task = asyncio.Future()
        bpcon_task.add_done_callback(self.got_commit_result)
        try:
            timer_result = asyncio.wait_for(bpcon_task, 3.0) # timer possibly unneccessary
            commit_result = yield from self.bpcon.phase1a(msg, bpcon_task) 
            print(commit_result)
            return commit_result
        except asyncio.TimeoutError:
            self.logger.info("db commit timed out")
        except asyncio.CancelledError:
            self.logger.info("db commit future cancelled")
        except Exception as e:
            self.logger.debug(e)

    @asyncio.coroutine
    def handle_2pc_request(self, request):
        """
        Another group requests 2pc
        """
        # assess suitability
        # acquire group lock
        if self.bpcon.state.state == 'normal':
            self.logger.debug("attempting to acquire lock")
            # attempt to acquire lock
            requesthash = SHA.new(request.encode()).hexdigest()
            msg = "L, {}, {}".format(self.address, requesthash)
            
            res = yield from self.bpcon_commit(msg)
            self.logger.debug("returning {}".format(res))
            return res
            
        elif self.bpcon.state.state == 'managing1':
            #msg = ",{},{}".format()
            print("Done!")
            #yield from self.bpcon_commit(msg)
            
         
            

        else:
            # other 2pc request currently being processed
            pass

    @asyncio.coroutine
    def make_2pc_request(self, request):
        """
        Contact another group with update/request
        Notify other neighbor if applies to them
        """
        
        yield from self.bpcon.send_msg(request, ["wss://127.0.0.1:9002"])
        
	
    @asyncio.coroutine
    def main_loop(self, websocket, path):
        try:
            print("got input")
            input_msg = yield from websocket.recv()
            self.logger.debug("< {}".format(input_msg))
            output_msg = yield from self.handle_2pc_request(input_msg)
            self.logger.debug("output_msg is {}".format(output_msg))
            if output_msg:
                yield from websocket.send(output_msg)
                #self.bmsgs.append(output_msg)
                
            else:
                self.logger.error("got bad input from peer")

            # adapt here

        except Exception as e:
            self.logger.error("mainloop exception: {}".format(e))









    def handle_join(self, request):
        # ungrouped peer sent a join request
        # notifiy neighbors of membership change
        pass

    def keyspace_update(self, update):
        pass

    def groupmem_update(self, update):
        pass

    def image_state(self):
        # create disc copy of system state
        backupData = [self.bpcon.state.db, self.bpcon.peers]
        dataBytes = pickle.dumps(backupData)
        dataInts = int.from_bytes(dataBytes, byteorder='little')

        newBackup = str(int(time.time())) + "_backup.pkli"
        with open(newBackup, 'w') as fh:
            fh.write(dataInts)

        self.backup = newBackup
    def reconfig_manager(self):
        """
        Autonomous Functions
            - initiate split
            - remove failed peer
            - detect malicious peers
            - queue and reduce communications with other groups
            - image db and routing state
        """
        pass





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
import time
from Crypto.Hash import SHA
from BPCon.routing import GroupManager
from algorithms import Autobot

class CongregateProtocol:
    def __init__(self, loop, conf, bpcon):
        self.conf = conf
        self.loop = loop
        self.logger = conf['logger']
        self.address = conf['c_wss']
        self.bpcon = bpcon

        self.keyspace = (0,1)
        

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
    def bpcon_request(self, msg):
        self.logger.debug("making request: {}".format(msg)) 
        bpcon_task = asyncio.Future()
        bpcon_task.add_done_callback(self.got_commit_result)
        try:
            timer_result = asyncio.wait_for(bpcon_task, 3.0) # timer possibly unneccessary
            commit_result = yield from self.bpcon.request(msg, bpcon_task) # returns boolean
            self.logger.info("bpcon request result: {}".format(commit_result))
            return commit_result

        except asyncio.TimeoutError:
            self.logger.info("bpcon commit timed out")
        except asyncio.CancelledError:
            self.logger.info("bpcon commit future cancelled")
        except Exception as e:
            self.logger.debug(e)

    @asyncio.coroutine
    def handle_2pc_request(self, request):
        """
        Another group requests 2pc
        """
        phase = request[:2]
        op = request[3:]
        if phase == "P1":  # is P1 (got requesthash)
            self.logger.info("remote P1 requesthash is {}".format(op))
            local_p1_msg = "G,{},{}".format("lock", op)
            lock_acquired = yield from self.bpcon_request(local_p1_msg) # acquire group lock
            response = "P1,ACK"
        elif phase == "P2": # is P2 
            # assess suitability (phase vs. state)
            self.logger.info("remote P2 request to be committed is {}".format(op))
            # TODO check input lots (is a proper request, hashes to locked value, is a suitable group op)
            p2_success = yield from self.bpcon_request(op)
            response = "P2,ACK"
        else:
            self.logger.error("peer sent bad 2pc request")

        if lock_acquired:                
            self.logger.debug("returning {}".format(response))
            return response
            
            
        #else:
            # other 2pc request currently being processed
            #pass
        

    @asyncio.coroutine
    def make_2pc_request(self, request, recipients=[]):
        """
        Multigroup update/request
        Notify neighbors of update 
        """
        # 1. acquire lock internal consensus 
        # TODO first check lock state
        if self.bpcon.state.state == "locked":
            return "failure: another group operation in progress"
        requesthash = SHA.new(request.encode()).hexdigest()
        local_p1_msg = "G,{},{}".format("lock", requesthash)
        lock_acquired = yield from self.bpcon_request(local_p1_msg) # request lock from local group
        self.logger.debug("bpcon lock acquired: {}".format(lock_acquired))
        
        if lock_acquired: 
            if len(recipients) == 0:
                # doing a local group operation
                request_success = yield from self.bpcon_request(request)
                if request_success:
                    self.logger.info("local group operation complete")
            else:
                # make 2pc P1 request to remote group
                remote_p1_msg = "P1,{}".format(requesthash)
                self.logger.info("sending {}".format(remote_p1_msg))
                p1_response = yield from self.bpcon.send_msg(remote_p1_msg, recipients)
                self.logger.info("P1 response: {}".format(p1_response)) 
            
                if p1_response == "P1,ACK":  
	                # commit operation locally
                    p2_success = yield from self.bpcon_request(request) # local_p2_msg
                
                    if p2_success: 
                        self.logger.info("P2 local commit succeeded. sending P2 request")
                        remote_p2_msg = "P2,{}".format(request)
                        p2_response = yield from self.bpcon.send_msg(remote_p2_msg, recipients)
                        if p2_response == "P2,ACK":
                            self.logger.info("Congregate 2pc request completed successfully")
            

    @asyncio.coroutine
    def main_loop(self, websocket, path):
        try:
            input_msg = yield from websocket.recv()
            self.logger.info("< {}".format(input_msg))
            output_msg = yield from self.handle_2pc_request(input_msg)
            
            if output_msg:
                self.logger.info("> {}".format(output_msg))
                yield from websocket.send(output_msg) # ACKs for P1 and P2
                #self.bmsgs.append(output_msg)
                
            else:
                self.logger.error("no consensus on input from peer")

            # adapt here
            # get_reconfig()

        except Exception as e:
            self.logger.error("mainloop exception: {}".format(e))

    @asyncio.coroutine
    def handle_join(self, wss, pubkey, cert):
        # ungrouped peer sent a join request
        # test peer
        # add to local routing table
        request = "A,{},{}<>{}".format(wss,pubkey,cert)
        added = yield from self.bpcon_request(request)
        # notifiy neighbors of membership change
        

    def keyspace_update(self, update):
        pass

    def groupmem_update(self, update):
        pass

    def create_peer_request(self, request_type, args=None):
        """
        Formats internal group consensus messages for:
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
        opList = []
        #msg = "0&" # prepend if conf['use_single_port'] = 1
        if request_type == "split":
            # action: divide group and keyspace in half
            # Initiator node goes in first group
            # 1. get new groups and keyspaces
            self.logger.debug("constructing split request")
            peers = sorted(self.bpcon.state.groups['G1'].peers)
            l = int(len(peers)/2)
            list_a = peers[:l]
            list_b = peers[l:] # these nodes will be in the new group

            keyspace = self.bpcon.state.groups['G1'].keyspace

            diff = (keyspace[1] - keyspace[0]) / 2
            mid = keyspace[0] + diff

            opList.append("S,G1,{}".format(self.conf['c_wss']))

            #for node in list_b:
            #    opList.append("M;G1;{};G1".format(node))
            
            #opList.append("K;G1;{};{}".format(keyspace[0],mid))
            #opList.append("K;G1;{};{}".format(mid,keyspace[1]))

        elif request_type == "remove_peer":
            op = "R;G1;{};".format(wss,None)
            print(op)
        elif request_type == "add_peer":
            op = "A;G1;{};{}".format(wss,key)
            print(op)
        else: # multigroup operations 
            msg = "2&"
            if request_type == "merge":
                # ks, peerlist
                print("merging groups in dict: {}".format(args.keys()))
            elif request_type == "migrate":
                #"G,commit,M;G1;wss://localhost:9000;G1" 
                pass
            elif request_type == "keyspace":
                # ks
                pass
        return "".join(opList)
        #return "G,commit,{}".format("<>".join(opList))

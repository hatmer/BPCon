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

class CongregateProtocol:
    def __init__(self, loop, conf, bpcon):
        self.conf = conf
        self.loop = loop
        self.logger = conf['logger']
        self.address = "wss://"+conf['ip_addr']+":"+str(conf['port']+1)
        self.bpcon = bpcon

        self.keyspace = (0,1)
        self.neighbors = [GroupManager(),GroupManager()]
        

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

            #return commit_result
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
            local_p1_msg = "G,{},{}".format("locked", op)
            res = yield from self.bpcon_request(local_p1_msg) # acquire group lock
            response = "P1,ACK"
        elif phase == "P2": # is P2 
            # assess suitability (phase vs. state)
            self.logger.info("remote P2 request to be committed is {}".format(op))
            # TODO check input lots (is a proper request, hashes to locked value, is a suitable group op)
            res = yield from self.bpcon_request(op)
            response = "P2,ACK"
        else:
            self.logger.error("peer sent bad 2pc request")

        if res == "success":                
            self.logger.debug("returning {}".format(response))
            return response
            
            
        #else:
            # other 2pc request currently being processed
            #pass
        

    @asyncio.coroutine
    def make_2pc_request(self, request, recipients):
        """
        Contact another group with update/request
        Notify other neighbor if applies to them
        """
        # 1. acquire lock internal consensus 
        # TODO first check lock state
        if self.bpcon.state.state == "locked":
            return "failure: another group operation in progress"
        requesthash = SHA.new(request.encode()).hexdigest()
        local_p1_msg = "G,{},{}".format("locked", requesthash)
        res = yield from self.bpcon_request(local_p1_msg) # request lock from local group
        self.logger.debug("result from bpcon lock request: {}".format(res))
        
        if res == "success": # lock acquired
            if len(recipients) == 0:
                # doing a local group operation
                res = yield from self.bpcon_request(request)
                if res == "success":
                    self.logger.info("local group operation complete")
            else:
                # make 2pc P1 request to remote group
                remote_p1_msg = "P1,{}".format(requesthash)
                self.logger.info("sending {}".format(remote_p1_msg))
                res = yield from self.bpcon.send_msg(remote_p1_msg, recipients)
                self.logger.info("P1 response: {}".format(res)) 
            
                if res == "P1,ACK":  
	                # commit operation locally
                    res = yield from self.bpcon_request(request) # local_p2_msg
                
                    if res == "success": 
                        self.logger.info("P2 local commit succeeded. sending P2 request")
                        remote_p2_msg = "P2,{}".format(request)
                        res = yield from self.bpcon.send_msg(remote_p2_msg, recipients)
                        if res == "P2,ACK":
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
            #self.reconfig()

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

    def reconfig(self):
        """
        Autonomous Functions
            - initiate split
            - remove failed peer
            - detect malicious peers
            - queue and reduce communications with other groups
            - image db and routing state
        """
        self.logger.info("reconfig did nothing")
       
    




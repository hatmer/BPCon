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
from Congregate.algorithms import Autobot
from BPCon.utils import encode_for_transport, decode_to_bytes
from random import randint


class CongregateProtocol:
    def __init__(self, loop, conf, bpcon):
        self.conf = conf
        self.loop = loop
        self.log = conf['log']
        self.address = conf['c_wss']
        self.bpcon = bpcon
        self.keyspace = (0,1)


    ### Interface with BPCon in-memory database ###

    def got_commit_result(self, future):
        """ Callback for BPCon commit: effectively a timout """
        if future.done():
            #do cleanup
            if not future.cancelled():
                self.log.info("commit result: {}".format(future.result()))
            else:
                self.log.info("future cancelled")
        else:    
            self.log.info("future not done ???")


    @asyncio.coroutine
    def bpcon_request(self, msg, future=None):
        """ Commit update to BPCon in-memory database """
        self.log.debug("making request: {}".format(msg)) 
        bpcon_task = asyncio.Future()
        bpcon_task.add_done_callback(self.got_commit_result)
        try:
            timer_result = asyncio.wait_for(bpcon_task, 3.0) # timer possibly unneccessary
            commit_result = yield from self.bpcon.request(msg, bpcon_task) # returns boolean
            self.log.info("bpcon request result: {}".format(commit_result))

            # repeatedly attempt to commit while failure was due to resync
            if commit_result['code'] == 2:
                commit_result = yield from self.bpcon_request(msg)
            
            if commit_result['code'] == 0:
                if future: # processing a request for client
                    future.set_result('OK')
                print("check OK")
                return True
            #else:
            #    if future:
            #        future.set_result('NO')
            #    return False
            #return commit_result  # always returns code = 0 or code = 1

        except asyncio.TimeoutError:
            self.log.info("bpcon commit timed out")
        except asyncio.CancelledError:
            self.log.info("bpcon commit future cancelled")
        except Exception as e:
            self.log.debug("bpcon commit misc. error: {}".format(e))
        
        if future:
            future.set_result('NO')
        return False


    ### Interface with other groups ###

    def sanity_check(self, op, value):
        """ security check on 2pc inputs from other group """
        if len(op) > 1:
            self.log.critical("refusing 2pc request. op too long: {}".format(op))
            return False

        if op == 'M':
            # value is group to merge with
            if value == 'G0' or value == 'G2':
                size_of_other_group = len(self.bpcon.state.groups[value].get_peers())
                size_of_my_group = len(self.bpcon.state.groups['G1'].get_peers())

                if (size_of_other_group + size_of_my_group) <= self.conf['MAX_GROUP_SIZE']:
                    return True

        return False

        # sanity check other types of group ops here

    @asyncio.coroutine
    def handle_2pc_request(self, request):
        """
        Process 2pc request initiated by another group
        """
        self.log.info("handling 2pc request: {}".format(request))    
        phase, op = request.split(",", 1)

        lock_acquired = True # prediction
        if phase == "P1": 

            is_locked = (self.bpcon.state.lock == "locked")
            if is_locked:
                self.log.debug("refusing 2pc request, state already locked")
                return

            action, group = op.split(",", 1)

            newgroup = "G2" if (group == 'G0') else "G0"

            if not self.sanity_check(action, newgroup):
                self.log.critical("refusing 2pc request: sanity check fails on input: {}".format(request))
                return

            self.log.info("remote P1 requested op is {}".format(action))
            local_request = "{},{},".format(action, newgroup)
            requesthash = SHA.new(local_request.encode()).hexdigest()

            local_p1_msg = "G,{},{}".format("lock", requesthash)
            lock_acquired = yield from self.bpcon_request(local_p1_msg) # acquire group lock
            data = encode_for_transport(self.bpcon.state.get_compressed_state())
            response = "P1ACK,{}".format(data)

        elif phase == "P2": # is P2 
            action, group, b64_files = op.split(";", 2)

            # switch to mirror view
            newgroup = "G2" if (group == 'G0') else "G0"
            newop = action+";"+newgroup+";"+b64_files  # TODO improve

            # assess suitability (phase vs. state)
            self.log.info("remote P2 request to be committed is {}".format(newop))
            requesthash = SHA.new((action+","+newgroup+",").encode()).hexdigest()
            if self.bpcon.state.group_p1_hashval != requesthash:
                self.log.critical("wrong group op in commit: {}".format(newop))
                return

            local_p2_msg = "G,commit,{}".format(newop)
            p2_success = yield from self.bpcon_request(local_p2_msg)
            response = "P2ACK"

        else:
            self.log.error("peer sent bad 2pc request")

        if lock_acquired:                
            self.log.debug("returning {}".format(response))
            return response      
        

    @asyncio.coroutine
    def make_2pc_request(self, request_type, target_group, future):
        """
        Initiate multigroup update/request and notify neighbors of update
        """ 
        if self.bpcon.state.lock == "locked":
            self.log.debug("2pc failure: another group operation in progress")
            future.set_result("NO")
            return

        # get members of target group
        group_members = self.bpcon.state.groups[target_group].get_peers()
        if len(group_members) == 0:
            self.log.error("initiated 2pc request with empty group {}".format(target_group))
            future.set_result("NO")
            return

        # acquire lock internal consensus
        request = "{},{},".format(request_type, target_group)
        requesthash = SHA.new(request.encode()).hexdigest()
        self.log.debug("{} -> {}".format(request, requesthash))

        local_p1_msg = "G,{},{}".format("lock", requesthash)
        lock_acquired = yield from self.bpcon_request(local_p1_msg) # request lock from local group
        self.log.debug("bpcon lock acquired: {}".format(lock_acquired))
        
        if lock_acquired: 
            # make 2pc P1 request to remote group
            remote_p1_msg = "P1,{},{}".format(request_type,target_group) 
            
            recipient = [group_members[randint(0,len(group_members)-1)]]# random member of group_members
            self.log.debug("sending {} to {}".format(remote_p1_msg, recipient))
            p1_response = yield from self.bpcon.send_msg(remote_p1_msg, recipient) #TODO break out send_msg
            self.log.debug("P1 response: {}".format(p1_response))
        
            if p1_response == None:
                # quit because other group did not respond
                # TODO will it return None if the other group says no? other cases?
                self.log.debug("2pc failure because other group response is None")
                future.set_result("NO")
                return

            self.log.debug("P1 response is: {}".format(p1_response))
            if p1_response.startswith("P1ACK"):   
                # commit operation locally
                try:
                    b64_files = ""
                    if len(p1_response) > 3:
                        b64_files = p1_response[6:]

                    # make a backup copy of current state
                    stateCopy = self.bpcon.state

                    # local commit
                    self.log.debug("initiating local commit 2pc response")
                    opList  = "{};{};".format(request_type, target_group)
                    request = "G,commit,{}".format(opList) + b64_files

                    compressed = encode_for_transport(self.bpcon.state.get_compressed_state())

                    p2_success = yield from self.bpcon_request(request) # local_p2_msg
                
                    if p2_success: 
                        self.log.debug("P2 local commit succeeded. sending P2 request")
                        self.log.debug("compressed state is {}".format(compressed))
                        
                        request = "{};{};{}".format(request_type, target_group, compressed)
                        remote_p2_msg = "P2,{}".format(request)
                        p2_response = yield from self.bpcon.send_msg(remote_p2_msg, recipient)
                        
                        if p2_response == "P2ACK":
                            self.log.info("Congregate 2PC request completed successfully")
                            future.set_result('OK')
                            return True # 2pc succeeded
                        else:
                            self.bpcon.state = stateCopy
                            self.log.info("2PC request failed. p2 response not received. Local state not changed")
                    else:
                        self.log.info("2PC request failed. Local commit failed. Local state not changed")
                except Exception as e:
                    self.log.info("2PC request failed: {} Local state not changed".format(e))
            else:
                self.log.info("non P1 response recieved: {}".format(p1_response))
        else:
            self.log.info("could not acquire lock")
        
        future.set_result('NO')
        return False # 2pc failed
            
    def copy_state(self):
        self.stateCopy = self.bpcon.state.image_state()


    ### Internal regulatory functions ###
    
    @asyncio.coroutine
    def main_loop(self, websocket, path):
        try:
            input_msg = yield from websocket.recv()
            self.log.info("# bytes recieved: {}".format(len(input_msg)))
            self.log.debug("received {} from {}".format(input_msg, websocket.remote_address))
            output_msg = yield from self.handle_2pc_request(input_msg)
            
            if output_msg:
                self.log.debug("> {}".format(output_msg))
                yield from websocket.send(output_msg) # ACKs for P1 and P2
                #self.bmsgs.append(output_msg)
                
            else:
                self.log.error("no consensus on input from peer")

        except Exception as e:
            self.log.error("mainloop exception: {}".format(e))


    @asyncio.coroutine
    def add_peer(self, wss, pubkey, cert, group):
        """ perform join request for ungrouped peer """
        # TODO test peer creds
        self.log.debug("peer group is: {}".format(self.bpcon.state.groups['G1'].peers))
        self.log.info("adding peer {} to group {}".format(wss, group)) # TODO write to file for autobot
        
        request = "A,{},{}<><><>{}".format(wss,pubkey,cert)
        res = yield from self.bpcon_request(request)
        self.log.info("join request result: {}".format(res)) 
        return res # True or False for bpcon commit success
        
        # TODO notify neighbor groups of membership change

    @asyncio.coroutine
    def remove_peer(self, wss, group):
        # TODO implement protocol
        pass

    def keyspace_update(self, update):
        #TODO implement gossip protocol
        pass


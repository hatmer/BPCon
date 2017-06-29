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
    def bpcon_request(self, msg):
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

            return commit_result  # always returns code = 0 or code = 1

        except asyncio.TimeoutError:
            self.log.info("bpcon commit timed out")
        except asyncio.CancelledError:
            self.log.info("bpcon commit future cancelled")
        except Exception as e:
            self.log.debug("bpcon commit misc. error: {}".format(e))


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
        if len(request) != 7:
            self.log.critical("refusing 2pc request. Invalid length of request: {} -> {}".format(request, len(request)))
            return
            
        phase, op, group = request.split(",")
        if phase == "P1": 

            if not self.sanity_check(op, group):
                self.log.critical("refusing 2pc request: sanity check on input fails")
                return

            self.log.info("remote P1 requested op is {}".format(op))
            local_request = "{},{},".format(op, group)
            requesthash = SHA.new(local_request.encode()).hexdigest()


            local_p1_msg = "G,{},{}".format("lock", requesthash)
            lock_acquired = yield from self.bpcon_request(local_p1_msg) # acquire group lock
            response = "P1ACK,{}".format(self.bpcon.state.get_compressed_state())

        elif phase == "P2": # is P2 
            # assess suitability (phase vs. state)
            self.log.info("remote P2 request to be committed is {}".format(op))
            # TODO check input lots (is a proper request, hashes to locked value, is a suitable group op)

            requesthash = SHA.new(op.encode()).hexdigest()

            p2_success = yield from self.bpcon_request(op)
            response = "P2ACK"
        else:
            self.log.error("peer sent bad 2pc request")

        if lock_acquired:                
            self.log.debug("returning {}".format(response))
            return response      
        #else:
            # other 2pc request currently being processed
            #pass
        

    @asyncio.coroutine
    def make_2pc_request(self, request_type, target_group):
        """
        Initiate multigroup update/request and notify neighbors of update
        """ 
        if self.bpcon.state.lock == "locked":
            self.log.debug("2pc failure: another group operation in progress")
            return

        # get members of target group
        group_members = self.bpcon.state.groups[target_group].get_peers()
        if len(group_members) == 0:
            self.log.error("initiated 2pc request with empty group {}".format(target_group))
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
            self.log.info("sending {} to {}".format(remote_p1_msg, recipient))
            p1_response = yield from self.bpcon.send_msg(remote_p1_msg, recipient) #TODO break out send_msg
            self.log.info("P1 response: {}".format(p1_response))
        
            if p1_response == None:
                # quit because other group did not respond
                # TODO will it return None if the other group says no? other cases?
                self.log.debug("2pc failure because other group response is None")
                return

            if p1_response.startswith("P1ACK"):   
                # commit operation locally
                try:
                    b64_files = ""
                    if len(p1_response) > 3:
                        b64_files = p1_response[3:]
                        # extract pickled and compressed state data from other group
                        #[g0, g1, g2, db] = zlib.decompress(pickle.loads(b64_files))
                        # sanity check groups and db keyspace values

                    # make a backup copy of current state
                    stateCopy = self.bpcon.state

                    # local commit
                    self.log.debug("initiating local commit 2pc response")
                    opList  = "{};{};{}".format(request_type, target_group, b64_files)
                    request = "G,commit,{}".format(opList)
                    p2_success = yield from self.bpcon_request(request) # local_p2_msg
                
                    if p2_success: 
                        self.log.info("P2 local commit succeeded. sending P2 request")
                        compressed = self.bpcon.state.get_compressed_state()
                        request = "{};{};{}".format(request_type, target_group, compressed)
                        remote_p2_msg = "P2,{}".format(request)
                        p2_response = yield from self.bpcon.send_msg(remote_p2_msg, recipients)
                        if p2_response == "P2ACK":
                            self.log.info("Congregate 2PC request completed successfully")
                            try:
                                # TODO modify state etc.
                                pass
                            except:
                                self.log.error("state modification failed")
                        else:
                            self.log.info("2PC request failed. p2 response not received. Local state not changed")
                    else:
                        self.log.info("2PC request failed. Local commit failed. Local state not changed")
                except:
                    self.log.info("2PC request failed. p1 response handling failed. Local state not changed")
            else:
                self.log.info("non P1 response recieved: {}".format(p1_response))
        else:
            self.log.info("could not acquire lock")
            
    def copy_state(self):
        self.stateCopy = self.bpcon.state.image_state()

    ### Internal regulatory functions ###

    #@asyncio.coroutine
    #def smart_recv(self, sock):
        

    @asyncio.coroutine
    def main_loop(self, websocket, path):
        try:
            input_msg = yield from websocket.recv()
            self.log.info("received {} from {}".format(input_msg, websocket.remote_address))
            output_msg = yield from self.handle_2pc_request(input_msg)
            
            if output_msg:
                self.log.info("> {}".format(output_msg))
                yield from websocket.send(output_msg) # ACKs for P1 and P2
                #self.bmsgs.append(output_msg)
                
            else:
                self.log.error("no consensus on input from peer")

            # adapt here
            # get_reconfig()

        except Exception as e:
            self.log.error("mainloop exception: {}".format(e))

    @asyncio.coroutine
    def handle_join(self, wss, pubkey, cert):
        """ perform join request for ungrouped peer """
        # TODO test peer creds
        self.log.info("peer group is: {}".format(self.bpcon.state.groups['G1'].peers))
        self.log.info("adding peer...")
        # add to local-group routing table
        request = "A,{},{}<>{}".format(wss,pubkey,cert)
        res = yield from self.bpcon_request(request)
        self.log.info("join request result: {}".format(res))
        
        # TODO notify neighbor groups of membership change
        

    def keyspace_update(self, update):
        pass

    def groupmem_update(self, update):
        pass

    @asyncio.coroutine
    def request_split(self):
        self.log.info("initiating split request")
        yield from self.bpcon_request("S,,")

    @asyncio.coroutine
    def request_merge(self, targetGroup):
        self.log.info("initiating merge request")
        yield from self.make_2pc_request("M", targetGroup)


    def create_request(self, request_type, args=None):
        """
        Formats internal group consensus messages for:
            - split
            - merge
            - migrate & add/remove peer
            - keyspace change
            - modify peer group membership

        Message format:  local/other, change type, data
ss
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
            self.log.debug("constructing split request")
            opList.append("S,,")

        elif request_type == "remove":
            op = "R;G1;{};".format(wss,None)
            print(op)
        elif request_type == "add":
            op = "A;G1;{};{}".format(wss,key)
            print(op)
        else: # multigroup operations 
            #msg = "2&"
            if request_type == "merge":
                pass
                #opList.append("M,
            elif request_type == "keyspace":
                # ks
                pass
        
        return "<>".join(opList)
        #return "G,commit,{}".format("<>".join(opList))

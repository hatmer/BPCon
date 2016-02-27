"""
State

Each group maintains up-to-date knowledge of the two neigh-
boring groups that immediately precede and follow it in the
key-space. These consistent lookup links form a global ring
topology, on top of which Scatter layers a best-effort rout-
ing policy based on cached hints.

"""
import asyncio

class CongregateProtocol:
    def __init__(self, loop, conf, bpcon):
        self.loop = loop
        self.logger = conf['logger']
        self.bpcon = bpcon
        self.neighbors = [[],[]]
        self.routing_cache = {}
        self.keyspace = (0,1)



    def handle_peer_request(self, request):
       	"""
        Internal group consensus achieved on:
            - split
            - merge
            - migrate
            - keyspace change
            - remove peer
        """
        pass

    def make_peer_request(self, request):
        """
        Attempt to get group consensus on keyspace/grouping changes

        """
        bpcon_task = asyncio.Future()
        bpcon_task.add_done_callback(self.got_commit_result)
        
        try:
            timer_result = asyncio.wait_for(bpcon_task, 3.0) # catch exceptions, verify result
            db_commit_task = self.db_commit(request, bpcon_task)
            self.loop.run_until_complete(db_commit_task)
        except Exception as e:
            self.logger.info("exception caught in commit: {}".format(e))
        
    @asyncio.coroutine
    def db_commit(self, msg, future):
        try:
            yield from self.bpcon.phase1a(msg, future)
        except asyncio.TimeoutError:
            logger.info("db commit timed out")
        except asyncio.CancelledError:
            logger.info("db commit future cancelled")

    def got_commit_result(self, future):
        if future.done():
            #do cleanup
            if not future.cancelled():
                logger.info("commit result: {}".format(future.result()))
            else:
                logger.info("future cancelled")
        else:    
            logger.info("future not done ???")


    def handle_group_request(self, request):
        """
        Another group requests 2pc
        """
        pass

    def make_group_request(self, request):
        """
        Contact another group with update/request
        Notify other neighbor if applies to them
        """
        pass
	
    # Client Request Handling
    def handle_db_request(self, request):
        # client request for data
        # route if necessary (manage for client)
        # verification of request and requestor permissions
        # self.db.get(k)
        pass

    @asyncio.coroutine
    def main_loop(self, websocket, path):
        logger.debug("checkpoint 1")
        try:
            input_msg = yield from websocket.recv()
            print(input_msg)
            yield from websocket.send("hello")
        except Exception as e:
            logger.debug(e)


    def handle_join(self, request):
        # ungrouped peer sent a join request
        # notifiy neighbors of membership change
        pass

    def keyspace_update(self, update):
        pass

    def groupmem_update(self, update):
        pass

    def reconfig_manager(self):
        """
        Autonomous Functions
            - initiate split
            - remove failed peer
            - detect malicious peers
            - queue and reduce communications with other groups
        """
        pass





import asyncio
import websockets
import logging
import ssl
import time
logger = logging.getLogger('websockets')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

logger2 = logging.getLogger('websockets.server')
logger2.setLevel(logging.ERROR)
logger2.addHandler(logging.StreamHandler())

from BPCon.quorum import Quorum
from BPCon.utils import get_ssl_context
from BPCon.routing import RoutingManager

"""
######### Message Formats ##########
1a-N
1b-N-{v,-1}-set(2avmsgs in (val,bal) format)-sig
1b-N-v-nack-sig
1c-N,v 
2b-N,v,acc
B: 1aMsg U 1bMsg U 1cMsg U 2avMsg U 2bMsg

"""
#peers = ['wss://localhost:9000/']

class BPConProtocol:
    def __init__(self):
        self.maxBal = -1
        self.maxVBal = -1 # highest ballot voted in
        self.maxVVal = None # value voted for maxVBal
        self.avs = {} # msg dict keyed by value (max ballot saved only)
        self.seen = {} #1b messages -> use to check if v is safe at b
        self.bmsgs = []
        self.peers = RoutingManager(['wss://localhost:9000/'])
        self.Q = None
        self.val = ""
        self.pending = None

    def sentMsgs(self, type_, bal):
        pass

    def knowsSafeAt(ac, b, v):
        pass

    @asyncio.coroutine
    def phase1a(self, val, future):
        logger.debug("sending 1a")
        #check for prior quorum being managed
        # bmsgs := bmsgs U ("1a", bal)
        self.maxBal += 1
        self.Q = Quorum(self.maxBal)
        self.val = val
        self.pending = future
        tosend = "&1a&{}".format(self.maxBal)
        yield from self.send_msg(self.peers.peers,tosend)

    @asyncio.coroutine
    def phase1b(self, N):
        logger.debug("sending 1b")
        if (N > int(self.maxBal)): #maxbal undefined behavior sometimes 
            self.maxBal = N
        tosend = "&1b&{}&{}&{}&mysig".format(N,self.maxVBal,self.maxVVal,self.avs)
        return tosend
        # bmsgs := bmsgs U ("1b", bal, acceptor, self.avs, self.maxVBal, self.maxVVal)
            

    #@asyncio.coroutine
    def phase1c(self):
        logger.debug("sending 1c")
        # bmsgs := bmsgs U ("1c", bal, val)
        tosend = "&1c&{}&{}&{}".format(self.Q.N, self.val, self.Q.get_signatures())
        return tosend

    @asyncio.coroutine
    def phase2av(self, b):
        if (self.maxBal <= b) and (r.bal < b for r in self.avs):
            # m in (1c, b) sentMsgs and b safe at vals from sentmsgs
            # bmsgs := bmsgs U ("2av", m.bal, m.val, acceptor)
            # remove r from avs where r.val == m.val
            self.maxBal = b

    #@asyncio.coroutine
    def phase2b(self, b,v):
        if int(self.maxBal) <= int(b): # undefined maxbal here also
                # exists (2av, b) pairing in sentMsgs that quorum of acceptors agree upon
                # bmsgs := bmsgs U ("2b", m.bal, m.val, acceptor)
            
            self.maxVVal = v

            self.maxBal = b
            self.maxVBal = b

            return "&2b&{}&{}".format(b,v)

    @asyncio.coroutine
    def main_loop(self, websocket, path):
        """
        server socket
        """ 
        logger.debug("main loop")
        # idle until receives network input
        try:
            input_msg = yield from websocket.recv()
            logger.debug("got input")
            output_msg = yield from self.handle_msg(input_msg)
            if output_msg:
                yield from websocket.send(str(time.time()) + output_msg)
                self.bmsgs.append(output_msg)
                print(self.bmsgs)
            else:
                logger.debug("did nothing")
        except Exception as e:
            logger.info(e)

    @asyncio.coroutine    
    def handle_msg(self, msg):    
        parts = msg.split('&')
        num_parts = len(parts)
        timestamp = parts[0]
        msg_type = parts[1]
        N = int(parts[2])
        output_msg = None
        
        if msg_type == "1a" and num_parts == 3:
            # a peer is leader for a ballot, requesting votes
            if N > int(self.maxBal):
                output_msg = yield from self.phase1b(N)

        elif msg_type == "1b" and num_parts == 6:
            # implies is leader for ballot, has quorum object
            logger.debug("got 1b!!!")
            [mb,mv,sig] = parts[3:6]

            # do stuff with v and 2avs
            # update avs structure here for newest ballot number for value
            self.Q.add(N,int(mb), mv,sig)
            if self.Q.is_quorum():
                logger.info("got quorum")
                if self.Q.got_majority_accept():
                    logger.info("quorum accepts")
                    output_msg = self.phase1c()
                    yield from self.send_msg(self.peers.peers,output_msg)
                else:
                    logger.info("quorum rejects")
                    self.Q = None
                    self.pending.set_result("commit failed")
                    
                 
        elif msg_type == "1c" and num_parts == 5:
            logger.debug("got 1c")
            [N,v,sigs] = parts[2:5]
            signatures = sigs.split(',')
            if len(signatures) <= self.peers.num_peers:
                for sig in sigs.split(','):
                    # test against pubkey
                    logger.debug("testing sig here...")
                    logger.debug(sig)
                output_msg = self.phase2b(N,v)

        elif msg_type == "2b" and num_parts == 4:
            logger.debug("got 2b")
            self.pending.set_result("Done!!!") 
        else:
            logger.info("non-paxos msg received")
        
        return output_msg

    @asyncio.coroutine
    def send_msg(self, dest_wss, to_send):
        """
        client socket
        """
        cctx = get_ssl_context('trusted/')
        for ws in dest_wss:
            try:
                client_socket = yield from websockets.client.connect(ws, ssl=cctx)
                yield from client_socket.send(str(time.time()) + to_send)
                input_msg = yield from client_socket.recv()
                yield from self.handle_msg(input_msg)
                #logger.info("got input msg: {}".format(input_msg)) 
                yield from client_socket.close()
            except Exception as e:
                logger.info(e)
                logger.info("ERROR EXCEPTION AAAAHHH") # better debug messages



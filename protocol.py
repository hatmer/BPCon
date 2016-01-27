import asyncio
import websockets
import logging
import ssl
import datetime
logger = logging.getLogger('websockets')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

from quorum import Quorum
from utils import get_ssl_context

"""
######### Message Formats ##########
1a-N
1b-N-{v,-1}-set(2avmsgs in (val,bal) format)-sig
1b-N-v-nack-sig
1c-N,v 
2b-N,v,acc
B: 1aMsg U 1bMsg U 1cMsg U 2avMsg U 2bMsg

"""
peers = ['wss://localhost:9000/']

class BPConProtocol:
    def __init__(self):
        self.maxBal = -1
        self.maxVBal = -1 # highest ballot voted in
        self.maxVVal = None # value voted for maxVBal
        self.avs = {} # msg dict keyed by value (max ballot saved only)
        self.seen = {} #1b messages -> use to check if v is safe at b
        self.bmsgs = []
        self.Q = None

    def sentMsgs(self, type_, bal):
        pass

    def knowsSafeAt(ac, b, v):
        pass

    @asyncio.coroutine
    def phase1a(self):
        logger.debug("sending 1a")
        #check for prior quorum being managed
        # bmsgs := bmsgs U ("1a", bal)
        self.maxBal += 1
        self.Q = Quorum(self.maxBal) 
        tosend = "{}&1a&{}".format(datetime.datetime.now().isoformat(), self.maxBal)
        yield from self.send_msg(peers,tosend)
        

    @asyncio.coroutine
    def phase1b(self, N):
        logger.debug("sending 1b")
        if (N > self.maxBal):  
            self.maxBal = N
            tosend = "&1b&{}&{}&{}&mysig".format(N,self.maxVBal,self.maxVVal,self.avs)
            return tosend
            # bmsgs := bmsgs U ("1b", bal, acceptor, self.avs, self.maxVBal, self.maxVVal)

    @asyncio.coroutine
    def phase1c(self):
        # bmsgs := bmsgs U ("1c", bal, val)
        val = "?" # specific value or all values safe
        tosend = "&1c&{}&{}&{}".format(self.Q.N, val, self.Q.acceptor_sigs)
        return tosend

    @asyncio.coroutine
    def phase2av(self, b):
        if (self.maxBal <= b) and (r.bal < b for r in self.avs):
            # m in (1c, b) sentMsgs and b safe at vals from sentmsgs
            # bmsgs := bmsgs U ("2av", m.bal, m.val, acceptor)
            # remove r from avs where r.val == m.val
            self.maxBal = b

    @asyncio.coroutine
    def phase2b(self, b):
        if self.maxBal <= b:
                # exists (2av, b) pairing in sentMsgs that quorum of acceptors agree upon
                # bmsgs := bmsgs U ("2b", m.bal, m.val, acceptor)
            
            self.maxVVal = v

            self.maxBal = b
            self.maxVBal = b
            value = "?"

            return "&2b&{}".format(value)

    @asyncio.coroutine
    def main_loop(self, websocket, path):
        logger.debug("main loop")
        # idle until receives network input
        try:
            input_msg = yield from websocket.recv()
            if len(input_msg) < 4:
                raise Exception()
        except:
            logger.info("Bad input")
            return
        
        parts = input_msg.split('&')
        num_parts = len(parts)
        timestamp = parts[0]
        msg_type = parts[1]
        N = int(parts[2])
        
        if msg_type == "1a" and num_parts == 3:
            # a peer is leader for a ballot, requesting votes
            
            output_msg = yield from self.phase1b(N)

        elif msg_type == "1b" and num_parts == 6:
            # implies is leader for ballot, has quorum object
            logger.debug("got 1b!!!")
            [v,vs,sig] = input_msg.split(delim)[3:6]

            # do stuff with v and 2avs
            # update avs structure here for newest ballot number for value
            self.Q.add(N,v,sig)
            if self.Q.is_quorum():
                logger.info("got quorum")
                if self.Q.got_majority_accept():
                    logger.info("quorum accepts")
                    output_msg = yield from self.phase1c()
                    logger.debug("sending 1c")
                    yield from self.send_msg(peers,output_msg)
                else:
                    logger.info("quorum rejects")
                    self.Q = None
                 
        elif msg_type == "1c" and num_parts == 5:
            logger.debug("got 1c")
            [N,v,sigs] = input_msg.split(delim)[1:5]
            
            for sig in sigs:
                # test against pubkey
                pass
            self.phase2b(N,v)

        elif msg_type == "2b" and num_parts == 4:
            logger.debug("got 2b")
    
        else:
            logger.debug("non-paxos msg received")
            output_msg = "b"
        # got client request -> send 1a
        # got 1a -> send 1b with avs
        # got 1b -> add avs to obj
        # got 1c -> send 2b 
        # got 2b -> process client request
        # generate output from socket input
        #print("< {}".format(input_msg))
        
        output_msg = datetime.datetime.now().isoformat() + output_msg
        yield from websocket.send(output_msg)
        #print("> {}".format(output_msg))
        
        self.bmsgs.append(output_msg)
        print(self.bmsgs)
        

    @asyncio.coroutine
    def send_msg(self, dest_wss, to_send):
        cctx = get_ssl_context('trusted/')
        for ws in dest_wss:
            websocket = yield from websockets.client.connect(ws, ssl=cctx)
        
            try:
                yield from websocket.send(to_send)
                input_msg = yield from websocket.recv()
                logger.info("got input msg: {}".format(input_msg))  
            except:
                logger.info("send failed")
            finally:
                yield from websocket.close()



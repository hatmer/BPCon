import asyncio
import websockets
import logging
import ssl
import time
#self.logger = logging.getLogger('websockets')
#self.logger.setLevel(logging.CRITICAL)
#self.logger.addHandler(logging.StreamHandler())

#self.logger2 = logging.getLogger('websockets.server')
#self.logger2.setLevel(logging.ERROR)
#self.logger2.addHandler(logging.StreamHandler())

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

class BPConProtocol:
    def __init__(self, peer_certs, logger, peers):
        self.logger = logger
        self.peer_certs = peer_certs
        self.maxBal = -1
        self.maxVBal = -1       # highest ballot voted in
        self.maxVVal = None     # value voted for maxVBal
        self.avs = {}           # msg dict keyed by value (max ballot saved only)
        self.seen = {}          # 1b messages -> use to check if v is safe at b
        self.bmsgs = []         # log of messages sent by this instance
        self.peers = RoutingManager(peers)
        self.Q = None
        self.val = ""
        self.pending = None
        self.ctx = get_ssl_context(self.peer_certs)

    def sentMsgs(self, type_, bal):
        pass

    def knowsSafeAt(ac, b, v):
        pass

    @asyncio.coroutine
    def phase1a(self, val, future):
        # bmsgs := bmsgs U ("1a", bal)
        if self.Q:
            self.logger.error("prior quorum being managed for ballot {}".format(self.Q.N))
        #else    
        self.maxBal += 1
        self.Q = Quorum(self.maxBal, self.peers.num_peers)
        self.logger.debug("creating Quorum object with N={}, num_peers={}".format(self.maxBal, self.peers.num_peers))
        self.pending = future
        self.logger.debug("sending 1a -> {}: {}".format(self.maxBal, val))
        msg_1a = "&1a&{}".format(self.maxBal)

        yield from asyncio.async(self.send_msg(msg_1a))
        
        if self.pending.done():
            # Error case
            return "cancelled"
        
        if self.Q.quorum_1b():
            if self.Q.got_majority_accept():
                self.logger.info("quorum accepts")
                yield from self.send_msg(self.phase1c(val)) # send 1c
                if self.Q.quorum_2b():
                    # Quorum Accepts and Commit succeeds case
                    self.pending.set_result("success")
                    return "accepted"
                # Quorum Accepts then Commit fails case    
                self.pending.set_result("commit failure")    
                return "commit failure"    
            else:
                # Quorum Rejects case
                self.pending.set_result("quorum rejects")
                return "rejected"

        else:
            self.pending.set_result("failure")
            return "no quorum"    
            
        
    def phase1b(self, N):
        # bmsgs := bmsgs U ("1b", bal, acceptor, self.avs, self.maxVBal, self.maxVVal)
        self.logger.debug("sending 1b -> {}".format(N))
        tosend = "{}&1b&{}&{}&{}".format(str(time.time()),N,self.maxVBal,self.maxVVal,self.avs)
        if (int(N) > int(self.maxBal)): #maxbal undefined behavior sometimes 
            self.maxBal = N
        tosend = tosend + "&mysig"
        return tosend
            
    def phase1c(self, val):
        # bmsgs := bmsgs U ("1c", bal, val)
        self.logger.debug("sending 1c -> {}: {}".format(self.Q.N, val))
        tosend = "&1c&{}&{}&{}".format(self.Q.N, val, self.Q.get_signatures())
        return tosend

    @asyncio.coroutine
    def phase2av(self, b):
        if (self.maxBal <= b) and (r.bal < b for r in self.avs):
            # m in (1c, b) sentMsgs and b safe at vals from sentmsgs
            # bmsgs := bmsgs U ("2av", m.bal, m.val, acceptor)
            # remove r from avs where r.val == m.val
            self.maxBal = b

    def phase2b(self, b, v):
        # bmsgs := bmsgs U ("2b", m.bal, m.val, acceptor)
        if int(self.maxBal) <= int(b): # undefined maxbal here also
            # exists (2av, b) pairing in sentMsgs that quorum of acceptors agree upon
            
            self.maxVVal = v
            self.maxBal = b
            self.maxVBal = b

            return "{}&2b&{}&{}".format(str(time.time()), b, v)

    @asyncio.coroutine
    def main_loop(self, websocket, path):
        """
        server socket for receiving 1a messages
        """ 
        self.logger.debug("main loop")
        # idle until receives network input
        try:
            input_msg = yield from websocket.recv()
            self.logger.debug("< {}".format(input_msg))
            output_msg = self.handle_msg(input_msg)
            if output_msg:
                yield from websocket.send(output_msg)
                self.bmsgs.append(output_msg)
                print(self.bmsgs)
            else:
                self.logger.debug("did nothing")
        except Exception as e:
            self.logger.info("mainloop exception: {}".format(e))
#        print("Pending tasks after mainloop: %s" % asyncio.Task.all_tasks(asyncio.get_event_loop()))    


    def preprocess_msg(self, msg): # TODO verification
        return msg.split('&')

    def handle_msg(self, msg):    
        parts = self.preprocess_msg(msg)
        num_parts = len(parts)
        timestamp = parts[0]
        msg_type = parts[1]
        N = int(parts[2])
        
        if msg_type == "1a" and num_parts == 3:
            # a peer is leader for a ballot, requesting votes
            output_msg = self.phase1b(N)
            return output_msg
            
        elif msg_type == "1b" and num_parts == 6:
            # implies is leader for ballot, has quorum object
            self.logger.debug("got 1b!!!")
            [mb,mv,sig] = parts[3:6]

            # do stuff with v and 2avs
            # update avs structure here for newest ballot number for value
            self.Q.add_1b(N,int(mb), mv, sig)
                 
        elif msg_type == "1c" and num_parts == 5:
            self.logger.debug("got 1c")
            [N,v,sigs] = parts[2:5]
            signatures = sigs.split(',')
            if len(signatures) <= self.peers.num_peers:
                for sig in signatures:
                    # test against pubkey
                    self.logger.debug("testing sig here...")
                    self.logger.debug(sig)
                output_msg = self.phase2b(N,v)
                return output_msg

        elif msg_type == "2b" and num_parts == 4:
            self.logger.debug("got 2b")
            self.Q.add_2b(N)
        else:
            self.logger.info("non-paxos msg received")

    @asyncio.coroutine
    def send_msg(self, to_send):
        """
        client socket 
        """
        for ws in self.peers.get_all():
            try:
                self.logger.debug("sending {} to {}".format(to_send, ws))
                client_socket = yield from websockets.connect(ws, ssl=self.ctx)
                yield from client_socket.send(str(time.time()) + to_send)
                input_msg = yield from client_socket.recv()
                self.handle_msg(input_msg)
            except Exception as e:
                self.logger.info("send to peer {} failed with {}".format(ws, e))
                
            finally:
                try:
                    yield from client_socket.close()
                except Exception as e:
                    self.logger.debug("no socket to close")
        
        #print("Pending tasks after send: %s" % asyncio.Task.all_tasks(asyncio.get_event_loop())) 

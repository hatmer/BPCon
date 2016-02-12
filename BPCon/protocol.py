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
    def __init__(self, peer_certs, logger):
        self.logger = logger
        self.peer_certs = peer_certs
        self.maxBal = -1
        self.maxVBal = -1       # highest ballot voted in
        self.maxVVal = None     # value voted for maxVBal
        self.avs = {}           # msg dict keyed by value (max ballot saved only)
        self.seen = {}          # 1b messages -> use to check if v is safe at b
        self.bmsgs = []         # log of messages sent by this instance
        self.peers = RoutingManager(['wss://localhost:9000/'])
        self.Q = None
        self.val = ""
        self.pending = None
        self.ctx = get_ssl_context(self.peer_certs)

    def sentMsgs(self, type_, bal):
        pass

    def knowsSafeAt(ac, b, v):
        pass
    def send_cleanup(self, future):
        if future.done():   
            self.logger.debug("send returned {}".format(future.result()))
        else:
            self.logger.debug("cleanup fail")

    @asyncio.coroutine
    def phase1a(self, val, future):
        #check for prior quorum being managed
        # bmsgs := bmsgs U ("1a", bal)
        self.maxBal += 1
        self.Q = Quorum(self.maxBal)
        self.logger.debug("creating Quorum object with N={}".format(self.maxBal))
        self.val = val
        self.pending = future
        self.logger.debug("sending 1a -> {}: {}".format(self.maxBal, self.val))
        tosend = "&1a&{}".format(self.maxBal)

        #quorum_response = yield from self.send_msg(tosend)
        quorum_response = yield from asyncio.async(self.send_msg(tosend))
        if quorum_response == "accepted":
            #verify = yield from self.send(self.phase1c())
            #if verify == "committed":
            self.pending.set_result("accepted")

        elif quorum_response == "rejected":
            self.pending.set_result("rejected")
            
        #return quorum_response
            
    @asyncio.coroutine
    def phase1b(self, N):
        # bmsgs := bmsgs U ("1b", bal, acceptor, self.avs, self.maxVBal, self.maxVVal)
        self.logger.debug("sending 1b -> {}".format(N))
        tosend = "{}&1b&{}&{}&{}".format(str(time.time()),N,self.maxVBal,self.maxVVal,self.avs)
        if (N > int(self.maxBal)): #maxbal undefined behavior sometimes 
            self.maxBal = N
        tosend = tosend + "&mysig"

        return tosend
            
    def phase1c(self):
        self.logger.debug("sending 1c -> {}: {}".format(self.Q.N, self.val))
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

    def phase2b(self, b, v):
        if int(self.maxBal) <= int(b): # undefined maxbal here also
            # exists (2av, b) pairing in sentMsgs that quorum of acceptors agree upon
            # bmsgs := bmsgs U ("2b", m.bal, m.val, acceptor)
            
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
            output_msg = yield from self.handle_msg(input_msg)
            if output_msg:
                yield from websocket.send(output_msg)
                self.bmsgs.append(output_msg)
                print(self.bmsgs)
            else:
                self.logger.debug("did nothing")
        except Exception as e:
            self.logger.info(e)
#        print("Pending tasks after mainloop: %s" % asyncio.Task.all_tasks(asyncio.get_event_loop()))    


    def preprocess_msg(self, msg): # TODO verification
        return msg.split('&')

    def handle_msg(self, msg):    
        parts = self.preprocess_msg(msg)
        num_parts = len(parts)
        timestamp = parts[0]
        msg_type = parts[1]
        N = int(parts[2])
        output_msg = None
        
        if msg_type == "1a" and num_parts == 3:
            # a peer is leader for a ballot, requesting votes
            #if N > int(self.maxBal):
            output_msg = self.phase1b(N)
            
        elif msg_type == "1b" and num_parts == 6:
            # implies is leader for ballot, has quorum object
            self.logger.debug("got 1b!!!")
            [mb,mv,sig] = parts[3:6]

            # do stuff with v and 2avs
            # update avs structure here for newest ballot number for value
            self.Q.add(N,int(mb), mv, sig)
            print(self.Q.is_quorum())
            if self.Q.is_quorum():
                self.logger.info("got quorum")
                if self.Q.got_majority_accept():
                    self.logger.info("quorum accepts")
                    return "accepted"
                else:   
                    self.logger.info("quorum rejects")
                    return "rejected"
            else:
                return "pending"
#                    self.logger.info("quorum rejects")
#                    if self.pending.done():
#                        self.logger.info("cancelled_1")
#                    else:    
#                        self.pending.set_result("rejected")
                    
                 
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

        elif msg_type == "2b" and num_parts == 4:
            self.logger.debug("got 2b")
            if self.pending.done():
                self.logger.info("cancelled_2")
            else:    
                self.pending.set_result("successful") 
        else:
            self.logger.info("non-paxos msg received")
        return output_msg 
    @asyncio.coroutine
    def send_msg(self, to_send):
        """
        client socket
        """
        connected = []
        res = "no_peers"
        for ws in self.peers.peers:
            try:
                client_socket = yield from websockets.connect(ws, ssl=self.ctx)
                yield from client_socket.send(str(time.time()) + to_send)
                connected.append(client_socket)
            except Exception as e:
                self.logger.info("peer {} unreachable".format(ws))
                
        for s in connected:
            try:
                input_msg = None
                try:
                    input_msg = yield from asyncio.wait_for(client_socket.recv(), 1.0) 
                except Exception as e:
                    self.logger.debug("recv from peer failed: {}".format(e))
                if input_msg:    
                    self.logger.debug("got input from peer: {}".format(input_msg))
                    res = self.handle_msg(input_msg)
                    self.logger.debug(res)
                else:
                    self.logger.debug("peer sent nothing")
            except Exception as e:
                self.logger.info("peer unresponsive: {}".format(e)) 
            finally:
                try:
                    yield from client_socket.close()
                except Exception as e:
                    self.logger.debug("no socket to close")
        
        #print("Pending tasks after send: %s" % asyncio.Task.all_tasks(asyncio.get_event_loop())) 
        return res

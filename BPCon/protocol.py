import asyncio
import websockets
import logging
import ssl
import time

from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA
from Crypto.PublicKey import RSA

from BPCon.quorum import Quorum
from BPCon.utils import get_ssl_context
from BPCon.routing import RoutingManager
from BPCon.storage import InMemoryStorage
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
    def __init__(self, peer_certs, keyfile, logger, peerlist, peer_keys):
        self.logger = logger
        self.peer_certs = peer_certs
        self.maxBal = -1
        self.maxVBal = -1       # highest ballot voted in
        self.maxVVal = None     # value voted for maxVBal
        self.avs = {}           # msg dict keyed by value (max ballot saved only)
        self.seen = {}          # 1b messages -> use to check if v is safe at b
        self.bmsgs = []         # log of messages sent by this instance
        self.peers = RoutingManager(peerlist, peer_keys)
        self.Q = None
        with open(keyfile, 'r') as fh:
            key = RSA.importKey(fh.read())
        self.signer = PKCS1_v1_5.new(key)
        self.pending = None
        self.ctx = get_ssl_context(self.peer_certs)
        self.db = InMemoryStorage()

    def shutdown(self):
        # clean shutdown
        # close server
        # save routing state
        # save db state
        # print stats
        pass
       
    def update_db(self, val):
        length, data = val.split('<>')
        kvbytes = int(data).to_bytes(int(length), byteorder='little')
        
        k,v = kvbytes.decode().split(',')
        self.db.put(k,v)

    def sentMsgs(self, type_, bal):
        pass

    def knowsSafeAt(ac, b, v):
        pass

    @asyncio.coroutine
    def phase1a(self, proposal, future):
        # bmsgs := bmsgs U ("1a", bal)
        #if self.Q:
        #    self.logger.error("Bad Client: prior quorum being managed for ballot {}".format(self.Q.N))
        if not ',' in proposal:
            self.logger.error("db commit proposal not in key,value format")
            return

        self.maxBal += 1
        self.Q = Quorum(self.maxBal, self.peers.num_peers)
        self.logger.debug("creating Quorum object with N={}, num_peers={}".format(self.maxBal, self.peers.num_peers))
        self.pending = future
        self.logger.debug("sending 1a -> {}: {}".format(self.maxBal, proposal))
        msg_1a = "1a&{}&{}".format(str(time.time()),self.maxBal)

        yield from asyncio.async(self.send_msg(msg_1a))
        
        if self.pending.done():
            # Error case
            self.logger.error("future done before voting complete")
        else:
            if self.Q.quorum_1b():
                if self.Q.got_majority_accept():
                    self.logger.debug("quorum accepts")
                    val_bytes = proposal.encode()
                    length = len(val_bytes)
                    prepped_val = str(length)+"<>"+str(int.from_bytes(val_bytes, byteorder='little'))
                    yield from self.send_msg(self.phase1c(prepped_val)) # send 1c
                    if self.Q.quorum_2b():
                        # Quorum Accepts and Commit succeeds case
                        res = "success"
                        self.logger.info("Quorum Accepted Value {} at Ballot {}".format(proposal,self.Q.N))
                        k,v = proposal.split(',')
                        self.db.put(k,v)
                    else:
                         # Quorum Accepts then Commit fails case
                        res = "failure: quorum commit failure"    
                else:
                    # Quorum Rejects case
                    res = "failure: quorum rejects"

            else:
                res = "failure: no quorum"

        if not self.pending.done():
            self.pending.set_result(res)
        
    def phase1b(self, N):
        # bmsgs := bmsgs U ("1b", bal, acceptor, self.avs, self.maxVBal, self.maxVVal)
        msg_1b = "1b&{}&{}&{}&{}&{}".format(str(time.time()),N,self.maxVBal,self.maxVVal,self.avs)
        if (int(N) > int(self.maxBal)): #maxbal undefined behavior sometimes 
            self.maxBal = N
        
        msg_hash = SHA.new(msg_1b.encode())
        sig = self.signer.sign(msg_hash)
        
        tosend = msg_1b + ";" + str(int.from_bytes(sig, byteorder='little'))
        self.logger.debug("sending 1b -> {}".format(tosend))
        return tosend
            
    def phase1c(self, val):
        # bmsgs := bmsgs U ("1c", bal, val)
        self.logger.debug("sending 1c -> {}: {}".format(self.Q.N, val))
        tosend = "1c&{}&{}&{}&;{}".format(str(time.time()),self.Q.N, val, self.Q.get_msgs())
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

            self.logger.info("Value accepted by BPCon: {}".format(v))
            self.update_db(v)
            tosend = "2b&{}&{}&{}".format(str(time.time()), b, v)
            return tosend

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
                #print(self.bmsgs)
            else:
                self.logger.error("got bad input from peer")
        except Exception as e:
            self.logger.error("mainloop exception: {}".format(e))
        self.logger.debug("Pending tasks after mainloop: %i" % len(asyncio.Task.all_tasks(asyncio.get_event_loop())))    


    def preprocess_msg(self, msg): # TODO verification
        
        return msg.split('&')

    def handle_msg(self, msg, peer_wss=None):    
        msg_type = msg[:2]
        if msg_type == "1c":
            msg,proofs = msg.split('&;')
        parts = self.preprocess_msg(msg)
        num_parts = len(parts)
        timestamp = parts[1]
        N = int(parts[2])

        
        if msg_type == "1a" and num_parts == 3:
            # a peer is leader for a ballot, requesting votes
            output_msg = self.phase1b(N)
            return output_msg
            
        elif msg_type == "1b" and num_parts == 6:
            # implies is leader for ballot, has quorum object
            self.logger.debug("got 1b!!!")
            [mb,mv,avs] = parts[3:6]
            # do stuff with v and 2avs
            # update avs structure here for newest ballot number for value
            if N == self.Q.N:
                self.Q.add_1b(int(mb), msg, peer_wss)
            else:
                self.logger.error("got bad 1b msg")
                 
        elif msg_type == "1c" and num_parts == 4:
            self.logger.debug("got 1c")
            v = parts[3]
            signed_msgs = proofs.split(',') # 1b proofs in wss;msg;sig format
            if len(signed_msgs) <= self.peers.num_peers:
                # test against pubkey
                self.logger.debug("testing sig here...")
                num_verified = self.peers.verify_sigs(signed_msgs)
                
                if num_verified >= self.peers.quorum_size():
                    self.logger.debug("signature verification succeeded")
                    output_msg = self.phase2b(N,v)
                    return output_msg
                else:
                    self.logger.error("signature verification failed")
            else:
                self.logger.error("too many signatures")
 
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
                yield from client_socket.send(to_send)
                input_msg = yield from client_socket.recv()
                self.handle_msg(input_msg, ws)
            except Exception as e: # custom error handling
                self.logger.info("send to peer {} failed".format(ws))
            finally:
                try:
                    yield from client_socket.close()
                except Exception as e:
                    self.logger.debug("no socket to close")
        

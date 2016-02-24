import hashlib
import os
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA
from Crypto.PublicKey import RSA

class RoutingManager(object):
    """
    This class manages peers for BPCon

    stores secure websockets keyed to IP address 
    
    manages peer pubkeys and certificates
    
    """
    def __init__(self, initlist=[], key_dir="/"):
        self.peers = {}
        self.uptime = {}
        for wss in initlist:
            fname = key_dir + self.get_ID(wss)+".pubkey"
            if os.path.exists(fname):
                #read key and add pair to self.peers
                with open(fname, 'r') as fh:
                    self.peers[wss] = RSA.importKey(fh.read()) 
                    self.uptime[wss] = 0
            else:
                print("missing key file for {}".format(wss))
        
        self.num_peers = len(self.peers)

    def quorum_size(self):
        return int((self.num_peers / 2) + (self.num_peers % 2))

    def get_ID(self, sock_str):
        encoded =  hashlib.sha1(sock_str.encode())
        return encoded.hexdigest()

    def add_peer(self, ip, port, key):
        sock_str = "wss://"+str(ip)+":"+str(port)
        ID = self.get_ID(sock_str)
        if not sock_str in self.peers.keys():
            key = str(key)
            self.peers[sock_str] = RSA.importKey(key) #error check here
            self.uptime[sock_str] = 0
            # write key to file
            with open(ID+".pubkey", 'w') as fh:
                fh.write(key)

            self.num_peers += 1
            return True

        return False    

    def remove_peer(self, wss):
        if self.peers[wss]:
            self.peers.pop(wss, None)
            self.uptime.pop(wss, None)
            self.num_peers -= 1
            return True
        else:
            print("remove failed")
            return False

    def get_all(self):
        # returns list of addresses of peers in group
        return self.peers.keys()

    def verify_sigs(self, msglist):
        num_verified = 0
        
        for item in msglist:
            wss, msg, sig = item.split(';')
            rsakey = self.peers[wss]
            h = SHA.new(msg.encode())

            sigmsg = int(sig).to_bytes(256, byteorder='little')
            verifier = PKCS1_v1_5.new(rsakey)
            if verifier.verify(h, sigmsg):
                num_verified += 1
        
        return num_verified

    def report_stats(self, wss, is_online): # 0 for false, 1 for true
        # unresponsive peers recorded
        self.uptime[wss] += is_online
    
    def print_stats(self):
        print(self.uptime)

    def save_state(self):
        pass
    
    def load_state(self):
        pass


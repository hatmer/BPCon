

class RoutingManager(object):
    """
    This class manages peers for BPCon

    stores secure websockets keyed to IP address 
    
    manages peer pubkeys and certificates
    
    

    """
    def __init__(self, initlist={}):
        self.peers = initlist           #dict of ip:wss
        self.num_peers = len(initlist)

    def add_peer(self, ip, port, ID, key):
        self.peers[ip] = "wss://"+str(ip)+":"+str(port)
        s = "wss://"+str(ip)+":"+str(port)
        print(s)
        self.num_peers += 1
        # write key to file

    def remove_peer(self, ip):
        self.peers[ip] = None
        self.num_peers -= 1

    def get(self, ip):
        return self.peers[ip]
    
    def get_all(self):
        return self.peers.values()
        #return [k+v for k,v in self.peers.items()]
    
    def save_state(self):
        pass
    
    def load_state(self):
        pass


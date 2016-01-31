

class RoutingManager(object):
    def __init__(self, initlist):
        self.peers = initlist
        self.num_peers = len(initlist)

    def add_peer(self, ip, port, ID, key):
        self.peers[ip] = ":"+str(port)+":"+str(ID)
        # write key to file

    def remove_peer(self, ip, port):
        pass

    def get(self, ip):
        return self.peers[ip]
    
    def get_all(self):
        return [k+v for k,v in self.peers.items()]
    
    def save_state(self):
        pass
    
    def load_state(self):
        pass


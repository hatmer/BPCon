

class RoutingManager(object):
    def __init__(self):
        self.peers = {}
    def get(self, ip):
        return self.peers[ip]
    def get_all(self):
        return [k+v for k,v in self.peers.items()]
    def put(self, ip, port, ID):
        self.peers[ip] = ":"+str(port)+":"+str(ID)
    def save_state(self):
        pass
    def load_state(self):
        pass


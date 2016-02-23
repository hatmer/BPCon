

class Quorum(object):
    """
    manages ballots

    """
    def __init__(self, ballot_num, num_peers):
        self.N = ballot_num
        self.num_peers = num_peers
        self.quorum = int((num_peers / 2) + (num_peers % 2))
        self.acceptors = 0
        self.rejectors = 0
        self.acceptor_msgs = {} 
        self.commits = 0

    def add_1b(self, mb, msg, peer_wss):
        if peer_wss not in self.acceptor_msgs.keys():
            if mb < self.N:
                self.acceptors += 1
                self.acceptor_msgs[peer_wss] = msg
            else:
                self.rejectors += 1
                self.acceptor_msgs[peer_wss] = None # 1b nack

    def add_2b(self, N):
        if N == self.N:
            self.commits += 1

    def quorum_2b(self):
        print("commits: {}, quorum: {}".format(self.commits, self.quorum))
        return self.commits >= self.quorum

    def quorum_1b(self):
        print("acceptors: {}, rejectors: {}, quorum: {}".format(self.acceptors, self.rejectors, self.quorum))
        # returns True if majority vote achieved
        return ((self.acceptors >= self.quorum) or (self.rejectors >= self.quorum))

    def got_majority_accept(self):
        # returns True for accepted, False for rejected
        return self.acceptors >= self.rejectors
    
    def get_msgs(self):
        return ",".join(k+";"+v for (k,v) in self.acceptor_msgs.items())


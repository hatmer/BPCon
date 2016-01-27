num_peers = 1

class Quorum(object):
    def __init__(self, ballot_num):
        self.N = ballot_num
        self.num_peers = num_peers
        self.quorum = num_peers / 2
        self.acceptors = 0
        self.rejectors = 0
        self.acceptor_sigs = {} 

    def add(self, n, v, a_sig):
        if a_id not in self.acceptor_ids:
            if n <= self.N:
                self.acceptors += 1
                accepted = 1
            else:
                self.rejectors += 1
                accepted = 0

            self.acceptor_sigs[a_sig] = accepted    

    def is_quorum(self):
        # returns Truee if majority vote achieved
        return self.acceptors >= self.quorum

    def got_majority_accept(self):
        # returns True for accepted, False for rejected
        return self.acceptors >= self.rejectors

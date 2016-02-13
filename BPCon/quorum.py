

class Quorum(object):
    def __init__(self, ballot_num, num_peers):
        self.N = ballot_num
        self.num_peers = num_peers
        self.quorum = int((num_peers / 2) + (num_peers % 2))
        self.acceptors = 0
        self.rejectors = 0
        self.acceptor_sigs = {} 

    def add(self, N, mb, mv, a_sig):
        if N == self.N: # do stuff with other ballots 
            if a_sig not in self.acceptor_sigs.keys():
                if mb < self.N:
                    self.acceptors += 1
                    accepted = 1
                else:
                    self.rejectors += 1
                    accepted = 0

                self.acceptor_sigs[a_sig] = accepted    

    def is_quorum(self):
        # returns True if majority vote achieved
        return ((self.acceptors >= self.quorum) or (self.rejectors >= self.quorum))

    def got_majority_accept(self):
        # returns True for accepted, False for rejected
        return self.acceptors >= self.rejectors
    
    def get_signatures(self):
        return ",".join(self.acceptor_sigs.keys())

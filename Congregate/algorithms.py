

class Autobot:
    """
    Reconfiguration Algorithms
    -> gets stats from CongregateProtocol instance, returns action command or None

    split: partition the state of an existing group into two
    groups.
    split if group is too big

    merge: create a new group from the union of the state
    of two neighboring groups.
    merge if group is too small and a neighboring group is also small

    migrate: move members from one group to a different
    group.
    migrate to improve IP subnetting inefficiencies

    repartition: change the key-space partitioning between
    two adjacent groups
    repartition to improve load balancing
    """
    def __init__(self, conf, state):
        self.state  = state
        self.conf = conf
        self.logger = conf['logger']
        self.groups = ['G0','G1','G2']

    def print_status(self):
        # latencies
        print("Peer Latencies:")
        for group in self.groups:
            print("unimplemented")
        
        # measure of IP distributedness
        # group sizes keyspaces
        for group in self.groups:
            selector = self.state.groups[group]
            print("{}: {} peers, {} keyspace".format(group,len(selector.peers),selector.keyspace))
        
        
        # failed peers removal history
        # bad peers removal history

    def get_reconfig(self):
        """
        Autonomous Functions
            - initiate split
            - remove failed peer
            - detect malicious peers
            - queue and reduce communications with other groups
            - image db and routing state
        """
        #self.logger.info("reconfig did nothing")
        action = None

        # trim avs size

        # failed and bad peer removal (reads dict of peer:latency/fail(0)/bad(-1))

        # group size check for split/merge
        s = {'G0':0, 'G1':0, 'G2':0}
        s['G0'] = len(self.state.groups['G0'].peers)
        if s['G0'] > self.conf['MAX_GROUP_SIZE']:
            action = self.create_peer_request("split")
            
        s['G1'] = len(self.state.groups['G1'].peers)
        s['G2'] = len(self.state.groups['G2'].peers)

        # merge if combined groupsize of largest groups < #
        del s[min(s)]
        if sum(s.values()) < (self.conf['MAX_GROUP_SIZE'] / 2): # tweak this eventually
            action = self.create_peer_request("merge", s)

        # IP inefficiencies -> migrate

        return action


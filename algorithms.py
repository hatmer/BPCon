

class Reconfiguror:
    """
    Reconfiguration Algorithms
    -> gets stats from CongregateProtocol instance, returns action or None

    split: partition the state of an existing group into two
    groups.
    -> group is too big

    merge: create a new group from the union of the state
    of two neighboring groups.
    -> group is too small and a neighboring group is also small

    migrate: move members from one group to a different
    group.
    -> IP subnetting inefficiencies

    repartition: change the key-space partitioning between
    two adjacent groups
    -> ???
    """
    def __init__(self, state):
        self.state  = state
        self.groups = ['G0','G1','G2']

    def print_status(self):
        # latencies
        print("Peer Latencies:")
        for group in self.groups:
            pass
        # measure of IP distributedness
        # group sizes keyspaces
        for group in self.groups:
            selector = self.state.groups[group]
            print("{}: {} peers, {} keyspace".format(group,len(selector.peers),selector.keyspace))
        
        
        # failed peers removal history
        # bad peers removal history
        
    def get_action(self):
        # failed and bad peer removal
        size0 = len(self.state.groups['G0'].peers)
        if size0 > 8:
            print("split")
            return
        size1 = len(self.state.groups['G1'].peers)
        size2 = len(self.state.groups['G2'].peers)
        if <equation>:
            print("merge")
            return

    def should_merge(self, group1, group2):
     
        pass
    def should_migrate(self, 

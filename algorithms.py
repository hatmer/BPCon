

class Reconfiguror:
    """
    Reconfiguration Algorithms
    -> gets stats from CongregateProtocol instance, returns action command or None

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
    def __init__(self, conf, state):
        self.state  = state
        self.conf = conf
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

        # failed and bad peer removal

        # group size check for split/merge
        size0 = len(self.state.groups['G0'].peers)
        if size0 > self.conf['MAX_GROUP_SIZE']:
            action = self.create_peer_request("split")
            
        size1 = len(self.state.groups['G1'].peers)
        size2 = len(self.state.groups['G2'].peers)
        # if groupsize < #, merge
        if <equation>:
            print("merge")
            return

        # IP inefficiencies -> migrate



    def create_peer_request(self, request_type):
        """
        Internal group consensus messages for:
            - split
            - merge
            - migrate
            - keyspace change
            - modify peer group membership

        Message format:  local/other, change type, data

        0: paxos operation
        1: routing
        2: congregate operation

        1: keyspace
        2: group membership
        3: both

        data: keyspace, groupmember sockets with corresponding keys

        """
        msg = "0&"
        if request_type == "split":
            # action: divide group and keyspace in half
            # Initiator is in first group
            # 1. get new groups and keyspaces
            
            opList = ["",""]
            peers = sorted(self.state.groups['G0'].peers) 
            l = int(len(peers)/2)
            list_a = peers[:l]
            list_b = peers[l:]

            keyspace = self.state.groups['G0'].keyspace

            diff = (keyspace[1] - keyspace[0]) / 2
            mid = keyspace[0] + diff

            k1 = (keyspace[0], mid)
            k2 = (mid, keyspace[1])

            # 2. 

            print(list_a, list_b, k1, k2)


            #"G,commit, M;G0;wss://localhost:9000;G1"

        elif request_type == "remove_peer":
            op = "G,commit,A;G0;{};{}".format(wss,None)
            print(op)
        elif request_type == "add_peer":
            op = "G,commit,A;G0;{};{}".format(wss,key)
            print(op)
        else: # multigroup operations 
            msg = "2&"
            if request_type == "merge":
                # ks, peerlist
                pass
            elif request_type == "migrate":
                #"G,commit, M;G0;wss://localhost:9000;G1" 
                pass
            elif request_type == "keyspace":
                # ks
                pass

        #return "<>".join(opList)

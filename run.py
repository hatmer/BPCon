"""
Reads and maintains config file
Starts paxos and congregate protocol instances
Autonomous congregate functions loop
Handles client requests (including join requests)


"""

import asyncio
import websockets
import sys
import time
import hashlib
import os.path
from BPCon.protocol import BPConProtocol
from Congregate.cProtocol import CongregateProtocol
from Congregate.state import StateManager
from Congregate.configManager import ConfigManager, log
from BPCon.utils import shell, get_ID, get_hash_index
from time import sleep

#configFile = "config.ini"

class Congregate:
    def __init__(self, configFile):
        """ Initialize system state and components """
        try:
            # Init, check for existing state
            self.init_state(configFile) 

            # Create main event loop
            self.loop = asyncio.get_event_loop()

            # Create BPCon instance
            #self.bpcon = BPConProtocol(self.conf, self.state)
            #self.state.groups['G1'] = self.bpcon.peers
            self.paxos_server = websockets.serve(self.bpcon.main_loop, self.conf['ip_addr'], self.conf['port'], ssl=self.conf['ssl'])
            self.loop.run_until_complete(self.paxos_server)
            log.info("Started BPCon on port {}".format(self.conf['port']))

            # Create Congregate instance
            self.c = CongregateProtocol(self.loop, self.conf, self.bpcon)       
            self.congregate_server = websockets.serve(self.c.main_loop, self.conf['ip_addr'], self.conf['port']+1, ssl=self.conf['ssl'])
            self.loop.run_until_complete(self.congregate_server)
            log.info("Started Congregate on port {}".format(self.conf['port']+1))

            # Create API server
            self.web_server = websockets.serve(self.mainloop, self.conf['ip_addr'], self.conf['port']+2) 
            self.loop.run_until_complete(self.web_server)
            log.info("Started Web Server on port {}".format(self.conf['port']+2))

            # Add self to local group
            log.debug("adding self to local group")
            #self.last_config_version = 0
            self.join_my_group()
            
            # Testing 
            if self.conf['is_client']:
                #self.c.reconfig()
                log.debug("is client. making test requests")
                sleep(1) # waiting for peer to join
                for x in range(1):
                    # emulate a running system by populating kvstore and increasing ballot #
                    self.put("000", "value000")                    
                    self.put(111, "value111")
                    self.put(222, "value222")
                    self.delete("000")
                log.info("requests complete")     
                log.info("cloning state")
                self.clone()
            
            else:
                log.info("testing split...")
                self.split()
                #self.local_request("S,,")

                log.info("testing merge...")
                self.merge("G0")

        except Exception as e:
            log.info(e)

    def init_state(self, configFile):
        """
        startup routine
        loads from cloned state if present
        """

        #if os.path.isfile("data/clone.tar.gz"):
        if os.path.isfile("data/clone.tar.gz"):
            # clean working dir
            log.info("Cleaning working directory...")
            command = "rm -rf data/creds"
            #shell(command) # TODO reenable these for deploy

            # extract config, creds, and state
            log.info("Extracting cloned state...")
            command = "cd data && tar xzf clone.tar.gz && cd .."
            #shell(command)
        
        # load config
        log.info("Loading configuration...")
        self.cm = ConfigManager()
        print("config file is: ", configFile)
        self.conf = self.cm.load_config(configFile)

        # load state
        log.info("Loading state...")
        s = StateManager(self.conf)
        s.load_state()

        # Create BPCon instance and assign state object
        self.bpcon = BPConProtocol(self.conf, s)
        self.bpcon.state = s
        

    ### Request helpers ###
    
    def result_handler(self, future):
        print(future.result())
    
    def local_request(self, msg):
        f = asyncio.Future()
        asyncio.ensure_future(self.c.bpcon_request(msg,f))
        f.add_done_callback(self.result_handler)
        self.loop.run_until_complete(f)
    
    def remote_request(self, optype, arg):
        f = asyncio.Future()
        asyncio.ensure_future(self.c.make_2pc_request(optype, arg, f))
        f.add_done_callback(self.result_handler)
        self.loop.run_until_complete(f)
    

    ### Database Requests ###

    def put(self, key, value):
        """ Put a key value pair into replicated db """
        msg = "P,{},{}".format(key, value)
        # do routing thing here
        self.loop.run_until_complete(self.c.bpcon_request(msg))
        #self.local_request(msg)

    def delete(self, key):
        """ Delete key value pair from replicated db"""
        msg = "D,{},".format(key)
        # do routing thing here
        self.local_request(msg)

    ### Overlay Requests ###

    def split(self):
        """ Split local group in half """
        log.info("initiating split request")
        msg = "S,,"
        self.local_request(msg)

    def merge(self, targetGroup):
        """ Merge with another group """
        log.info("initiating merge request")
        new_groupmembers = self.c.bpcon.state.groups[targetGroup].get_peers()
        self.remote_request("M", targetGroup)
        newgroup = "G2" if (group == 'G0') else "G0"
        
        self.c.remote_request("G", val)
        #self.loop.run_until_complete(self.c.make_2pc_request("M", targetGroup))

    

    def join_my_group(self):
        """ add self to local group (as specified in config) """
        log.debug("attempting to join group specified in config...")
        try:
            with open(self.conf['certfile'], 'r') as fh:
                cert = fh.read()
            with open(self.conf['keyfile'], 'r') as fh:
                pubkey = fh.read()
            wss = self.conf['p_wss']

            msg = "A,{},{}<><><>{}".format(wss,pubkey,cert)
            
            self.local_request(msg)

            #self.loop.run_until_complete(self.c.add_peer(wss,pubkey,cert,"G1"))

        except Exception as e:
            log.debug(e)

    def remove_peer(self, wss):
        """ Remove a peer from group """
        pass
        #log.debug("removing peer: {}

        

    ### Misc. Functions ###

    def clone(self):
        """
        create a copy of db, peers, peer creds, and config
        save to compressed archive
        used to add new nodes to system
        """
        try:
            # save own credentials in clone's peer folder
            ownCert = "data/creds/local/server.crt"
            ownPubKey = "data/creds/local/server.pub"

            ID = get_ID(self.conf['p_wss'])

            certCopy = "data/creds/peers/certs/{}.crt".format(ID)
            keyCopy = "data/creds/peers/keys/{}.pub".format(ID)
            shell("cp {} {}".format(ownCert, certCopy))
            shell("cp {} {}".format(ownPubKey, keyCopy))
            # save groups and db to backup_dir
            self.c.bpcon.state.image_state()
            cloneConfigFile = configFile + "-startup"

            # TODO get these from interaction with clone
            clone_ip = "localhost"
            clone_port = "9000"
            self.cm.save_config_for_clone(clone_ip, clone_port, cloneConfigFile)
            log.debug("config saved")

            command = "cd data && tar czf clone.tar.gz {} backup creds/peers logging_config.ini".format(cloneConfigFile)
            shell(command)
            log.info("clone of state successfully created")
        
        except Exception as e:
            log.info("clone of state failed")
    
    
    def shutdown(self):
        print("\nShutdown initiated...")
        print("\nDatabase contents:\n{}".format(self.c.bpcon.state.db.kvstore)) # save state here
        self.paxos_server.close()
        self.congregate_server.close()
        print("\nPeer Groups:")
        for gname, rmgr in self.state.groups.items():
            print("{}: {} {}".format(gname, list(rmgr.peers.keys()), rmgr.keyspace))
    
    def direct_msg(self, msg):
        msg_type = msg[0]
        if msg_type == '0':
        # 0 -> bpcon
            return "hello"
        elif msg_type == '1': #custom or https -> serve or register and pass to congregate
        # 1 -> congregate
            return "hello"
        elif msg_type == '2': 
        # 2 -> external request
            self.handle_external_request(msg[1:])    
        # Client Request Handling

    def handle_db_request(self, request):
        # 1. extract and check input
        print("A")
        op, k, v = request.split(" ", 2) # I expect key to have no spaces, should hash on frontend? or use magic number

        if op not in ["GET", "PUT", "DEL"]:
            return "Invalid request"

        # if len(value) > xxx  # optional: cap maximum value size

        keyhash = get_hash_index(k)


        # check if in local keyspace
        local_keyspace = self.c.bpcon.groups['G1'].keyspace
        if keyhash >= local_keyspace[0] and keyhash < local_keyspace[1]:
            # is local
            if op == "GET":
                ret = self.c.bpcon.state.db.get(keyhash)
            elif op == "PUT":
                ret = self.put(keyhash, value)
            elif op == "DEL":
                ret = self.delete(keyhash)

        else:
            # route request (look in gossiped routing table, attempt, fail if it doesn't work)
            pass
        print("YAY!")
        return ret
        

    @asyncio.coroutine
    def mainloop(self, websocket, path):
        try:
            log.info("client connected") #TODO get info
            input_msg = yield from websocket.recv()
            log.info("< {}".format(input_msg))

            if self.conf['use_single_port']:
                output_msg = self.direct_msg(input_msg)
            else:   #using this port for external requests only
                output_msg = yield from self.handle_db_request(input_msg)
                
            if output_msg:
                yield from websocket.send(output_msg)
                #self.bmsgs.append(output_msg)
            else:
                log.error("something went wrong while processing request")
            
                
        except Exception as e:
            log.debug(input_msg)
            log.error("mainloop exception: {}".format(e))


def start():
    if len(sys.argv) > 2:
        print("Usage: python congregate.py <configfile>")
    if len(sys.argv) == 2:
        print("using custom config file")
        configFile = sys.argv[1]
        print("configFile is now: ", configFile)
    else:
        configFile = "config.ini"

    try:
        c = Congregate(configFile)
        try:
            try:
                asyncio.get_event_loop().run_forever()
            except Exception as e:
                log.info("mainloop exception")
                log.error(e)
        except KeyboardInterrupt:
            c.shutdown()
        finally:
            asyncio.get_event_loop().close()
            print('\nShutdown complete')
    except Exception as e:
        log.info("System failure: {}".format(e))

if __name__ == '__main__':
    start()
        

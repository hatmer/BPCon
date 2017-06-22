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

from BPCon.protocol import BPConProtocol
from Congregate.cProtocol import CongregateProtocol
from Congregate.state import StateManager
from Congregate.configManager import ConfigManager, log
from BPCon.utils import shell, get_ID
from time import sleep

if len(sys.argv) == 2:
    configFile = sys.argv[1]
else:
    configFile = "data/config.ini"

class Congregate:
    def __init__(self):
        """ Initialize system state and components """
        try:
            # Load state
            self.startup() # TODO make an init without cloned state an option
            # Create main event loop
            self.loop = asyncio.get_event_loop()
            # Create BPCon instance
            self.bpcon = BPConProtocol(self.conf, self.state)
            self.state.groups['G1'] = self.bpcon.peers # connect BPCon to Congregate instance
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
            self.join_request()
            

            # Testing 
            if self.conf['is_client']:
                #self.c.reconfig()
                log.debug("is client. making test requests")
                sleep(1) # waiting for peer to join
                for x in range(1):
                    # emulate a running system by populating kvstore and increasing ballot #
                    request = "P,{},hello{}".format(x,x)
                    self.local_request("P,test,value")
                    self.local_request("P,test1,value1")
                    self.local_request("P,test2,value2")
                    self.local_request("P,test,value3")
                    self.local_request("D,test2,")
                    self.local_request(request)
                log.info("requests complete")     
                log.info("cloning state")
                self.clone()
                #log.info("doing nothing...")
            else:
                #self.c.request_split()
                log.info("testing split...")
                self.local_request("S,,")

                #log.info("testing merge...")
                #self.c.request_merge("G2")
                #self.group_request("M", "G2")

        except Exception as e:
            log.info(e)

    ### State operations ###

    def startup(self):
        """
        startup routine
        Loads from cloned state
        """
        # clean working dir and extract config, creds, and state
        log.info("Cleaning working directory...")
        command = "rm -rf data/creds"
        #shell(command)
        log.info("Extracting cloned state...")
        command = "cd data && tar xzf clone.tar.gz && cd .."
        #shell(command)
        # load config
        log.info("Loading configuration...")
        self.cm = ConfigManager()
        self.conf = self.cm.load_config(configFile)

        # load state
        log.info("Loading state...")
        self.state = StateManager(self.conf)
        #self.state.load_state()

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
            self.state.image_state() 
            self.cm.save_config()
            backupdir = "backup/"
            cfile = "config.ini"
            command = "cd data/ && tar czf clone.tar.gz {} {} creds/peers".format(cfile,backupdir)
            shell(command)
            log.info("clone of state successfully created")

        except Exception as e:
            log.info("clone of state failed")

    ### Requests ###

    def local_request(self, msg):
        log.info("replicating {}".format(msg))
        self.loop.run_until_complete(self.c.bpcon_request(msg))

    def group_request(self, req_type, target_group):
        log.debug("group request initiated")
        self.loop.run_until_complete(self.c.make_2pc_request(req_type, target_group))

    def join_request(self):
        """ Command Congregate instance to join its peers by supplying credentials """
        log.debug("attempting to join")
        try:
            with open(self.conf['certfile'], 'r') as fh:
                cert = fh.read()
            with open(self.conf['keyfile'], 'r') as fh:
                pubkey = fh.read()
            wss = self.conf['p_wss']
            self.loop.run_until_complete(self.c.handle_join(wss,pubkey,cert))
        except Exception as e:
            log.debug(e)

    def handle_reconfig_request(self):
        """ Create and return state clone """
        self.clone()
        with open('data/clone.tar.gz', 'r') as fh:
            toreturn = fh.read()
            log.debug("cloned state added successfully")
            return toreturn    

    def make_reconfig_request(self, wss):
        """ Request state clone """ 
        # TODO send a request
        pass


    def shutdown(self):
        print("\nShutdown initiated...")
        print("\nDatabase contents:\n{}".format(self.bpcon.state.db.kvstore)) # save state here
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
        # client request for data
        # route if necessary (manage for client)
        # verification of request and requestor permissions
        # self.db.get(k)
        pass


    def handle_API_request(self, msg):
        # client API requests
        if len(msg) < 4:
            log.info("bad external request: too short")
        else:    
            try:
                a,b,c = msg.split('<>')
            except Exception as e:
                log.info("bad external request: malformed")

            if a == '0': # GET
                #first check cache
                self.state.db.get(b)
            elif a == '1': # PUT
                self.local_request("P,{},{}".format(b,c))
            elif a == '2': # DEL
                self.local_request("D,{},{}".format(b,"null"))
            else: # malformed
                log.info("bad API request")
        

    @asyncio.coroutine
    def mainloop(self, websocket, path):
        try:
            input_msg = yield from websocket.recv()
            log.debug("< {}".format(input_msg))
            if self.conf['use_single_port']:
                output_msg = self.direct_msg(input_msg)
            else:   #using this port for external requests only
                output_msg = yield from self.handle_external_request(input_msg)
                
            if output_msg:
                yield from websocket.send(output_msg)
                #self.bmsgs.append(output_msg)
                
            else:
                yield from websocket.send("Hello from Congregate!")
                log.error("got bad input from peer")

            # adapt here
                # reconfig requests
                

        except Exception as e:
            log.debug(input_msg)
            log.error("mainloop exception: {}".format(e))


def start():
    if len(sys.argv) > 2:
        print("Usage: python congregate.py <configfile>")
    else:    
        try:
            c = Congregate()
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
start()


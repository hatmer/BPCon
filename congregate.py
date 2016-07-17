"""
Reads and maintains config file
Starts paxos and congregate protocol instances
Autonomous congregate functions loop
Handles client requests (including join requests)


"""

import asyncio
import websockets
import sys
import hashlib
import time

from BPCon.protocol import BPConProtocol
from congregateProtocol import CongregateProtocol
from state import StateManager
from configManager import ConfigManager, log
from BPCon.utils import shell

if len(sys.argv) == 2:
    configFile = sys.argv[1]
else:
    configFile = "config.ini"

class Congregate:
    def __init__(self):
        try:
            self.startup() #add option for clean start
            self.loop = asyncio.get_event_loop()
            self.bpcon = BPConProtocol(self.conf, self.state)
            self.paxos_server = websockets.serve(self.bpcon.main_loop, self.conf['ip_addr'], self.conf['port'], ssl=self.conf['ssl'])
            self.loop.run_until_complete(self.paxos_server)
            log.info("Started BPCon on port {}".format(self.conf['port']))

            self.c = CongregateProtocol(self.loop, self.conf, self.bpcon)       
            self.congregate_server = websockets.serve(self.c.main_loop, self.conf['ip_addr'], self.conf['port']+1, ssl=self.conf['ssl'])
            self.loop.run_until_complete(self.congregate_server)
            log.info("Started Congregate on port {}".format(self.conf['port']+1))

            self.web_server = websockets.serve(self.mainloop, self.conf['ip_addr'], self.conf['port']+2) 
            self.loop.run_until_complete(self.web_server)
            log.info("Started Web Server on port {}".format(self.conf['port']+2))

            self.join_request()

            if self.conf['is_client']:
                #self.c.reconfig()
                log.debug("is client. making test requests")
                for x in range(1):
                    request = "P,{},hello{}".format(x,x)
                    self.local_request("P,test,value")
                    #log.debug("request is {}".format(request))
                    self.local_request("P,test1,value1")
                    self.local_request("P,test2,value2")
                    self.local_request("P,test,value3")
                    self.local_request("D,test2,")
                    self.local_request(request)
                    pass

                log.debug("requests complete")     

        except Exception as e:
            log.info(e)

    def startup(self):
        """
        startup routine
        Loads from cloned state

        """
        # clean working dir and extract config, creds, and state
        log.info("Cleaning working directory...")
        command = "rm config.ini && rm -rf data && rm -rf creds"
        shell(command)
        log.info("Extracting cloned state...")
        command = "tar xzf clone.tar.gz"
        shell(command)
        # load config
        log.info("Loading configuration...")
        self.cm = ConfigManager()
        self.conf = self.cm.load_config(configFile)

        # load state
        log.info("Loading state...")
        self.state = StateManager(self.conf)
        self.state.load_state()

    def clone(self):
        """
        create a copy of db, peers, peer creds, and config
        save to compressed archive
        used to add new nodes to system
        """
        try:
            self.state.image_state()
            self.cm.save_config()
            backupdir = "data/"
            cfile = "config.ini"
            command = "tar czf clone.tar.gz {} {} creds/".format(cfile,backupdir)
            shell(command)
            log.info("clone of state successfully created")

        except Exception as e:
            log.info("clone of state failed")

    def local_request(self, msg):
        log.info("replicating {}".format(msg))
        self.loop.run_until_complete(self.c.bpcon_request(msg))

    def group_request(self, msg):
        log.debug("group request initiated")
        self.loop.run_until_complete(self.c.make_2pc_request(msg))

    def join_request(self):
        log.debug("attempting to join")
        try:
            with open("creds/local/server.crt", 'r') as fh:
                cert = fh.read()
            with open("creds/local/server.pub", 'r') as fh:
                pubkey = fh.read()
            wss = self.conf['p_wss']
            self.loop.run_until_complete(self.c.handle_join(wss,pubkey,cert))
        except Exception as e:
            log.debug(e)

    def handle_reconfig_request(self, epoch=0):
        toreturn = self.bpcon.bmsgs
        if epoch != 0:
            self.clone()
            with open('clone.tar.gz', 'r') as fh:
                toreturn += "<>{}".format(fh.read())
                log.debug("cloned state added successfully")

        return toreturn    

    def make_reconfig_request(self, wss):
        # epoch = current epoch
        pass


    def shutdown(self):
        print("\nShutdown initiated...")
        print("\nDatabase contents:\n{}".format(self.bpcon.state.db.kvstore)) # save state here
        self.paxos_server.close()
        self.congregate_server.close()
        print("\nPeer Groups:")
        for gname, rmgr in self.state.groups.items():
            print("{}: {}".format(gname, list(rmgr.peers.keys())))
    
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


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
from configManager import ConfigManager

if len(sys.argv) != 2:
    print("invalid initialization parameters")

configFile = sys.argv[1] # TODO remove

class Congregate:
    def __init__(self):
        try:
            self.cm = ConfigManager()
            self.conf = self.cm.load_config(configFile) 
            print('config loaded')
            self.logger = self.conf['logger']
            print(self.conf['logger'])
            self.logger.info("Loaded config")
            self.state = StateManager(self.conf)
            self.logger.info("Loaded state")
            
            self.loop = asyncio.get_event_loop()
            self.bpcon = BPConProtocol(self.conf, self.state)
            self.paxos_server = websockets.serve(self.bpcon.main_loop, self.conf['ip_addr'], self.conf['port'], ssl=self.conf['ssl'])
            self.loop.run_until_complete(self.paxos_server)
            self.logger.info("Started BPCon on port {}".format(self.conf['port']))

            self.c = CongregateProtocol(self.loop, self.conf, self.bpcon)       
            self.congregate_server = websockets.serve(self.c.main_loop, self.conf['ip_addr'], self.conf['port']+1, ssl=self.conf['ssl'])
            self.loop.run_until_complete(self.congregate_server)
            self.logger.info("Started Congregate on port {}".format(self.conf['port']+1))

            self.web_server = websockets.serve(self.mainloop, self.conf['ip_addr'], self.conf['port']+2) 
            self.loop.run_until_complete(self.web_server)
            self.logger.info("Started Web Server on port {}".format(self.conf['port']+2))

            if self.conf['is_client']:
                #self.c.reconfig()
                self.logger.debug("is client. making test requests")
                with open("creds/local/server.crt") as fh:
                    cert = fh.read()
                with open("creds/local/server.pub") as fh:
                    pubkey = fh.read()
                wss = "wss://localhost:8000"
                for x in range(1):
                    #request = "P,{},hello{}".format(x,x)
                    #request = self.c.create_peer_request("split")
                    self.loop.run_until_complete(self.c.handle_join(wss,pubkey,cert))
#                    self.logger.debug("request is {}".format(request))
#                    self.local_request(request)

                self.logger.debug("requests complete")     

        except Exception as e:
            self.logger.info(e)

    def local_request(self, msg):
        self.logger.debug("db commit initiated")
        self.loop.run_until_complete(self.c.bpcon_request(msg))

    def group_request(self, msg):
        self.logger.debug("group request initiated")
        self.loop.run_until_complete(self.c.make_2pc_request(msg))

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
            self.logger.info("bad external request: too short")
        else:    
            try:
                a,b,c = msg.split('<>')
            except Exception as e:
                self.logger.info("bad external request: malformed")

            if a == '0': # GET
                #first check cache
                self.state.db.get(b)
            elif a == '1': # PUT
                self.local_request("P,{},{}".format(b,c))
            elif a == '2': # DEL
                self.local_request("D,{},{}".format(b,"null"))
            else: # malformed
                self.logger.info("bad API request")
        

    @asyncio.coroutine
    def mainloop(self, websocket, path):
        try:
            input_msg = yield from websocket.recv()
            self.logger.debug("< {}".format(input_msg))
            if self.conf['use_single_port']:
                output_msg = self.direct_msg(input_msg)
            else:   #using this port for external requests only
                output_msg = yield from self.handle_external_request(input_msg)
                
            if output_msg:
                yield from websocket.send(output_msg)
                #self.bmsgs.append(output_msg)
                
            else:
                yield from websocket.send("Hello from Congregate!")
                self.logger.error("got bad input from peer")

            # adapt here

        except Exception as e:
            self.logger.debug(input_msg)
            self.logger.error("mainloop exception: {}".format(e))


def start():
    try:
        c = Congregate()
        try:
            try:
                asyncio.get_event_loop().run_forever()
            except Exception as e:
                print("mainloop exception")
                c.logger.error(e)
        except KeyboardInterrupt:
            c.shutdown()
        finally:
            asyncio.get_event_loop().close()
            print('\nShutdown complete')
    except Exception as e:
        print("System failure")
        print(e)

start()  


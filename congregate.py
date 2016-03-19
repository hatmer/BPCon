"""
Reads and maintains config file
Starts paxos and congregate protocol instances
Autonomous congregate functions loop
Handles client requests


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
            conf = self.cm.load_config(configFile) 
            self.logger = conf['logger']
            self.state = StateManager()
            self.loop = asyncio.get_event_loop()
            self.bpcon = BPConProtocol(conf, self.state) 
            self.c = CongregateProtocol(self.loop, conf, self.bpcon)       
            self.paxos_server = websockets.serve(self.bpcon.main_loop, conf['ip_addr'], conf['port'], ssl=conf['ssl'])
            self.congregate_server = websockets.serve(self.c.main_loop, conf['ip_addr'], conf['port'] + 2, ssl=conf['ssl'])
            #self.web_server = websockets.serve(self.mainloop, conf['ip_addr'], conf['port'] + 1)
            self.loop.run_until_complete(self.paxos_server)
            self.loop.run_until_complete(self.congregate_server)
          

            if conf['is_client']:
                self.logger.debug("making requests")
                
                for x in range(1):
                    self.loop.run_until_complete(self.c.make_2pc_request("G,commit,M;G0;wss://localhost:9000;G1", []))#["wss://127.0.0.1:9002"]
                    self.c.bpcon_commit("P,{},hello{}".format(x,x))
                  

        except Exception as e:
            self.logger.info(e)

    def db_request(self, msg):
        self.logger.debug("db commit initiated")
        self.c.make_bpcon_request(msg)

    def shutdown(self):
        print(self.bpcon.state.db.kvstore) # save state here
        self.paxos_server.close()
        self.congregate_server.close()
        for gname, rmgr in self.state.groups.items():
            print("{}: {}".format(gname, list(rmgr.peers.keys())))
    
    def direct_msg(self, msg):
        msg_type = msg[0]
        if msg_type == '0':
        # 0 -> bpcon
            return "hello"
        elif msg_type == '1': #custom or https -> serve or register and pass to congregate
        # 1 -> db get
            return "hello"
        elif msg_type == '2': 
        # 2 -> congregate
            return "hello"
        # Client Request Handling

    def handle_db_request(self, request):
        # client request for data
        # route if necessary (manage for client)
        # verification of request and requestor permissions
        # self.db.get(k)
        pass

        

    @asyncio.coroutine
    def mainloop(self, websocket, path):
        try:
            input_msg = yield from websocket.recv()
            self.logger.debug("< {}".format(input_msg))
            output_msg = self.direct_msg(input_msg)
            if output_msg:
                yield from websocket.send(output_msg)
                #self.bmsgs.append(output_msg)
                
            else:
                self.logger.error("got bad input from peer")

            # adapt here

        except Exception as e:
            self.logger.error("mainloop exception: {}".format(e))


def start():
    try:
        c = Congregate()
        try:
            try:
                asyncio.get_event_loop().run_forever()
            except Exception as e:
                c.logger.debug(e)
        except KeyboardInterrupt:
            c.shutdown()
            print('done')
        finally:
            asyncio.get_event_loop().close()
    except Exception as e:
        print(e)

start()  


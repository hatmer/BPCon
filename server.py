import asyncio
import websockets
import logging
import ssl
import configparser
import sys
import hashlib
import time
from logging.config import fileConfig
from BPCon.protocol import BPConProtocol

#FORMAT = '%(levelname)s %(asctime)-15s [%(filename)s %(funcName)s] %(message)s'
FORMAT = '%(levelname)s [%(filename)s %(funcName)s] %(message)s'
logging.basicConfig(format=FORMAT)

#logger = logging.getLogger('websockets')
#logger.setLevel(logging.DEBUG)
#logger.addHandler(logging.StreamHandler())

fileConfig('logging_config.ini')
logger = logging.getLogger()

configFile = sys.argv[1] # TODO improve
config = configparser.ConfigParser()
config.read(configFile)

ip_addr = config['network']['ip_addr']
port = int(config['network']['port'])

peerlist = []
for key,val in config.items('peers'):
    wss = "wss://"+key+":"+val
    peerlist.append(wss) 

peer_certs = config['creds']['peer_certs']
certfile = config['creds']['certfile']
keyfile = config['creds']['keyfile']
peer_keys = config['creds']['peer_keys']

def getContext():
    ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)   
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return ctx

class CongregateProtocol:
    def __init__(self):
        try:
            self.b = BPConProtocol(peer_certs, keyfile, logger, peerlist, peer_keys) 
            self.paxos_server = websockets.serve(self.b.main_loop, ip_addr, port, ssl=getContext())
            self.congregate_server = websockets.serve(self.server_loop, ip_addr, port+1, ssl=getContext())
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(self.paxos_server)
            self.loop.run_until_complete(self.congregate_server)

        except Exception as e:
            logger.info(e)

    def shutdown(self):
        self.paxos_server.close()
        self.congregate_server.close()

    @asyncio.coroutine
    def server_loop(self, websocket, path):
        logger.debug("checkpoint 1")
        try:
            input_msg = yield from websocket.recv()
            print(input_msg)
            yield from websocket.send("hello")
        except Exception as e:
            logger.debug(e)

    def commit(self, msg):
        logger.debug("db commit initiated")
        bpcon_task = asyncio.Future()
        bpcon_task.add_done_callback(self.got_commit_result)
        
        try:
            timer_result = asyncio.wait_for(bpcon_task, 3.0) # catch exceptions, verify result
            db_commit_task = self.db_commit(msg, bpcon_task)
            self.loop.run_until_complete(db_commit_task)
        except Exception as e:
            logger.info("exception caught in commit: {}".format(e))

        #self.loop.run_until_complete(asyncio.sleep(1))         
    @asyncio.coroutine
    def db_commit(self, msg, future):
        try:
            yield from self.b.phase1a(msg, future)
        except asyncio.TimeoutError:
            logger.info("db commit timed out")
        except asyncio.CancelledError:
            logger.info("db commit future cancelled")
            

    def got_commit_result(self, future):
        if future.done():
            #do cleanup
            if not future.cancelled():
                logger.info("commit result: {}".format(future.result()))
            else:
                logger.info("future cancelled")
        else:    
            logger.info("future not done ???")

def start():
    try:
        c = CongregateProtocol()
        try:
            try:
                asyncio.get_event_loop().run_forever()
                
            except Exception as e:
                logger.debug(e)
        except KeyboardInterrupt:
            c.shutdown()
            print('done')
        finally:
            asyncio.get_event_loop().close()
    except Exception as e:
        logger.debug(e)

start()        

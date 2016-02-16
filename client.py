import asyncio
import websockets
import logging
import ssl
import configparser
import sys

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

peerlist = {}
for key,val in config.items('peers'):
    peerlist[key] = val

peer_certs = config['creds']['peer_certs']
certfile = config['creds']['certfile']
keyfile = config['creds']['keyfile']

def getContext():
    ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)   
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return ctx

class CongregateProtocol1:
    def __init__(self):
        self.b = BPConProtocol(peer_certs, keyfile, logger, peerlist) 
        self.c = CongregateProtocol()
        self.c.parent = self
        self.paxos_server = websockets.serve(self.b.main_loop, ip_addr, port, ssl=getContext())
        self.congregate_server = websockets.serve(self.c.server_loop, ip_addr, port+1, ssl=getContext())
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.paxos_server)
        self.loop.run_until_complete(self.congregate_server)

        self.c.commit("hello1")
        self.c.commit("hello2")
#        self.c.commit("hello3")
#        self.c.commit("hello4")
#        self.c.commit("hello5")
#        self.c.commit("hello1")
#        self.c.commit("hello2")
#        self.c.commit("hello3")
#        self.c.commit("hello4")
#        self.c.commit("hello5")
        self.loop.close()
        print("Pending tasks after close: %s" % asyncio.Task.all_tasks(asyncio.get_event_loop()))
    def shutdown(self):
        self.paxos_server.close()
        self.congregate_server.close()
        print("Pending tasks after shutdown: %s" % asyncio.Task.all_tasks(asyncio.get_event_loop()))

class CongregateProtocol:
    @asyncio.coroutine
    def server_loop(self, websocket, path):
        logger.debug("checkpoint 1")
        try:
            input_msg = yield from websocket.recv()
            print(input_msg)
            yield from websocket.send("hello")
        except Exception as e:
            logger.debug(e)
        logger.debug('checkpoint 2')

    def commit(self, msg):
        logger.info("db commit initiated")
        bpcon_task = asyncio.Future()
        bpcon_task.add_done_callback(self.got_commit_result)
        
        asyncio.wait_for(bpcon_task, 3.0) # catch exceptions, verify result
        self.parent.loop.run_until_complete(self.db_commit(msg, bpcon_task))
        self.parent.loop.run_until_complete(asyncio.sleep(1))
        
    @asyncio.coroutine
    def db_commit(self, msg, future):
        try:
            a = yield from self.parent.b.phase1a(msg, future)
            print(a) 
        except asyncio.TimeoutError:
            logger.info("db commit timed out")
            print("{}".format(a))
        except asyncio.CancelledError:
            logger.info("db commit future cancelled")
    def got_commit_result(self, future):
        if future.done():
            #do cleanup
            if not future.cancelled():
                logger.info("future result: {}".format(future.result()))
            else:
                logger.info("future cancelled")
        else:    
            logger.info("future not done ???")

def tester():
    try:
        c = CongregateProtocol1()
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

tester()        

import asyncio
import websockets
import logging
import ssl
from contextlib import suppress

from logging.config import fileConfig
from BPCon.protocol import BPConProtocol

#FORMAT = '%(levelname)s %(asctime)-15s [%(filename)s %(funcName)s] %(message)s'
#FORMAT = '%(levelname)s [%(filename)s %(funcName)s] %(message)s'
#logging.basicConfig(format=FORMAT)

#logger = logging.getLogger('websockets')
#logger.setLevel(logging.DEBUG)
#logger.addHandler(logging.StreamHandler())

fileConfig('logging_config.ini')
logger = logging.getLogger()

ip_addr = 'localhost'
port = 9000
certfile = 'creds/keys/server.crt'
keyfile = 'creds/keys/server.key'

peer_certs = 'creds/trusted/'

def getContext():
    ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)   
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return ctx

class CongregateProtocol1:
    def __init__(self):
        self.b = BPConProtocol(peer_certs, logger) 
        self.c = CongregateProtocol()
        self.c.parent = self
        self.paxos_server = websockets.serve(self.b.main_loop, ip_addr, port, ssl=getContext())
        self.congregate_server = websockets.serve(self.c.server_loop, ip_addr, port+1, ssl=getContext())
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.paxos_server)
        self.loop.run_until_complete(self.congregate_server)

    def shutdown(self):
        self.paxos_server.close()
        self.congregate_server.close()

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
        
        t1 = asyncio.async(asyncio.shield(self.db_commit(msg, bpcon_task)))
        self.parent.loop.run_until_complete(t1)
        
        #self.parent.loop.run_until_complete(asyncio.gather(*asyncio.Task.all_tasks()))
        #print("Pending tasks after gather: %s" % asyncio.Task.all_tasks(self.parent.loop))

    @asyncio.coroutine
    def db_commit(self, msg, future):
#        tasks = []
        try:
            paxos_task = asyncio.async(asyncio.shield(self.parent.b.phase1a(msg,future)))
#            tasks.append(paxos_task)
            
            res = yield from asyncio.wait_for(future, timeout=3)

#            yield from asyncio.gather(*asyncio.Task.all_tasks())
#            logger.debug("test2")
#            print("Pending tasks in db_commit: %s" % asyncio.Task.all_tasks(self.parent.loop))

        except asyncio.TimeoutError:
            logger.info("db commit timed out")
        
    def got_commit_result(self, future):
        if future.done():
            #do cleanup
            logger.info(future.result())
            print("future done")
            print(future.result())
        else:    
            print("future not done")
            print(future.result())
            

#c = CongregateProtocol1()

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
            c.shutdown()
            asyncio.get_event_loop().close()
    except Exception as e:
        logger.debug(e)

tester()        

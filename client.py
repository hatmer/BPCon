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
port = 8000
certfile = 'creds/keys2/server.crt'
keyfile = 'creds/keys2/server.key'

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
#        self.c.commit("hello2")
        self.c.commit("hello1")
        print("Pending tasks: %s" % asyncio.Task.all_tasks(self.loop))

    def shutdown(self):
        self.paxos_server.close()
        self.congregate_server.close()

class CongregateProtocol:
    @asyncio.coroutine
    def server_loop(self, websocket, path):
        logger.debug("checkpoint 1")
#        self.commit("hello")      
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
        self.parent.loop.run_until_complete(self.db_commit(msg, bpcon_task))

    @asyncio.coroutine
    def db_commit(self, msg, future):
        tasks = []
        try:
            #yield from self.parent.b.phase1a(msg,future)
            tasks.append(asyncio.async(self.parent.b.phase1a(msg,future)))
            
            res = yield from asyncio.wait_for(future, timeout=1)
            logger.info(res)
            yield from asyncio.gather(*asyncio.Task.all_tasks())
            print("Pending tasks in db_commit: %s" % asyncio.Task.all_tasks(self.parent.loop))

        except asyncio.TimeoutError:
            logger.info("db commit timed out")
        
        print("Pending tasks before gather: %s" % asyncio.Task.all_tasks(self.parent.loop))
        yield from asyncio.gather(*tasks)    

    def got_commit_result(self, future):
        if future.cancelled():
            return "cancelled"
        else:    
            return future.result()
            
        print("Pending tasks after results: %s" % asyncio.Task.all_tasks(self.parent.loop))            

c = CongregateProtocol1()

def tester():
    try:

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

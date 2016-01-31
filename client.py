import asyncio
import websockets
import logging
import ssl
from BPCon.protocol import BPConProtocol

logger = logging.getLogger('websockets')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

ip_addr = 'localhost'
port = 8000
certfile = 'creds/keys2/server.crt'
keyfile = 'creds/keys2/server.key'

peer_certs = 'creds/trusted/'

def getContext():
    ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)   
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    ctx.check_hostname = False
    return ctx

class CongregateProtocol1:
    def __init__(self):
        self.b = BPConProtocol(peer_certs) 
        self.c = CongregateProtocol()
        self.c.parent = self
        self.paxos_server = websockets.serve(self.b.main_loop, ip_addr, port, ssl=getContext())
        self.congregate_server = websockets.serve(self.c.server_loop, ip_addr, port+1, ssl=getContext())
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.paxos_server)
        self.loop.run_until_complete(self.congregate_server)
        self.c.commit("hello2")

class CongregateProtocol:
    @asyncio.coroutine
    def server_loop(self, websocket, path):
        logger.debug("main loop")
        input_msg = "recv error"
        try:
            input_msg = yield from websocket.recv()
        except Exception as e:
            logger.debug(e)
        print(input_msg)
        yield from websocket.send("hello")
        logger.debug('checkpoint')

    def commit(self, msg):
        logger.info("db commit initiated")
        bpcon_task = asyncio.Future()
        bpcon_task.add_done_callback(self.got_commit_result)
        self.parent.loop.run_until_complete(self.db_commit(msg, bpcon_task))

    @asyncio.coroutine
    def db_commit(self, msg, future):
        try:
            yield from self.parent.b.phase1a(msg,future)
            yield from asyncio.wait_for(future, timeout=3)
        except asyncio.TimeoutError:
            logger.info("db commit timed out")

    def got_commit_result(self, future):
        if future.cancelled():
            print("cancelled")
        else:    
            print(future.result())
            
CongregateProtocol1()

try:
    asyncio.get_event_loop().run_forever()
except KeyboardInterrupt:
    print('done')
finally:
    asyncio.get_event_loop().close()

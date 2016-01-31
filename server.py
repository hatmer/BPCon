import asyncio
import websockets
import logging
import ssl
from BPCon.protocol import BPConProtocol

logger = logging.getLogger('websockets')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

ip_addr = 'localhost'
port = 9000
certfile = 'keys/server.crt'
keyfile = 'keys/server.key'

def getContext():
    ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)   
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return ctx

class CongregateProtocol1:
    def __init__(self):
        self.b = BPConProtocol()
        self.b.parent = self
        self.c = CongregateProtocol()
        self.c.parent = self
        self.paxos_server = websockets.serve(self.b.main_loop, ip_addr, port, ssl=getContext())
        self.congregate_server = websockets.serve(self.c.server_loop, ip_addr, port+1, ssl=getContext())
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.paxos_server)
        self.loop.run_until_complete(self.congregate_server)
        #self.db_commit("hello")

    def db_commit(self, msg):
        bpcon_task = asyncio.Future()
        bpcon_task.add_done_callback(self.got_commit_result)
        self.loop.run_until_complete(self.b.phase1a("hello", bpcon_task))
        

    def got_commit_result(self, future):
        print(future.result())

class CongregateProtocol:
    @asyncio.coroutine
    def server_loop(self, websocket, path):
        logger.debug("main loop")
        try:
            input_msg = yield from websocket.recv()
        except Exception as e:
            logger.debug(e)
        print(input_msg)
        logger.debug('checkpiont')

    @asyncio.coroutine
    def db_commit(self, msg):
        print("db_commit method")
        bpcon_task = asyncio.Future()
        asyncio.async(self.b.phase1a(msg, bpcon_task))
        self.loop.run_until_complete(bpcon_task)
        return bpcon_task.result()

CongregateProtocol1()

try:
    asyncio.get_event_loop().run_forever()
except KeyboardInterrupt:
    print('done')
finally:
    asyncio.get_event_loop().close()

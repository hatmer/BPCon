import asyncio
import websockets
import logging
import ssl
from protocol import BPConProtocol

ip_addr = 'localhost'
port = 8000
certfile = 'keys2/server.crt'
keyfile = 'keys2/server.key'

b = BPConProtocol()

def getContext():
    ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return ctx

cxnSockets = ['wss://localhost:9000/']

start_server = websockets.serve(b.main_loop, ip_addr, port, ssl=getContext())

loop = asyncio.get_event_loop()
loop.run_until_complete(start_server)


loop.run_until_complete(b.phase1a())
#loop.run_until_complete(b.send_msg(cnxSockets, "1a-1"))
#loop.run_until_complete(b.send_msg(cnxSockets, "1a-2"))
#loop.run_until_complete(b.send_msg(cnxSockets, "1"))
try:
    loop.run_forever()
except KeyboardInterrupt:
    print('done')
finally:
    loop.close()


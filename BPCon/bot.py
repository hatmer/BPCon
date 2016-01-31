import asyncio
import websockets
import logging
import ssl
from protocol import BPConProtocol


ip_addr = 'localhost'
port = 9000
certfile = 'keys/server.crt'
keyfile = 'keys/server.key'

b = BPConProtocol()

def getContext():
    ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)   
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return ctx

start_server = websockets.serve(b.main_loop, ip_addr, port, ssl=getContext())

loop = asyncio.get_event_loop()
loop.run_until_complete(start_server)

try:
    loop.run_forever()
except KeyboardInterrupt:
    print('done')
finally:
    loop.close()


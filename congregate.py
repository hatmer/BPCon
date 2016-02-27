"""
Reads config file
starts and maintains state of paxos and congregate protocol instances

"""

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
from congregateProtocol import CongregateProtocol

FORMAT = '%(levelname)s [%(filename)s %(funcName)s] %(message)s'
logging.basicConfig(format=FORMAT)

fileConfig('logging_config.ini')
logger = logging.getLogger()

configFile = sys.argv[1] # TODO improve
config = configparser.ConfigParser()
config.read(configFile)

conf = {}
conf['ip_addr'] = config['network']['ip_addr']
conf['port'] = int(config['network']['port'])

conf['peerlist'] = []
for key,val in config.items('peers'):
    wss = "wss://"+key+":"+val
    conf['peerlist'].append(wss) 

conf['peer_certs'] = config['creds']['peer_certs']
conf['certfile'] = config['creds']['certfile']
conf['keyfile'] = config['creds']['keyfile']
conf['peer_keys'] = config['creds']['peer_keys']


ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
ctx.load_cert_chain(certfile=conf['certfile'], keyfile=conf['keyfile'])
conf['ssl'] = ctx
conf['logger'] = logger

is_client = int(config['testing']['is_client'])

class Congregate:
    def __init__(self):
        try:
            self.loop = asyncio.get_event_loop()
            self.bpcon = BPConProtocol(conf) 
            self.c = CongregateProtocol(self.loop, conf, self.bpcon)       
            self.paxos_server = websockets.serve(self.bpcon.main_loop, conf['ip_addr'], conf['port'], ssl=conf['ssl'])
            self.congregate_server = websockets.serve(self.c.main_loop, conf['ip_addr'], conf['port']+1, ssl=conf['ssl'])
            self.loop.run_until_complete(self.paxos_server)
            self.loop.run_until_complete(self.congregate_server)

            if is_client:
                x = 0
                while x < 10:
                    self.commit("P,{},hello{}".format(x,x))
                    x += 1

        except Exception as e:
            logger.info(e)


    def shutdown(self):
        print(self.bpcon.db.kvstore)
        self.paxos_server.close()
        self.congregate_server.close()


    def commit(self, msg):
        logger.debug("db commit initiated")
        self.c.make_peer_request(msg)


def start():
    try:
        c = Congregate()
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


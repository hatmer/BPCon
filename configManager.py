"""
Reads configuration for congregate, bpcon, and logging
Creates any missing credential files (in linux only)
"""

import configparser
import ssl
import logging
from logging.config import fileConfig
import os.path
import subprocess

FORMAT = '%(levelname)s [%(filename)s %(funcName)s] %(message)s'
logging.basicConfig(format=FORMAT)

fileConfig('logging_config.ini')
logger = logging.getLogger()

def shell(command):
    try:
        subprocess.check_output(['sh','-c', command])
    except Exception as e:
        print("shell command failed: {}".format(e))

class ConfigManager:
 
    def load_config(self, configFile):
    
        self.config = configparser.ConfigParser()
        self.config.read(configFile)

        conf = {}
        conf['ip_addr'] = self.config['network']['ip_addr']
        conf['port'] = int(self.config['network']['port'])
        conf['c_wss'] = "wss://"+conf['ip_addr'] +":"+ str(conf['port']+1)
        conf['peerlist'] = []
        for key,val in self.config.items('peers'):
            wss = "wss://"+key+":"+val
            conf['peerlist'].append(wss) 
    
        conf['peer_certs'] = self.config['creds']['peer_certs']
        conf['certfile'] = self.config['creds']['certfile']
        conf['keyfile'] = self.config['creds']['keyfile']
        conf['peer_keys'] = self.config['creds']['peer_keys']

        ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ctx.load_cert_chain(certfile=conf['certfile'], keyfile=conf['keyfile'])
        conf['ssl'] = ctx
        conf['is_client'] = int(self.config['testing']['is_client'])
        
        # Logging
        conf['logger'] = logger

        # verify credential file tree
        if not os.path.exists('creds'):
            shell("mkdir creds")
        if not os.path.exists('creds/peers'):
            shell("mkdir -p creds/peers/certs")
            shell("mkdir -p creds/peers/pubkeys")
        if not os.path.exists('creds/local'):
            shell("mkdir creds/local")
        if not os.path.isfile('creds/local/server.key'):
            print("Generating private key")
            shell("openssl genrsa -des3 -passout pass:x -out server.pass.key 2048")
            shell("openssl rsa -passin pass:x -in server.pass.key -out creds/local/server.key")
            shell("rm server.pass.key")
            shell("openssl rsa -in creds/local/server.key -pubout > creds/local/server.pub")
        if not os.path.isfile('creds/local/server.crt'):
            print("Signing certificate")
            shell("openssl req -new -subj '/C=SE/ST=XX/L=XX/O=XX/CN=localhost' -key creds/local/server.key -out creds/local/server.csr")
            shell("openssl x509 -req -days 365 -in creds/local/server.csr -signkey creds/local/server.key -out creds/local/server.crt")
            
        conf['MAX_GROUP_SIZE'] = int(self.config['vars']['MAX_GROUP_SIZE'])

        return conf
    
    def save_config(self):
        with open('config.ini', 'w') as configfile:
            self.config.write(configfile)



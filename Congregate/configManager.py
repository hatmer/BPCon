"""
Reads configuration for congregate, bpcon, and logging
Creates any missing credential files (in linux only)
"""

import configparser
import ssl
import logging
from logging.config import fileConfig
import os.path
from Crypto.PublicKey import RSA
from BPCon.utils import shell

### Logging Configuration ###

fileConfig('data/logging_config.ini')
log = logging.getLogger()

#############################

class ConfigManager:

    def load_config(self, configFile):
        log = logging.getLogger() 
        self.config = configparser.ConfigParser()
        self.config.read(configFile)

        conf = {}
        conf['log'] = log
        conf['ip_addr'] = self.config['network']['ip_addr']
        conf['port'] = int(self.config['network']['port'])
        conf['p_wss'] = "wss://"+conf['ip_addr'] +":"+ str(conf['port'])
        conf['c_wss'] = "wss://"+conf['ip_addr'] +":"+ str(conf['port']+1)
        
        log.info("adding peers from config")
        conf['peerlist'] = []
        print(self.config.items('peers'))
        for key,val in self.config.items('peers'):
            wss = "wss://"+key+":"+val
            conf['peerlist'].append(wss) 
        log.info("peers added")

        conf['peer_certs'] = self.config['creds']['peer_certs']
        conf['peer_keys'] = self.config['creds']['peer_keys']
        conf['certfile'] = self.config['creds']['certfile']
        conf['keyfile'] = self.config['creds']['keyfile']

        # Logging
        log.info("verifying credentials...")
        # verify credential file tree
        if not os.path.exists('data/creds'):
            shell("mkdir -p data/creds")
        if not os.path.exists('data/creds/peers'):
            shell("mkdir -p data/creds/peers/certs")
            shell("mkdir -p data/creds/peers/keys")
        if not os.path.exists('data/creds/local'):
            shell("mkdir data/creds/local")
        if not os.path.isfile('data/creds/local/server.key'):
            log.info("Generating private key")
            shell("openssl genrsa -passout pass:x -out server.pass.key 2048")
            shell("openssl rsa -passin pass:x -in server.pass.key -out data/creds/local/server.key")
            shell("rm server.pass.key")
            shell("openssl rsa -in data/creds/local/server.key -pubout > data/creds/local/server.pub")
        if not os.path.isfile('data/creds/local/server.crt'):
            log.info("Signing certificate")
            shell("openssl req -new -subj '/C=SE/ST=XX/L=XX/O=XX/CN=localhost' -key data/creds/local/server.key -out data/creds/local/server.csr")
            shell("openssl x509 -req -days 365 -in data/creds/local/server.csr -signkey data/creds/local/server.key -out data/creds/local/server.crt")
        log.info("credentials okay")

        conf['use_single_port'] = bool(self.config['system']['use_single_port'])
        conf['config_file'] = self.config['state']['config_file']
        conf['backup_dir'] = self.config['state']['backup_dir']
        conf['MAX_GROUP_SIZE'] = int(self.config['vars']['MAX_GROUP_SIZE'])


        ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ctx.load_cert_chain(certfile=conf['certfile'], keyfile=conf['keyfile'])
        conf['ssl'] = ctx
        conf['is_client'] = int(self.config['testing']['is_client'])
        return conf
    
    def save_config(self):
        with open(self.config['state']['config_file'], 'w') as configfile:
            self.config.write(configfile)



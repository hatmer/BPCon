import configparser

config = configparser.ConfigParser()

config.read('config.ini')
sections = config.sections()

peer_certs = config['creds']['peer_certs']

peerlist = {}
for key,val in config.items('peers'):
    peerlist[key] = val

print(peerlist)    

"""
for section in sections:
    for item in config[section]:
        print("{} : {}".format(item, config[section][item]))a
"""        

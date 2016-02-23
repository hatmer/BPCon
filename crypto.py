from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA
from Crypto.PublicKey import RSA

privkey = RSA.importKey(open('creds/keys2/server.key').read())
signer = PKCS1_v1_5.new(privkey)

message = "test"
msg_hash = SHA.new(message.encode())

sig = signer.sign(msg_hash)
sig_intstr = str(int.from_bytes(sig, byteorder='little'))

print(sig_intstr)
print(len(message+sig_intstr))


# sent over network here

# string of ints -> bytes
sigmsg_bytes = int(sig_intstr).to_bytes(256, byteorder='little')
print(len(sigmsg_bytes))


pubkeyfile = 'creds/keys2/server.pub'

with open(pubkeyfile, 'r') as fh:
    pubkey = RSA.importKey(fh.read())

verifier = PKCS1_v1_5.new(pubkey)
#print("verify 1: {}".format(pubkey1.verify(msg_hash, sigmsg)))
print("verify: {}".format(verifier.verify(msg_hash, sigmsg_bytes)))

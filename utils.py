import ssl

class Utils:
    def save_state(self, fname):
        with open(fname) as fh:
            pickle.dumps(self.kvstore, fh)
    def load_state(self, fname):
        with open(fname) as fh:
            return pickle.loads(fh)

def get_ssl_context(path):
    cctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    cctx.check_hostname = False
    cctx.load_verify_locations(capath=path)
    return cctx

import pickle

class InMemoryStorage(object):
    def __init__(self):
        self.kvstore = {}
    def get(self, k):
        return self.kvstore[k]
    def put(self, k, v):
        self.kvstore[k] = v
    def delete(self,k):
        return self.kvstore.pop(k,None)
    def save_state(self): # need metadata here
        with open("data/bpcon_storage.txt") as fh:
            pickle.dumps(self.kvstore, fh)
    def load_state(self):
        with open("data/bpcon_storage.txt") as fh:
            self.kvstore = pickle.loads(fh)

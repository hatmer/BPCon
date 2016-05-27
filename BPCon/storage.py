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
    
    def save(self): # need metadata here
        save_state('data/bpcon_storage.txt', self.kvstore)
    
    def load(self):
        self.kvstore = load_state('data/bpcon_storage.txt')

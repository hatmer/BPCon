import pickle
from BPCon.utils import save_state, load_state
from sortedcontainers import SortedDict

class InMemoryStorage(object):
    def __init__(self):
        self.kvstore = SortedDict() # hashtable
    
    def get(self, k):
        try:
            return self.kvstore[k]
        except:
            return 1
    
    def put(self, k, v):
        self.kvstore[k] = v
        return 0

    def delete(self,k):
        try:
            del self.kvstore[k]
            return 0
        except:
            return 1

    def split(self, section, keyspace_mid):
        """ delete one half of keystore for group split operation """
        midKey = None 
        for key in self.kvstore.keys(): # TODO make more efficient for better performance
            if key > str(keyspace_mid): # use iloc to estimate midpoint
                midKey = self.kvstore.index(key)
                break

        if section: # section is either 0 or 1
            self.kvstore = self.kvstore.items()[midKey:]

        else:
            self.kvstore = self.kvstore.items()[:midKey]
        print(self.kvstore)
        return 0

    def save(self): # need metadata here
        save_state('data/backup/db_copy.pkl', self.kvstore)
    
    def load(self):
        self.kvstore = load_state('data/backup/db_copy.pkl')

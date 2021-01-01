import sys
import os
import json
import signal
from threading import Thread

def load(location = "D://database_test_file.json", auto_dump, sig = True):
    return Testdb(location, auto_dump, sig)

class Testdb(object):
    key_string_error = TypeError("key name must be a string")

    def __init__(self, location, auto_dump, sig):
        """Creates a database object and loads the data from the location path.
        If the file does not exist it will be created on the first update.
        """
        
        self.load(location, auto_dump)
        self.dthread = None
        if sig:
            self.set_sigterm_handler()

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, key, val):
        return self.set(key, val)
    
    def __delitem__(self, key):
        return self.rem(key)

    def set_sigterm_handler(self):
        def sigterm_handler():
            if self.dthread is not None:
                self.dthread.join()
            sys.exit(0)
        signal.signal(signal.SIGTERM, sigterm_handler)


    def load(self, location, auto_dump):
        """Loads, reloads or changes the path to the db file"""

        location = os.path.expanduser(location)
        self.loco = location
        self.auto_dump = auto_dump

        if os.path.exists(location):
            self._loaddb()
        else:
            self.db = {}
            
        return True

    def _loaddb(self):
        """Load or reload the json info from the file"""
        try: 
            self.db = json.load(open(self.loco, "rt"))
        except ValueError:
            if os.stat(self.loco).st_size == 0:
                self.db = {}
            else:
                raise

    def _autodump(self):
        """Write/save the json dump into the file if auto_dump is enabled"""
        if self.auto_dump:
            self.dump()
        

    def dump(self):
        """Force dump memory db to file"""
        json.dump(self.db, open(self.loco, "wt"))
        self.dthread = Thread(target = json.dump,
                              args = (self.db, open(self.loco, "wt")))
        self.dthread.start()
        self.dthread.join()
        return True

    def set(self, key, val):
        """If key length > 32, give another key and value"""
        if len(key) > 32:
            key = input("key size exceeded, enter valid key")
            val = input("enter value for the entered key")
            if isinstance(key, str):
                self.db[key] = val
                self._autodump()
            else:
                raise self.key_string_error
            return "key added to database"
            

    def get(self, key):
        """Get the value of a key"""
        try:
            return self.db[key]
        except KeyError:
            return "key is not present"

    def rem(self, key):
        """Delete a key"""
        try:
            del self.db[key]
            return "key deleted"
        except:
            return "can not delete non existing key"
    
            
        
            



    

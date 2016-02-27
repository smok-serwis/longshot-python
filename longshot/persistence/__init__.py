import shelve
import time

class NoPersistenceLayer(object):
    def __init__(self):
        pass

    def get_value(self, path):
        """Return timestamp, value or None"""
        return None

    def set_value(self, path, value, timestamp=None):
        pass

    def sync(self):
        pass

    def del_value(self):
        pass


class ShelfPersistenceLayer(object):
    def __init__(self, filename):
        self.shelf = shelve.open(str(filename), protocol=-1)

    def sync(self):
        self.shelf.sync()

    def del_value(self, path):
        try:
            del self.shelf[path]
        except KeyError:
            pass

    def get_value(self, path):
        try:
            return self.shelf[path]
        except KeyError:
            return None

    def set_value(self, path, value, timestamp=None):
        self.shelf[path] = timestamp or time.time(), value

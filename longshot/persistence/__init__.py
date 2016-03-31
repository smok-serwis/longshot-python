import shelve
import time

class NoPersistenceLayer(object):
    """
    This is a base persistence layer for storing data. It stores current value of a sensor,
    and its historic value (in case net is broken and we need to restore later

    Paths are to be stored WITHOUT prefix
    """
    def __init__(self):
        pass

    def get_current_value(self, path):
        """
        Obtain current value of a sensor
        :param path: path of the value
        """
        return None

    def set_current_value(self, path, value, timestamp=None):
        """
        Set the value of a sensor.
        :param path: path of the sensor
        :param value: sensor's value
        :param timestamp: timestamp of the write. If None, current will be used.
        :return:
        """
        pass

    def sync(self):
        """Synchronize to disk"""
        pass

    def del_current_value(self, path):
        """
        Delete a sensor - it's current value and historic
        :param path: sensor path
        """
        pass


class ShelfPersistenceLayer(object):
    def __init__(self, filename):
        self.shelf = shelve.open(str(filename), protocol=-1)

    def sync(self):
        """Synchronize to disk"""
        self.shelf.sync()

    def get_current_value(self, path):
        """
        Obtain current value of a sensor
        :param path: path of the value
        :return: timestamp, value
        """
        try:
            return self.shelf[path]
        except KeyError:
            return None

    def set_current_value(self, path, value, timestamp=None):
        """
        Set the value of a sensor.
        :param path: path of the sensor
        :param value: sensor's value
        :param timestamp: timestamp of the write. If None, current will be used.
        :return:
        """
        self.shelf[path] = timestamp or time.time(), value

    def del_current_value(self, path):
        """
        Delete a sensor - it's current value and historic
        :param path: sensor path
        """
        try:
            del self.shelf[path]
        except KeyError:
            pass

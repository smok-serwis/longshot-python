import time
import threading
import warnings
import requests
from .persistence import NoPersistenceLayer


class LongshotThread(threading.Thread):
    def __init__(self, device, api_root):
        threading.Thread.__init__(self)
        self.device = device
        self.api_root = api_root
        self.terminating = False

    def _syncpaths(self):
        """synchronize pathpoints against the server"""
        all_pathpoints = list(self.device.pathpoints.keys())

        r = requests.post(self.api_root + '/v1/redefine_paths/',
                          json={
                              'device_id': self.device.device_id,
                              'secret': self.device.secret,
                              'paths': all_pathpoints
                          })

        if r.status_code == 200:
            self.device.paths_synced = True

            r = r.json()

            for path, tsv in r['values'].iteritems():
                self.device.pathpoints[path]._change(*tsv)

    def _check_write_queue(self):
        r = requests.post(self.api_root + '/v1/get_writes/',
                          json={'device_id': self.device.device_id, 'secret': self.device.secret})
        if r.status_code != 200:
            return

        r = r.json()
        paths_updated = []

        for pathpoint, value in r['writes'].iteritems():

            timestamp, value = value

            try:
                pp = self.device.pathpoints[pathpoint]
            except KeyError:
                continue
            else:
                paths_updated.append(pathpoint)

            pp._change(timestamp, value)

        if paths_updated:
            r = requests.post(self.api_root + '/v1/confirm_writes/', json={'device_id': self.device.device_id,
                                                                           'secret': self.device.secret,
                                                                           'paths_updated': paths_updated})

    def run(self):
        self._syncpaths()

        while not self.terminating:
            # sync paths
            if not self.device.paths_synced:
                self._syncpaths()

            # check write queue
            self._check_write_queue()

            # check if there's a need to sync any values with the server
            need_to_sync = False

            for pathpoint in self.device.pathpoints.values():
                if not pathpoint.synced:
                    need_to_sync = True
                    break

            if need_to_sync:
                # sync'em

                sync_dict = {}

                for pathpoint in self.device.pathpoints.values():
                    if pathpoint.values_to_store:
                        sync_dict[pathpoint.path] = sorted(pathpoint.values_to_store)
                        pathpoint.values_to_store = []

                r = requests.post(self.api_root + '/v1/sync_values/', json={'device_id': self.device.device_id,
                                                                            'secret': self.device.secret,
                                                                            'values': sync_dict})

                if r.status_code != 200:
                    # If you failed syncing, return the values to pool and try another time
                    for path, values in sync_dict.iteritems():
                        try:
                            self.device.pathpoints[path].values_to_store.extend(values)
                        except KeyError:
                            pass
                else:
                    for pathpoint in self.device.pathpoints.values():
                        pathpoint.synced = True

            self.device.persistence.sync()
            time.sleep(60)


class LongshotDevice(object):
    """
    A root class that will do the interfacing for a single device
    """

    def __init__(self, device_id, secret, persistence_layer=None, longshot_path='http://longshot.smok4.development/'):
        """
        Initialize the device
        :param device_id: device ID
        :param secret: device secret
        :param longshot_path: Longshot API access
        """

        self.device_id = device_id
        self.secret = secret
        self.pathpoints = {}  # path name => LongshotPathpoint
        self.done_registering = False       #: was first /v1/redefine_paths/ called?
        self.paths_synced = False           #: is there a need to synchronize patches?
        self.thread = LongshotThread(self, longshot_path)

        self.persistence = persistence_layer or NoPersistenceLayer()

    def unregister(self, path):
        """
        Unregister a path.

        Throws NameError if path was not registered previously

        :param path: path name
        """
        if isinstance(path, LongshotPathpoint):
            path = path.path

        self.persistence.del_value(path)

        try:
            del self.pathpoints[path]
        except KeyError:
            raise NameError

        self.paths_synced = False

    def get(self, path):
        """
        Return LongshotPathpoint object for given path

        Raises KeyError if not registered
        """
        return self.pathpoints[path]    # raises KeyError

    def register(self, path, default=None):
        """Create and register new pathpoint

            p = LongshotPathpoint(path, device)

        is equivalent to

            p = device.register(path)

        :param path: path name
        :return: LongshotPathpoint to interface with the pathpoint
        """

        if path in self.pathpoints:
            return self.pathpoints[path]
        else:
            pp = LongshotPathpoint(path, self)

            try:
                timestamp, value = self.persistence.get_value(path)
            except TypeError:   # None can't be unpacked
                pp.value = default
                pp.timestamp = None

            self.paths_synced = False
            self.pathpoints[path] = pp
            return pp

    def done(self):
        """
        Tell longshot that all pathpoints have been registered.

        At this point application has registered all pathpoints it knows about. Pathpoints that are
        registered server-side and not here can be lost.

        This function, by itself, does not call any listeners

        After this, further pathpoints can be registered/unregistered via method calls, and they will be
        synced too
        """
        if self.done_registering:
            warnings.warn(RuntimeWarning('done() called twice or more'))
            return  # this cannot be called twice

        self.done_registering = True
        self.thread.start()

    def shutdown(self):
        """Shut this device down"""
        self.thread.terminating = True
        self.thread.join()


class LongshotPathpoint(object):
    """
    A single pathpoint, as registered in Longshot system
    """

    def __init__(self, path, device=None):
        """
        :type path: str or unicode
        :param device: pass a LongshotDevice if this should be registered immediately. Pass None
            if call to .register() is needed
        :type device: LongshotDevice
        """
        self.device = device
        self.path = path

        self.values_to_store = []  # list of timestamp, value
        self.listeners = []  # callable/1 to be invoked with new value upon change

        self.value = None  # current value
        self.timestamp = None
        self.synced = False
        self.registered = False

    def unregister(self):
        self.device.unregister(self.path)

    def register(self, device):
        if self.registered:
            raise RuntimeError('Already registered')

        self.device = device

        if self.path in self.device.pathpoints:
            raise RuntimeError('Already registered')

        self.device.register(self)
        self.registered = True

    def listen(self, callable):
        """
        Register callable to be called if value of this pathpoint changes.
        Two sources of these changes are possible:
            - listeners are called with updated value after registering this pathpoint
            - server issues us a change command
        Listener will not be called on updating the value by .store()

        Listener can be invoked from either the thread that launches .done() or from internal Longshot thread.
        Take care with concurrency.
        """
        self.listeners.append(callable)


    def push(self):
        """
        a store() with current values. No effect is value empty

        Returns self
        """
        if self.value is None:
            return self

        self.store(self.value, self.timestamp)
        return self

    def update(self, value, timestamp=None):
        """
        Value of the register has changed.

        This is meant to be called by other part inside this application, but other than the one
        that registered the pathpoint and is responsible for this.

        This will schedule a sync with the server, and call listeners - just as it a new value
        was received from the server.

        :param value: new register value
        :param timestamp: timestamp of write. None for current system time. If timestamp is less
            than current, nothing will happen
        """
        self._change(timestamp or time.time(), value)

    def _change(self, timestamp, value):
        """
        This will JUST set current value and call listeners.

        If value is the same, nothing will change
        If timestamp is less, nothing will change"""

        if self.timestamp > timestamp:
            return

        self.value = value
        self.timestamp = timestamp

        for listener in self.listeners:
            listener(self.value)

        self.device.persistence.set_value(self.path, value)

    def store(self, value, timestamp=None):
        """
        Update this pathpoint with a new value.

        This is meant to be called by the application that registered the point itself,
        means it knows about the update.

        This will NOT invoke all listeners on this path

        :param timestamp: UNIX timestamp or None for current system time
        """
        timestamp = int(timestamp or time.time())

        self.values_to_store.append((timestamp*1000, value))    # because server deals in ms
        self.device.persistence.set_value(self.path, value)

        self.synced = False

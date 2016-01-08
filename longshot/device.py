import time
import threading
import warnings
import requests


class LongshotThread(threading.Thread):
    def __init__(self, device, api_root):
        threading.Thread.__init__(self)
        self.device = device
        self.api_root = api_root

    def _syncpaths(self):
        """synchronize pathpoints against the server"""
        all_pathpoints = list(self.device.pathpoints.keys())

        r = requests.post(self.api_root+'/v1/redefine_paths/',
                          json={
                              'device_id': self.device.device_id,
                              'secret': self.device.secret,
                              'paths': all_pathpoints
                          })

        if r.status_code == 200:
            self.device.paths_synced = True

            r = r.json()

            for path, value in r['values'].iteritems():
                self.device.pathpoints[path]._change(value)

    def _check_write_queue(self):
        r = requests.post(self.api_root+'/v1/get_writes/', json={'device_id': self.device.device_id, 'secret': self.device.secret})
        if r.status_code != 200:
            return

        r = r.json()
        paths_updated = []

        for pathpoint, value in r['writes'].iteritems():
            try:
                pp = self.device.pathpoints[pathpoint]
            except KeyError:
                continue
            else:
                paths_updated.append(pathpoint)

            pp.store(value)

        if paths_updated:
            r = requests.post(self.api_root+'/v1/confirm_writes/', json={'device_id': self.device.device_id,
                                                                         'secret': self.device.secret,
                                                                         'paths_updated': paths_updated})

    def run(self):
        self._syncpaths()

        while True:
            time.sleep(60)
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


                print 'syncing %s' % (repr(sync_dict), )
                r = requests.post(self.api_root+'/v1/sync_values/', json={'device_id': self.device.device_id,
                                                                          'secret': self.device.secret,
                                                                          'values': sync_dict})

                if r.status_code != 200:
                    # If you failed syncing, return the values to pool and try another time
                    for path, values in sync_dict:
                        try:
                            self.device.pathpoint[path].values_to_store.extend(values)
                        except KeyError:
                            pass
                else:
                    for pathpoint in self.device.pathpoints.values():
                        pathpoint.synced = True


class LongshotDevice(object):
    """
    A root class that will do the interfacing for a single device
    """

    def __init__(self, device_id, secret, longshot_path='http://longshot.smok4.development/'):
        """
        Initialize the device
        :param device_id: device ID
        :param secret: device secret
        :param longshot_path: Longshot API access
        """

        self.device_id = device_id
        self.secret = secret
        self.pathpoints = {}        # path name => LongshotPathpoint
        self.done_registering = False
        self.paths_synced = False
        self.thread = LongshotThread(self, longshot_path)

    def unregister(self, path):
        """
        Unregister a path.

        Throws NameError if path was not registered previously

        :param path: path name
        """
        if isinstance(path, LongshotPathpoint):
            path = path.path

        try:
            del self.pathpoints[path]
        except KeyError:
            raise NameError

        self.paths_synced = False

    def register(self, path):
        """Register new pathpoint
        :param path: path name
        :return: LongshotPathpoint to interface with the pathpoint
        """

        if path in self.pathpoints:
            return self.pathpoints[path]
        else:
            self.paths_synced = False
            pp = LongshotPathpoint(self.device_id, path)
            self.pathpoints[path] = pp
            return pp

    def done(self):
        """
        Tell longshot that all pathpoints have been registered

        At this point application has registered all pathpoints it knows about. Pathpoints that are
        registered server-side and not here can be lost.

        After this, further pathpoints can be registered/unregistered via method calls, and they will be
        synced too
        """
        if self.done_registering:
            warnings.warn(RuntimeWarning('done() called twice or more'))
            return      # this cannot be called twice

        self.done_registering = True
        self.thread.start()


class LongshotPathpoint(object):
    """
    A single pathpoint, as registered in Longshot system
    """

    def __init__(self, device_id, path):
        self.device_id = device_id
        self.path = path

        self.values_to_store = []   # list of timestamp, value
        self.listeners = []     # callable/1 to be invoked with new value upon change

        self.value = None       # current value
        self.synced = True

    def listen(self, callable):
        """
        Register callable to be called if value of this pathpoint changes.
        Listener can be invoked from either the thread that updates a pathpoint or from internal Longshot thread.
        Take care with concurrency.
        """
        self.listeners.append(callable)

    def _change(self, value):
        if value == self.value:
            return

        self.value = value

        for listener in self.listeners:
            listener(value)

    def store(self, value, timestamp=None):
        """
        Update this pathpoint with a new value.

        values are gathered by the application.

        This will invoke all listeners on this path

        :param timestamp: UNIX timestamp or None for current system time
        """
        timestamp = int(timestamp or time.time())

        self.values_to_store.append((timestamp, value))
        self._change(value)

        self.synced = False



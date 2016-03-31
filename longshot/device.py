import time
import threading
import warnings
import requests
from .persistence import NoPersistenceLayer


class _LongshotThread(threading.Thread):
    def __init__(self, device, api_root, pathpoint_prefix):
        threading.Thread.__init__(self)
        self.device = device
        self.api_root = api_root
        self.terminating = False
        self.pathpoint_prefix = pathpoint_prefix

    def _syncpaths(self):
        """synchronize pathpoints against the server"""
        all_pathpoints = list(self.device.pathpoints.keys())

        r = requests.post(self.api_root + '/v1/redefine_paths/',
                          json={
                              'device_id': self.device.device_id,
                              'secret': self.device.secret,
                              'paths': all_pathpoints,
                              'prefix': self.pathpoint_prefix
                          })

        if r.status_code != 200:
            raise IOError('Failed to redefine paths')

        self.device.paths_synced = True

        r = r.json()

        # Now we need to check default values for sensors that are registered first time
        # in this session

        for path, tsv in r['values'].iteritems():
            try:
                p = self.device.pathpoints[path]
            except KeyError:     # it might have been deleted while we were syncing!
                continue

            if p.declared:        # No need to check it.
                continue

            try:
                candidate_ts, candidate_v = tsv
                candidate_ts /= 1000    # server expresses them in milliseconds
            except TypeError:   # tsv was None
                did_server_value_win = False
            else:
                did_server_value_win = False        # because if it does, we need to schedule a sync...
                if p.value is None:     # server value automatically wins...
                    did_server_value_win = True
                else: # there is a conflict
                    if candidate_ts > p.timestamp:
                        did_server_value_win = True

            if did_server_value_win:
                p.on_write_arrived(candidate_ts, candidate_v)
            else:
                if not (p.timestamp is None or p.value is None):
                    p.stored_values.append((p.timestamp, p.value))
                    p.needs_sync = True

            p.declared = True

    def _check_order_queue(self):
        r = requests.post(self.api_root + '/v1/get_orders/',
                          json={'device_id': self.device.device_id, 'secret': self.device.secret})
        if r.status_code != 200:
            raise IOError('Failed to get orders')

        r = r.json()

        for pathpoint, value in r['writes'].iteritems():
            timestamp, value = value
            timestamp /= 1000    # server counts in ms

            try:
                pp = self.device.pathpoints[pathpoint]
            except KeyError:
                continue
            else:
                pp.on_write_arrived(timestamp, value)

        for pathpoint in r['reads']:
            try:
                pp = self.device.pathpoints[pathpoint]
            except KeyError:
                continue

            v = pp.obtain_value()
            if v is not None:
                pp.stored_values.append((time.time(), v))
                pp.needs_sync = True

        if len(r['writes']) == len(r['reads']) == 0:
            return
        else:
            requests.post(self.api_root + '/v1/confirm_orders/', json={'device_id': self.device.device_id,
                                                                       'secret': self.device.secret,
                                                                       'pot': r['pot']})


    def _syncvalues(self):
        # we did not break - this means there is a need to sync
        sync_dict = {}

        for pathpoint in self.device.pathpoints.values():
            if pathpoint.stored_values:
                    # server deals in MS
                q = ((ts * 1000, v) for ts, v in pathpoint.stored_values)

                sync_dict[pathpoint.prefixed_path] = sorted(q)
                pathpoint.stored_values = []

        r = requests.post(self.api_root + '/v1/sync_values/', json={'device_id': self.device.device_id,
                                                                    'secret': self.device.secret,
                                                                    'values': sync_dict})

        if r.status_code != 200:
            # If you failed syncing, return the values to pool and try another time
            for path, values in sync_dict.iteritems():
                try:
                    self.device.pathpoints[path].stored_values.extend([(ts/1000, v) for ts, v in values])
                except KeyError:
                    pass
            raise IOError('Failed to sync')
        else:
            for pathpoint in self.device.pathpoints.values():
                pathpoint.synced = True


    def run(self):
        while True:
            try:
                self._syncpaths()
            except IOError:
                time.sleep(20)
            else:
                break

        while not self.terminating:
            try:
                # sync paths
                if not self.device.paths_synced:
                    self._syncpaths()

                # check order queue
                self._check_order_queue()

                need_to_sync = False
                for synced in [not pathpoint.needs_sync for pathpoint in self.device.pathpoints.values()]:
                    if not synced:
                        need_to_sync = True
                        break

                if need_to_sync:
                    self._syncvalues()

                self.device.persistence.sync()
                time.sleep(30)

            except IOError:
                pass


class Device(object):
    """
    Class that presents a device registered in SMOK system.
    """

    def __init__(self, device_id, secret,
                                  persistence_layer=None,
                                  longshot_path='http://longshot.smok-serwis.pl/',
                                  pathpoint_prefix='l'):
        """
        Initialize the device
        :param device_id: device ID, as assigned by SMOK administrator
        :param secret: device secret, as assigned by SMOK administrator
        :param longshot_path: Longshot API URL.
        :param pathpoint_prefix: Pathpoint prefix. Will be automatically appended to defined pathpoints.
            If prefix is 'l', pathpoint 'Waccess' will be reinterpreted as 'Wlaccess'.
            before call to .register() all pathpoints with matching prefixes MUST be declared, or they
            will be deleted.
            Prefix is used to support linking multiple longshots on a single device ID.
        """

        self.device_id = device_id
        self.secret = secret
        self.prefix = pathpoint_prefix
        self.pathpoints = {}  # path name (including prefix) => LongshotPathpoint
        self.done_registering = False       #: was .done() called?
        self.paths_synced = False           #: is there a need to synchronize patches?
        self.thread = _LongshotThread(self, longshot_path, pathpoint_prefix)

        self.persistence = persistence_layer or NoPersistenceLayer()

    def unregister(self, path):
        """
        Unregister a path.

        path has to be a prefixed path or a Pathpoint object

        Throws NameError if path was not registered previously

        :param path: path name
        """
        from .pathpoints import BasePathpoint
        if isinstance(path, BasePathpoint):
            path = path.prefixed_path

        self.persistence.del_current_value(path)

        try:
            del self.pathpoints[path]
        except KeyError:
            raise NameError

        self.paths_synced = False

    def get(self, path):
        """Obtain a pathpoint. Path is unprefixed.
        Raises KeyError if does not exist"""
        return self.pathpoints[path[0] + self.prefix + path[1:]]

    def register(self, pathpoint):
        """Register a Pathpoint object into this device"""

        self.pathpoints[pathpoint.prefixed_path] = pathpoint
        self.paths_synced = False

    def done(self):
        """
        Tell longshot that all pathpoints have been registered.

        At this point application has registered all pathpoints it knows about. Pathpoints that are
        registered server-side and not here can be lost.

        This function, by itself, does not call any listeners

        After this, further pathpoints can be registered/unregistered via method calls, and they will be
        synced too

        :raises RuntimeError: called more than twice
        """
        if self.done_registering:
            raise RuntimeError('called .done() twice')

        self.done_registering = True
        self.thread.start()

    def shutdown(self):
        """Shut this device down"""
        self.thread.terminating = True
        self.thread.join()

    def __eq__(self, other):
        return self.device_id == other.device_id

    def __hash__(self):
        return hash(self.device_id)

class LongshotPathpoint(object):
    """
    A single pathpoint, as registered in Longshot system
    """

    def __init__(self, path, device):
        """
        :type path: str or unicode
        :param device: attached device
        :type device: LongshotDevice
        """
        self.device = device
        self.path = path

        self.values_to_store = []  # list of (timestamp, value) - archival value to store
        self.listeners = []  # callable/1 to be invoked with new value upon change

        # Current value - timestamp, value
        self.value = None
        self.timestamp = None       # in seconds !

        self.synced = True          # Are there any new values that need to be sent to server?
        self.registered = False      # Has this been declared on the remote server?

        # default value will be loaded just now by LongshotDevice.register() that called this constructor

    def listen(self, callable):
        """
        Register callable to be called if value of this pathpoint changes.
        Following sources of these changes are possible:
            - server has a more recent value than we do (we are just declaring this path)
            - server issues us a WRITE command
            - internal change via .update()

        Listener can be invoked from either the thread that launches .store() or from internal Longshot thread.
        Take care with concurrency.
        :return: self
        """
        self.listeners.append(callable)
        return self


    def update(self, value, timestamp=None):
        """
        Value of the register has changed due to application-internal reasons.

        This is meant to be called by other part inside this application, but other than the one
        that registered the pathpoint and is responsible for this. Because of this, it can be used
        to implement a sort of intra-process communication.

        This will schedule a sync with the server, and call listeners - just as it a new value
        was received from the server.

        :param value: new register value
        :param timestamp: timestamp of write. None for current system time. If timestamp is less
            than current, nothing will happen
        """
        self._change(timestamp or time.time(), value)

    def _schedulesync(self, timestamp, value):
        """
        Make timestamp and value sync to server at later time
        """
        self.values_to_store.append((timestamp, value))    # because server deals in ms
        self.synced = False

    def _change(self, timestamp, value):
        """
        Set the current value and call the listeners. DO NOT schedule a server update
        :param timestamp: timestamp of the update. Nothing will happen if this is less
            than current timestamp
        """

        if self.timestamp > timestamp:
            return

        self.value = value
        self.timestamp = timestamp

        for listener in self.listeners:
            listener(self.value)

        self.device.persistence.set_current_value(self.path, value)

    def store(self, value, timestamp=None):
        """
        Update this pathpoint with a new value.

        This is meant to be called by the part of application that registered the point itself,
        means it knows about the update.

        This will NOT invoke listeners on this path

        :param timestamp: UNIX timestamp or None for current system time
        """
        timestamp = timestamp or time.time()

        self._schedulesync(timestamp, value)

        self.device.persistence.set_current_value(self.path, value)


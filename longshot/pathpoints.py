import time

class BasePathpoint(object):
    """
    A pathpoint you can register for a LongshotDevice.
    All registered pathpoints must descend from this.
    """

    def __init__(self, path, device, default_value=None,
                             default_timestamp=None):
        """
        Create a pathpoint.
        :param path: Name of the path, BEFORE applying prefix
        :param device: Longshot Device
        :param default_timestamp: timestamp in seconds, or 'now' for current
        """
        self.device = device
        self.path = path
        self.prefixed_path = path[0] + device.prefix + path[1:]

        if default_timestamp == 'now':
            default_timestamp = time.time()

        # Try to ascertain current value and timestamp. It's our defaults against device's Persistence

        try:
            timestamp, value = self.device.persistence.get_current_value(path)
        except TypeError:   # None can't be unpacked. Use provided defaults
            # We MUST use out defaults
            self.value = default_value
            self.timestamp = default_timestamp
        else:
            if timestamp > default_timestamp:   # PL provided value won
                self.value = value
                self.timestamp = timestamp
            else:   # default values won
                self.value = default_value
                self.timestamp = default_timestamp


        self.stored_values = []     # awaiting to send to server
        self.needs_sync = False     # do we need synchronizing with the server?
        self.declared = False       # is it registered on the server?

    def on_write_arrived(self, timestamp, value):
        """
        An order from the server to write this register arrived.

        Called by Longshot thread

        :param timestamp: Server demands to write this timestamp
        :param value: Server demands to write this value
        """
        self.value = value
        self.timestamp = timestamp

    def store(self, value, timestamp=None):
        """
        Request to send a value to server.
        :param value: value to send
        :param timestamp: optional timestamp to use. If None specified, current will be used
        """
        self.stored_values.append((timestamp or time.time(), value))
        self.device.persistence.set_current_value(self.prefixed_path, value, timestamp or time.time())
        self.needs_sync = True

    def obtain_value(self):
        """
        Server required us to read the value.

        Longshot will call this, so this should not block

        :return: current value of this sensor, or None if nothing could be obtained
        """

    def __eq__(self, other):
        return self.path == other.path and self.device == other.device

    def __hash__(self, other):
        return hash(self.path) ^ hash(self.device)

    def __call__(self):
        """Return current value"""
        return self.value

    def register(self):
        self.device.register(self)
        return self

    def unregister(self):
        self.device.unregister(self.prefixed_path)


def pathpoint_from_functions(path, device,
                                on_write_arrived=lambda timestamp, value: None,
                                on_read_requested=lambda: None,
                                default_value=None,
                                default_timestamp=None):
    class FuncPathpoint(BasePathpoint):
        def __init__(self):
            if default_timestamp == 'now':
                default_timestamp = time.time()
            BasePathpoint.__init__(self, path, device, default_value, default_timestamp)

        def on_write_arrived(self, timestamp, value):
            BasePathpoint.on_write_arrived(self, timestamp, value)
            on_write_arrived(timestamp, value)

        def obtain_value(self):
            if on_read_requested == 'self':
                return self.value
            else:
                return on_read_requested()

    return FuncPathpoint()

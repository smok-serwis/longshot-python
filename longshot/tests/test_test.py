from unittest import TestCase
import mock
import time
from longshot import LongshotDevice, LongshotPathpoint
import requests

class TestRegistering(TestCase):

    class FakeResponse(object):
        def __init__(self, val):
            self.val = val
            self.status_code == 200 if val is not None else 500

        def json(self):
            return self.val


    @mock.patch('requests.post')
    def testRegister1(self, rpo):
        rpo = lambda *args, **kwargs: self.FakeResponse({'values': []})

        d1 = LongshotDevice('dupa', 'xx')
        d1.register('lel')
        d1.done()

        time.sleep(3)

        d1.shutdown()

        assert requests.post.called


    @mock.patch('requests.post')
    def testRegister2(self, rpo):
        rpo = lambda *args, **kwargs: self.FakeResponse({'values': []})

        d1 = LongshotDevice('dupa', 'xx')
        LongshotPathpoint('lel').register(d1)
        d1.done()

        time.sleep(3)

        d1.shutdown()

        assert requests.post.called


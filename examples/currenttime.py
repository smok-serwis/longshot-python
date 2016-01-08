"""
This example is a SMOK device that defines two paths
They contain local time.

Values will be archived and periodically synced to server
"""

import longshot
import time
import datetime

d = longshot.LongshotDevice('long1', 'long1')

hour = d.register('Wlhour')
minute = d.register('Wlminute')

d.done()

while True:
    chour = datetime.datetime.now().hour
    cminute = datetime.datetime.now().minute

    hour.store(chour)
    minute.store(cminute)

    time.sleep(60)       # wait for next minute hopefully
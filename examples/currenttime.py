"""
This example is a SMOK device that defines two paths
They contain local time.

Values will be archived and periodically synced to server
"""

import longshot
import time
import datetime

d = longshot.LongshotDevice('long1', 'long1')

hour = longshot.pathpoint_from_functions('Whour', d,
                                         on_read_requested=lambda: datetime.datetime.now().hour,
                                         default_value=datetime.datetime.now().hour,
                                         default_timestamp='now'
                                         ).register()
minute = longshot.pathpoint_from_functions('Wminute', d,
                                           on_read_requested=lambda: datetime.datetime.now().minute,
                                           default_value=datetime.datetime.now().minute,
                                           default_timestamp='now'
                                           ).register()

d.done()

while True:
    hour.store(datetime.datetime.now().hour)
    minute.store(datetime.datetime.now().minute)

    time.sleep(60)       # wait for next minute hopefully
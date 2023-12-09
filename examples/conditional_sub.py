from bitstring import pack, Bits
from pydsvdc import DsVdc
import numpy as np
from dateutil import parser
from kafka import KafkaConsumer
import json
import uuid
import re

conn_str="sockets://172.17.0.2:4000"

"""
def sub_fn(a):
    import np
    if np.max(a) > .1:
        return (a - 30)
    else:
        return(None)
"""

def sub_fn(a):
    import numpy as np
    return a - np.max(a)

dv = DsVdc()
#dv = DsVdc(conn_str)
subscription = dv.sub(dv, name = f'RadC/C12/Rad', start_time = '20220101', end_time = '20230630', xform = sub_fn)

print(f'subscription handle: {subscription.handle}')
for result in subscription:
    print(result['data'])
    print(f'received result with timestamp {result["tstamp"]}, max value: {np.max(result["data"])}')
print('all done')


#subscription = dv.joinsub(handle)

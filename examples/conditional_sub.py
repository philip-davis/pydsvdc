from bitstring import pack, Bits
from pydsvdc import DsVdc
import numpy as np
from dateutil import parser
from kafka import KafkaConsumer
import json
import uuid
import re

conn_str="sockets://172.17.0.2:4000"

'''
def sub_fn(a):
    import numpy as np
    if np.max(a) > 500000:
        return(a+0)
'''
def sub_fn(a):
    import numpy
    if(numpy.max(a) > 500000):
        return((numpy.max(a), 1))
    else:
        return((numpy.max(a), 0))

dv = DsVdc(conn_str)
subscription = dv.sub(name = f'FDCC/Area', start_time = '20220101', end_time = '20230630', xform = sub_fn, debug = False)

print(f'subscription handle: {subscription.handle}')
for result in subscription:
    print(result['data'])
    print(f'received result with timestamp {result["tstamp"]}, max value: {np.max(result["data"])}')
print('all done')


#subscription = dv.joinsub(handle)

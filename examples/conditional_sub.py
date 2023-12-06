from bitstring import pack, Bits
from pydsvdc import DsVdc
import numpy as np
from dateutil import parser
from kafka import KafkaConsumer
import json
import uuid
import re

def _pack_version(year, day, hour, fnum):
    check = 0
    bits = pack('uint:2, uint:8, uint:9, uint:5, uint:8', 0, year-1900, day, hour, fnum)
    if check != 0:
        print(f'WARNING: version check value mismatch. Expected 0, got {check}.', file=sys.stderr)
    return(bits.uint)



def _get_channel(channel_str):
    if channel_str[0] != 'C':
        print(f'ERROR: {channel_str} does not appear to be a channel.', file=sys.stderr)
        return(-1)
    return(int(channel_str[1:]))

def _get_abi_platform_id(name):
    name_parts = name.split('/')
    product = name_parts[0]
    if product == 'FDCC':
        return('ABI-L2-FDCC')
    elif product == 'RadC':
        channel = _get_channel(name_parts[1])
        return(f'ABI-L1b-RadC-M.C{channel:02d}')
    elif product == 'RadM':
        zone = name_parts[1]
        channel = _get_channel(name_parts[2])
        return(f'ABI-L1b-Rad{zone}-M.C{channel:02d}')

def get_kafka_conn_str():
    kafka_sock = '54.145.37.197:9092'
    return(kafka_sock)

class NSDFEventStream:
    def __init__(self, ds, matchfns, termfn, streamname, name):
        topic = streamname
        conn_str = get_kafka_conn_str()
        print(topic)
        self.consumer = KafkaConsumer(topic, bootstrap_servers=conn_str, value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        self.handle = uuid.uuid1()
        self.ds = ds
        self.matchfns = matchfns
        self.termfn = termfn
        self.name = name

    def __iter__(self):
        self.kiter = iter(self.consumer)
        return(self)

    def __next__(self):
        while True:
            message = next(self.kiter)
            url = message.value[0]
            file_index = message.value[1]
            if self.termfn(url):
                raise StopIteration
            hit = True
            for fn in self.matchfns:
                if not fn(url):
                    hit = False
                    break
            if hit:
                ord_tstamp = url.split('_')[3][1:8] + 'T' + url.split('_')[3][8:-1]
                tstamp = parser.isoparse(ord_tstamp)
                dsver = _pack_version(tstamp.year, int(ord_tstamp[4:7]), tstamp.hour, file_index)
                return {'tstamp':tstamp, 'data':self.ds.Get(name, dsver, None, None, -1, None)}
            else:
                print("not ", url)
        return True

conn_str="sockets://172.17.0.2:4000"

def _url_to_tstamp(url):
    ord_tstamp = url.split('_')[3][1:8] + 'T' + url.split('_')[3][8:-1]
    return(parser.isoparse(ord_tstamp))

def sub(self, **kwargs):
    name = kwargs['name']
    if 'start_time' in kwargs:
            start_time = parser.parse(str(kwargs['start_time']))
    if 'end_time' in kwargs:
            end_time = parser.parse(str(kwargs['end_time']))
    print(_get_abi_platform_id(name))
    matchfns = [lambda a,name=name : re.compile(_get_abi_platform_id(name)).search(a)]
    matchfns.append(lambda a,start_time=start_time : _url_to_tstamp(a) >= start_time)
    termfn = lambda a, end_time=end_time : _url_to_tstamp(a) > end_time
    estream = NSDFEventStream(self.ds, matchfns, termfn, 'test', name)

    return estream

def sub_fn(a):
    if np.max(a) > 30:
        return (a - 30)
    else:
        return(None)

dv = DsVdc(conn_str)
#subscription = dv.sub(f'RadC/C04/Rad', start = '20230101', end = '20240630', lb = lb, ub = ub, xform = sub_fn)
subscription = sub(dv, name = f'RadC/C11/Rad', start_time = '20220101', end_time = '20230630', xform = sub_fn)

print(f'subscription handle: {subscription.handle}')
for result in subscription:
    print(f'received result with timestamp {result["tstamp"]}, max value: {np.max(result["data"])}')
print('all done')


subscription = dv.joinsub(handle)

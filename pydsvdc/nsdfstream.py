from kafka import KafkaConsumer
from dateutil import parser
from dspaces import DSClient
from bitstring import pack, Bits
import json
import uuid

def _pack_version(year, day, hour, fnum, check = 0):
    if check == 0:
        bits = pack('uint:2, uint:8, uint:9, uint:5, uint:8', 0, year-1900, day, hour, fnum)
    elif check == 1:
        minutes = fnum
        bits = pack('uint:2, uint:8, uint:9, uint:5, uint:8', 1, year-1900, day, hour, minutes)
    else:
        print(f'WARNING: version check value mismatch. Expected 0, got {check}.', file=sys.stderr)
    return(bits.uint)

def get_kafka_conn_str():
    kafka_sock = '54.145.37.197:9092'
    return(kafka_sock)

class NSDFEventStream:
    def __init__(self, ds, matchfns, termfn, streamname, name, fn, debug = False):
        topic = streamname
        conn_str = get_kafka_conn_str()
        if debug:
            print(f'topic = {topic}')
        self.consumer = KafkaConsumer(topic, bootstrap_servers=conn_str, value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        self.handle = uuid.uuid1()
        self.ds = ds
        self.matchfns = matchfns
        self.termfn = termfn
        self.name = name
        self.fn = fn
        self.debug = debug

    def __iter__(self):
        self.kiter = iter(self.consumer)
        return(self)

    def __next__(self):
        while True:
            message = next(self.kiter)
            url = message.value[0]
            if self.debug:
                print(f'url = {url}')
            if self.termfn(url):
                if self.debug:
                    print(f'{url} meets termination condtions')
                raise StopIteration
            hit = True
            for fn in self.matchfns:
                if not fn(url):
                    if self.debug:
                        print(f'{url} does not match')
                    hit = False
                    break
            if hit:
                if self.debug:
                    print(f'{url} matches')
                ord_tstamp = url.split('_')[3][1:8] + 'T' + url.split('_')[3][8:-1]
                tstamp = parser.isoparse(ord_tstamp)
                dsver = _pack_version(tstamp.year, int(ord_tstamp[4:7]), tstamp.hour, tstamp.minute, check = 1)
                if self.fn:
                    result = self.ds.Exec(self.name, dsver, fn=self.fn)
                else:
                    result = self.ds.Get(self.name, dsver, None, None, -1, None)
                if result is None:
                    next
                else:
                    return {'tstamp':tstamp, 'data':result}
        return True

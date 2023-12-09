from kafka import KafkaConsumer
from dateutil import parser
from dspaces import DSClient
import json
import uuid


class NSDFEventStream:
    def __init__(self, ds, matchfns, termfn, streamname, name, fn):
        topic = streamname
        conn_str = get_kafka_conn_str()
        print(topic)
        self.consumer = KafkaConsumer(topic, bootstrap_servers=conn_str, value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        self.handle = uuid.uuid1()
        self.ds = ds
        self.matchfns = matchfns
        self.termfn = termfn
        self.name = name
        self.fn = fn

    def __iter__(self):
        self.kiter = iter(self.consumer)
        return(self)

    def __next__(self):
        while True:
            message = next(self.kiter)
            url = message.value[0]
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
                dsver = _pack_version(tstamp.year, int(ord_tstamp[4:7]), tstamp.hour, tstamp.minute, check = 1)
                if self.fn:
                    return {'tstamp':tstamp, 'data':self.ds.Exec(self.name, dsver, fn=self.fn)}
                else:
                    return {'tstamp':tstamp, 'data':self.ds.Get(self.name, dsver, None, None, -1, None)}
        return True

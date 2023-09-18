from pydsvdc.vdcmetadata import *
from dateutil import parser
from bitstring import pack, Bits
from dspaces import DSClient
import numpy

def _unpack_version(version):
    bits = Bits(uint=version, length=32)
    check, year, day, hour, fnum = bits.unpack('uint:2, uint:8, uint:9, uint:5, uint:8')
    if check != 0:
        print(f'WARNING: version check value mismatch. Expected 0, got {check}.', file=sys.stderr)
    return(1900+year, day, hour, fnum)

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

def _do_goes_metadata_query(query):
    results = []
    if 'op' in query:
        pass
    elif 'start_time' not in query and 'end_time' not in query:
        limit = 1
        platform_identifier = query['platform_identifier']
        start_time = query['time']
        results = find_nearest_times_with_limit_and_sort(platform_identifier, start_time, limit)
    return results

def _do_metadata_query(query):
    if query['domain'] == 'goes17':
        return(_do_goes_metadata_query(query))

class DsVdc:
    def __init__(self, conn_str = None, domain = 'goes17'):
        self.domain = domain
        self.ds = DSClient(conn = conn_str)
        self.ds.SetNSpace(domain)

    def query(self, **kwargs):
        query = {}
        query['domain'] = self.domain
        if self.domain == 'goes17':
            if 'name' in kwargs:
                query['platform_identifier'] = _get_abi_platform_id(kwargs['name'])
            var_name = kwargs['name']
        if 'time' in kwargs:
            query['time'] = parser.parse(kwargs['time'])
        if 'start_time' in kwargs:
            query['start_time'] = parser.parse(kwargs['start_time'])
        if 'end_time' in kwargs:
            query['end_time'] = parser.parse(kwargs['end_time'])
        if 'find_max' in kwargs:
            query['op'] = 'max'
            query['op_on'] = kwargs['find_max']
        elif 'find_min' in kwargs:
            query['op'] = 'min'
            query['op_on'] = kwargs['find_min']

        lb = kwargs['lb']
        ub = kwargs['ub']
            
        handles = _do_metadata_query(query)
        results = []
        for (url, file_index) in handles:
            ord_tstamp = url.split('_')[3][1:8] + 'T' + url.split('_')[3][8:-1]
            tstamp = parser.isoparse(ord_tstamp)
            dsver = _pack_version(tstamp.year, int(ord_tstamp[4:7]), tstamp.hour, file_index)
            results.append((tstamp, self.ds.Get(var_name, dsver, lb, ub, -1, None)))
        return(results)


if __name__ == "__main__":
    dv = DsVdc(conn_str = 'sockets://172.17.0.2:4000')
    data = dv.query(domain = 'goes17', name = 'FDCC/Mask', time = '202005010001', lb = (0,0), ub = (100, 100))
    print(data)

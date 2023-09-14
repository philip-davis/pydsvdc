from vdcmetadata import *
from dateutil import parser
from bitstring import pack, Bits

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

if __name__ == "__main__":
    platform_identifier = "ABI-L2-FDCC"  # Please note that the "." is important in both cases for the regex
    start_time = datetime(2020, 5, 7, 10, 0, 0)  # Use a datetime object here
    print(start_time)
    limit = 100  # None will retrieve all from the given point onwards

    results = find_nearest_times_with_limit_and_sort(platform_identifier, start_time, limit)

    if results:
        for i, result in enumerate(results):
            url, file_index = result
            ord_tstamp = url.split('_')[3][1:8] + 'T' + url.split('_')[3][8:-1]
            tstamp = parser.isoparse(ord_tstamp)
            dsver = _pack_version(tstamp.year, int(ord_tstamp[4:7]), tstamp.hour, file_index)
            print(tstamp, dsver, _unpack_version(dsver), result)
    else:
        print("No matching times found.")

from netCDF4 import Dataset
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from pydsvdc import DsVdc
import sys

if sys.argc != 4 and sys.argv != 6:
    print(f'Usage: {sys.argv[0]} <connection_string> <timestamp> <channel> [lower-bounds uppper-bounds]') 

conn_str = sys.argv[1]
timestamp = sys.argv[2]
channel = int(sys.argv[3])
if(sys.argc > 3):
    lb = tuple([int(x) for x in sys.argv[3].split(',')])
    ub = tuple([int(x) for x in sys.argv[4].split(',')])

dv = DsVdc(conn_str)
qres = dv.query(name = f'RadC/C{channel:02d}/Rad', time = timestamp, lb=lb, ub=ub)
area = qres[0][1] # query returns a list of tuples of form (timestamp, ndarray)

# Plot gamma adjusted reflectance
fig = plt.figure(figsize=(4,4),dpi=200)
print(np.count_nonzero(area.mask == False))
im = plt.imshow(area, vmin=0.0, vmax=400000, cmap='Reds')
cb = fig.colorbar(im, orientation='horizontal')
cb.set_ticks([0, 100000, 200000, 300000, 400000])
cb.set_label('Fire Area')
plt.show()

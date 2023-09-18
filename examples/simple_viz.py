from netCDF4 import Dataset
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from pydsvdc import DsVdc
import sys

if len(sys.argv) != 6:
    print(f'Usage: {sys.argv[0]} <connection_string> <timestamp> <channel> lower-bounds upper-bounds') 
    exit(1)

conn_str = sys.argv[1]
timestamp = sys.argv[2]
channel = int(sys.argv[3])
lb = tuple([int(x) for x in sys.argv[4].split(',')])
ub = tuple([int(x) for x in sys.argv[5].split(',')])

dv = DsVdc(conn_str)
qres = dv.query(name = f'RadC/C{channel:02d}/Rad', time = timestamp, lb=lb, ub=ub)
radiance = qres[0][1] # query returns a list of tuples of form (timestamp, ndarray)

# Define some constants needed for the conversion. From the pdf linked above
Esun_Ch_01 = 726.721072
Esun_Ch_02 = 663.274497
Esun_Ch_03 = 441.868715
d2 = 0.3
# Apply the formula to convert radiance to reflectance
ref = (radiance * np.pi * d2) / Esun_Ch_02

# Make sure all data is in the valid data range
ref = np.maximum(ref, 0.0)
ref = np.minimum(ref, 1.0)

# Apply the formula to adjust reflectance gamma
ref_gamma = np.sqrt(ref)

# Plot gamma adjusted reflectance
fig = plt.figure(figsize=(4,4),dpi=200)
im = plt.imshow(ref_gamma, vmin=0.0, vmax=1.0, cmap='Greys_r')
cb = fig.colorbar(im, orientation='horizontal')
cb.set_ticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
cb.set_label('Reflectance')
plt.show()

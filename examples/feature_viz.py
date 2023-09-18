from netCDF4 import Dataset
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from pydsvdc import DsVdc
from dateutil import parser
from datetime import timedelta
import sys

if len(sys.argv) != 4:
    print(f'Usage: {sys.argv[0]} <connection_string> lower-bounds upper-bounds') 
    exit(1)

conn_str = sys.argv[1]
lb = tuple([int(x) for x in sys.argv[2].split(',')])
ub = tuple([int(x) for x in sys.argv[3].split(',')])

dv = DsVdc(conn_str)
qres = dv.query(name = f'FDCC/Mask', start_time = '202005010000', end_time = '202010312359', find_max = 'total_number_of_pixels_with_fire_area', lb=lb, ub=ub)
(tstamp, mask, max_pixels) = qres[0] # query returns a list of tuples of form (timestamp, ndarray, tags...)
print(f'Maximum of {int(max_pixels)} pixels seen with fire area at {tstamp}')

ub = (ub[0]*2+1, ub[1]*2+1)
qres = dv.query(name = f'RadC/C3/Rad', time = tstamp - timedelta(hours=6), lb=lb, ub=ub)
(tstamp, radiance) = qres[0]

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

pixel_count = 0
fire_pixels = np.zeros(radiance.shape)
for i in range(len(mask)):
    for j in range(len(mask[i])):
        if mask[i][j] == 30:
            idx = (slice(2*i,2*i+40), slice(2*j,2*j+40))
            fire_pixels[idx] = .7
            pixel_count = pixel_count + 4
fire_pixels = np.ma.masked_where(fire_pixels == 0,fire_pixels)

# Plot gamma adjusted reflectance
fig = plt.figure(figsize=(4,4),dpi=200)
im = plt.imshow(ref_gamma, vmin=0.0, vmax=1.0, cmap='Greys_r')
im2 = plt.imshow(fire_pixels, vmin=0.0, vmax=1.0, cmap='Reds')
cb = fig.colorbar(im, orientation='horizontal')
cb.set_ticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
cb.set_label('Reflectance')
plt.show()

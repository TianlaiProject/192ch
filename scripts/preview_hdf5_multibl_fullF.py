#!/usr/bin/env python
import numpy as np
import h5py
import matplotlib.pyplot as plt
import sys
import pickle
import numpy.ma as ma
import BLindex

t0 = 80
t1 = 110

bl = BLindex.BLindex([int(sys.argv[2]),int(sys.argv[3])])
data = h5py.File(sys.argv[1], 'r')
vis = data['vis'][t0 : t1, :, bl]
vi = ma.masked_invalid(vis)

#delta_f = 250. / 2048
#f_axis = np.arange(0, 1024*delta_f, delta_f) + 685
f_axis = np.arange(len(vi[0]))
t_axis = np.arange(len(vi))

plt.figure(figsize=(20,10))
plt.title('Amplitude 2D')
plt.pcolormesh(f_axis, t_axis, np.abs(vi))
plt.colorbar()
plt.xlim(min(f_axis),max(f_axis))
plt.ylim(min(t_axis),max(t_axis))
plt.xlabel('Frequency')
plt.ylabel('Time')

plt.figure(figsize=(20,10))
plt.title('Amplitude 1D')
plt.plot(t_axis, np.sum(np.abs(vi), axis = 1))
plt.grid()
#plt.xlim(min(f_axis),max(f_axis))
#plt.ylim(min(t_axis),max(t_axis))
#plt.xlabel('Frequency')
plt.xlabel('Time')

plt.figure(figsize = (20,10))
plt.title('Spectrum')
plt.plot(f_axis, ma.mean(np.abs(vi), axis=0))
plt.xlabel('Frequency points')
plt.ylabel('Amplitude')

#plt.figure(figsize = (20,10))
#plt.title('Phase 2D')
#plt.pcolormesh(f_axis, t_axis, np.angle(vi))
#plt.xlabel('Frequency')
#plt.ylabel('Time')
#plt.xlim(min(f_axis),max(f_axis))

plt.show()


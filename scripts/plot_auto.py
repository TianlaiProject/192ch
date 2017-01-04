#!/usr/bin/env python
import numpy as np
import h5py
import matplotlib.pyplot as plt
import numpy.ma as ma
import BLindex
from multiprocessing import Pool
import sys

def plot_auto(i):
    print "plot figure", i
    bl = BLindex.BLindex([i, i])
    vi = vis[:, :, bl]
    vi = ma.masked_invalid(vi)
   
    plt.figure(figsize = (16,8))
    #plt.plot(ma.mean(np.abs(vi), axis=0))
    f_1d = ma.mean(np.abs(vi), axis=0)
    plt.plot(f_1d)
    plt.title('Spectrum of %d_%d (Mean = %.2fe10)' % (i, i, ma.mean(f_1d)/1e10))
    plt.xlabel('Frequency points')
    plt.ylabel('Amplitude')
    plt.grid()
    plt.savefig('auto_graph/%d_%d.png' % (i, i))
 

t0 = 4
t1 = 45

data = h5py.File(sys.argv[1], 'r')
vis = data['vis'][t0 : t1]

p = Pool(10)
p.map(plot_auto, xrange(1, 193))


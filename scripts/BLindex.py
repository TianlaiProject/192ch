##########################################################
# Raw data processing program for 192 channel correlator #
# made of Institute of Automation.                       #
# This is verison 0.0 last modified on 2015/10/20.       #
# Any problem, contact jxli@bao.ac.cn                    #
##########################################################

import numpy as np
import matplotlib.pyplot as plt
import struct
import time
import sys
import ephem

def BLindex(bl_name = -1):
    '''
    Return baseline bl_name's index from a 2D array, where bl_name can be a list like [1,2] or an array like np.array([1,2]).
    When use default value bl_name == -1, return the baseline 2D array.
    Mostly translated from lichengcheng's matlab script.
    '''
    order = 0
    data_pair = np.empty((18528, 2), int) # 18528 = (192 + 1) * 192 / 2
    for i in xrange(96):
        j = 2*i + 3
        for k in xrange(47):
            j = j - j/193*192
            data_pair[order,0] = 2*i+2
            data_pair[order,1] = j+1
            order=order+1
            data_pair[order,0] = 2*i+1
            data_pair[order,1] = j
            order=order+1
            data_pair[order,0] = 2*i+2
            data_pair[order,1] = j
            order=order+1
            data_pair[order,0] = 2*i+1
            data_pair[order,1] = j+1
            order=order+1
            j=j+2
    for k in xrange(48):
        i = 2*k + 1
        data_pair[order,0] = i+1
        data_pair[order,1] = i+97
        order=order+1;
        data_pair[order,0] = i
        data_pair[order,1] = i+96
        order=order+1
        data_pair[order,0] = i+1
        data_pair[order,1] = i+96
        order=order+1
        data_pair[order,0] = i
        data_pair[order,1] = i+97
        order=order+1
    for k in xrange(96):
        i = 2*k + 1
        data_pair[order,0] = i+1
        data_pair[order,1] = i+1
        order=order+1
        data_pair[order,0] = i
        data_pair[order,1] = i
        order=order+1
        data_pair[order,0] = i+1
        data_pair[order,1] = i
        order=order+1
    try:
        return np.where(np.all(bl_name == data_pair, axis=1))[0][0]
    except IndexError:
        try:
            return np.where(np.all(bl_name[::-1] == data_pair, axis=1))[0][0]
        except:
            if bl_name == -1:
                return data_pair
            else:
                print 'Error: Cannot find baseline %s.' % str(bl_name)
                return

if __name__ == '__main__':
    t = int(sys.argv[1])
    #tm_st = time.time()
    #raw_data_file = open('./data/20151019123456/f56_00000_1_2_5_6_9_10.dat', 'rb') # very first.
    #raw_data_file = open('/home/camera/data/20151023213353/f56_00000.dat', 'rb') # wufq
    #raw_data_file = open('/home/camera/data/20151023224431/f56_00000.dat', 'rb') #1st
    #raw_data_file = open('/home/camera/data/20151023232424/f56_00000.dat', 'rb')
    #raw_data_file = open('/home/camera/data/20151023234024/f56_00000.dat', 'rb')
    #raw_data_file = open('/home/camera/data/20151023235632/f56_00000.dat', 'rb')
    #raw_data_file = open('/home/camera/data/20151023235720/f56_00000.dat', 'rb') # good.
    #raw_data_file = open('/home/camera/data/20151024112550/f56_00001.dat', 'rb')
    #raw_data_file = open('/home/camera/data/20151029020428/f56_00001.dat', 'rb')
    #raw_data_file = open('/home/camera/data/20151030114635/f56_00000.dat', 'rb')
    #raw_data_file = open('/home/camera/data/20151030115725/f56_00000.dat', 'rb')
    #raw_data_file = open('/home/camera/data/20151103214557/f56_00000.dat', 'rb')
    raw_data_file = open('/home/camera/data/20151124181312/lz1_00000.dat', 'rb')

    raw_data_file.seek(4,0)
    st_fre_id = np.fromstring(raw_data_file.read(4), np.int32)[0]
    raw_data_file.seek(-8,1)
    if st_fre_id != 42:
        print 'Not start from beginning.'
        fre_id = np.arange(42,1008).reshape(72-3,14).swapaxes(0,1).reshape(-1)
        for i in xrange(len(fre_id)):
            if fre_id[i] == st_fre_id:
                break
        raw_data_file.seek((1008-42-i)*(8+598*248), 1)

    raw_data = raw_data_file.read(t*(1008-42)*(8+598*248)) # Load data.
    b = np.fromstring(raw_data, np.complex64)
    d = b.reshape(t,14,(72-42/14),-1).swapaxes(1,2).reshape(t,1008-42, -1)
    #print time.time() - tm_st
    #exit()

    #chns = [1,2,5,6,9,10]
    #chns = [5,6,7,8]
    chns = [168]
    #chns = [37,38,39,40]
    #chns = [165,166,167,168]

# work part 1.
#    for i in xrange(len(chns)):
#        #plt.plot(d[t-1, :, BLindex([chns[i],chns[i]]) + 1].real, label='%d_%d' % (chns[i],chns[i]))
#        plt.plot(np.log10(d[t-1, :, BLindex([chns[i],chns[i]]) + 1].real), label='%d_%d' % (chns[i],chns[i]))
#    plt.legend(loc='best')
#    plt.grid()
#    #plt.ylim(-2,)
#    plt.xlabel('Frequency(Point)')
#    plt.ylabel('Correlation(dB)')
#    plt.show()
#    exit()

    #a = np.unwrap(np.angle(d[1, 270:650, BLindex([chns[0], chns[1]]) + 1]))
    #print a[0], a[-1], a[-1]-a[0]
    #delta_f = (650.-270) * (125e6/1024.)
    #delta_t = (a[-1] - a[0]) / delta_f / 2./ np.pi
    #print 'delta_t =', delta_t
    #c_cable_speed = 7.5 / delta_t
    #print 'cable c speed(m/s) =', c_cable_speed
    #c_percent = c_cable_speed / ephem.c
    #print 'cable c speed(%) =', c_percent
    print
    #plt.plot(np.unwrap(np.angle(d[1, 270:650, BLindex([chns[0], chns[1]]) + 1])))
    plt.figure(figsize=(12,8))
    plt.title('2D Phase')
    plt.pcolormesh(np.abs(d[:, :, BLindex([chns[0], chns[0]]) + 1 ]).swapaxes(0,1))
    #plt.pcolormesh(np.abs(d[:, :, BLindex([chns[0], chns[1]]) + 1 ]))
    #plt.pcolormesh(np.angle(d[:, :, BLindex([chns[0],chns[1]])+1]).swapaxes(0,1))
    plt.colorbar()
    plt.xlim(0, 10)
    plt.ylim(0, 966)
    plt.ylabel('Frequency(Point)')
    plt.xlabel('Time(integration cycle[4sec])')
    #plt.savefig('test_phase_1.png')
    plt.legend(loc='best')
    plt.grid()
    plt.show()
    raw_data_file.close()




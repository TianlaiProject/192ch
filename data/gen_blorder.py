import numpy as np

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

# save blorder to file
np.savetxt('blorder.dat', data_pair, fmt='%4d')

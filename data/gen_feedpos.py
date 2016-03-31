import numpy as np

nfeeds = [31, 32, 33]
feed_delta = [31*0.4/30, 0.4, 31*0.4/32] # meter
cyl_width = 15.0 # meter

feedpos = np.zeros((sum(nfeeds), 3), dtype=np.float32)
feedno = 0
for idx, (nfeed, delta) in enumerate(zip(nfeeds, feed_delta)):
    for feed in range(nfeed):
        feedpos[feedno] = np.array([idx * cyl_width, feed * delta, 0.0])
        feedno += 1

# save feedpos to file
np.savetxt('feedpos.dat', feedpos, fmt='%10.6f')
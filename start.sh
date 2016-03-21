#!/bin/bash 
#Cur_Dir=$(pwd)
#filepath=$Cur_Dir'/data/'
filepath='/home/data_hdf5/'
#outputpath=$Cur_Dir'/tmp/'
outputpath='/home/tmp_hdf5/'
filetime=`date "+%Y%m%d%H%M%S"`
outfile=${outputpath}
filepath=${filepath}${filetime}
if [ -d "$filepath" ]
then
    echo "The folder existed"
else
    nohup ./recvPacket $filepath 1>${outfile}recv_std.out 2>${outfile}recv_err.out &
#    ./recvPacket $filepath 
fi


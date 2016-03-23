#!/usr/bin/env bash


create_time=`date "+%Y%m%d%H%M%S"`

cur_path=$(pwd)
config_file="${cur_path}/cyl_config.ini"

data_path='/home/data_hdf5/'
data_path=${data_path}${create_time}

output_path='/home/tmp_hdf5/'
output_path=${output_path}${create_time}

if [ ! -d "$data_path" ]
then
    echo "Data folder $data_path does not exist, create it..."
    mkdir $data_path
else
    echo "Data folder $data_path exists, data will be saved in it..."
fi

if [ ! -d "$output_path" ]
then
    echo "Output folder $output_path does not exist, create it..."
    mkdir $output_path
else
    echo "Output folder $output_path exists, output will be saved in it..."
fi

if test -e "$config_file"
then
    echo "Run program with configure parameters in file $config_file..."
    nohup ./recvPacket $data_path $config_file -v --gen_obslog 1>${outputpath}recv_std.out 2>${outputpath}recv_err.out &
    # ./recvPacket $data_path $config_file
else
    echo "Error: configure parameters file $config_file does not exit!!!"
fi



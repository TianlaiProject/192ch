#!/usr/bin/env bash

PID=$(pgrep recvPacket)
kill -USR1 $PID

echo ""
t=48
while [ $( pgrep recvPacket ) ]
do
  printf "\rNow Exiting!!! Please wait < %2d seconds." $t
  sleep 1
  t=$[$t-1]
  if [ $t -lt 0 ]
  then break
  fi
done

if [ $t -lt 0 ]
  then printf "\n[Error] The Process 'recvPacket' can not Exit!!!\n"
else
  printf "\nSuccessfully Exit.\n"
fi


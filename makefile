objects = ini.o recvdev.o

all: recvPacket

#recvPacket:recvdev.c
#	h5cc ini.c recvdev.c -fopenmp -o recvPacket -Wall 
#	gcc recvdev.c -fopenmp -o recvPacket -Wall 
#	gcc recvdev.c -fopenmp -lpcap -o recvPacket -Wall 
recvPacket: $(objects)
	#h5cc -L/usr/lib/python2.7/ -lpython2.7 -fopenmp -ldl -O3 $(objects) -o recvPacket
	h5cc -L/usr/lib/python2.7/ -lpython2.7  -openmp -ldl $(objects) -o recvPacket
recvdev.o: recvdev.c
	#h5cc -g -std=gnu99 -fopenmp -Wall -c -O3 recvdev.c -I/usr/include/python2.7/
	h5cc -g -std=gnu99  -openmp -Wall -c recvdev.c -I/usr/include/python2.7/
ini.o: ini.c ini.h
	#gcc -c ini.c
	icc -c ini.c

.PHONY : clean
clean:
	rm recvPacket $(objects)

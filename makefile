objects = ini.o recvdev.o

PYINC=-I/usr/include/python2.7
PYLIB=-L/usr/lib/python2.7 -lpython2.7 -lpthread -ldl -lutil
# PYINC=-I/opt/python-2.7.5-mkl/include/python2.7
# PYLIB=-L/opt/python-2.7.5-mkl/lib -lpython2.7 -lpthread -ldl -lutil


all: recvPacket

recvPacket: $(objects)
	# h5cc $(PYLIB) -fopenmp -pthread -O3 $(objects) -o recvPacket
	h5cc $(PYLIB) -fopenmp -pthread $(objects) -o recvPacket
recvdev.o: recvdev.c
	# h5cc -g -std=gnu99 -fopenmp -pthread -Wall -O3 -c recvdev.c $(PYINC)
	h5cc -g -std=gnu99  -fopenmp -pthread -Wall -c recvdev.c $(PYINC)
ini.o: ini.c ini.h
	gcc -c ini.c
	# icc -c ini.c

.PHONY : clean
clean:
	rm recvPacket $(objects)

all:recvPacket 
recvPacket:recvdev.c
	h5cc ini.c recvdev.c -fopenmp -o recvPacket -Wall 
#	gcc recvdev.c -fopenmp -o recvPacket -Wall 
#	gcc recvdev.c -fopenmp -lpcap -o recvPacket -Wall 
clean:
	rm recvPacket 

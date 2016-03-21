all:recvPacket 
recvPacket:recvdev.c
	h5cc recvdev.c -fopenmp -o recvPacket -Wall 
#	gcc recvdev.c -fopenmp -o recvPacket -Wall 
#	gcc recvdev.c -fopenmp -lpcap -o recvPacket -Wall 
clean:
	rm recvPacket 

#include <stdio.h>
#include <stdlib.h>
//#include <pcap.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
//#include <math.h>
#include <omp.h>
#include <netinet/in.h>
//#include <net/if.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/if_packet.h>
#include <linux/if_ether.h>
#include <linux/if_arp.h>
#include <signal.h>
#include "hdf5.h"

#define BUFSIZE          2000
#define MAX_PACKET_ID    100
#define MAX_PACKET_SIZE  1488
#define FIRST_PACKET_SIZE  1488 - 8
#define MIN_PACKET_SIZE  1000
#define MAX_RAWPACKET_SIZE 1510
#define MIN_RAWPACKET_SIZE 1022

#define N_BASELINE ((MAX_PACKET_ID - 1)*MAX_PACKET_SIZE + MIN_PACKET_SIZE - 88) / 8 // 4 bytes real and 4 bytes imag
#define N_FREQUENCY 1008 //(1008 - 42)
#define N_INTEGRA_TIME 10       // N_INTEGRA_TIME integration times in one buf
#define buflen 8 * N_BASELINE * N_FREQUENCY * N_INTEGRA_TIME // Bytes
#define N_BUFFER_PER_FILE 45   // 30 min data per file
#define N_TIME_PER_FILE N_INTEGRA_TIME * N_BUFFER_PER_FILE


typedef struct {
    float   r;
    float   i;
} complex_t;


int Running = 1, DataExist = 1;
//u_char Src[12];
//u_char Flags[4];
FILE *fp; // Log file pointer.
hid_t file_id, filetype, memtype, dataspace_id, dataset_id; /* HDF% handles */
hsize_t dims[3] = {N_TIME_PER_FILE, N_FREQUENCY, N_BASELINE};
hsize_t sub_dims[3] = {N_INTEGRA_TIME, N_FREQUENCY, N_BASELINE};
hsize_t offset[3] = {0, 0, 0}; /* subset offset in the file */
hsize_t count[3] = {N_INTEGRA_TIME, N_FREQUENCY, N_BASELINE}; /* size of subset in the file */
hsize_t stride[3] = {1, 1, 1}; /* subset stride in the file */
hsize_t block[3] = {1, 1, 1}; /* subset block in the file */

int buf_cnt = 0;
int file_count = 0;
int folder_state = 1;

unsigned char * buf01;
unsigned char * buf02;
int buf01_state = 0 ;
int buf02_state = 0 ;

char filepath[150];

/////////////////////////////////////////////
//int cmpSrcAddress( u_char *SrcA , u_char *SrcB);
//int cmpFlags(u_char *SrcA);
//////////////////////////////////////////////

/*
  int cmpSrcAddress( u_char *SrcA , u_char *SrcB)
  {
  int i;

  for (i=0;i<12;++i)
  {
  if (SrcA[i] != SrcB[i])
  return 0;
  }
  return 1;
  }

  int cmpFlags( u_char *SrcA  )
  {
  int i;

  for (i=0;i<4;++i)
  {
  if ( SrcA[i] != Flags[i] )
  return 0;
  }
  return 1;

  }
*/


// When use end.sh to kill, running this function
void kill_handler(int sig_no)
{
    if (sig_no == SIGUSR1) Running = 0;
}


void get_filepath(char *time_file_path)
{
    char folderpath[150];

    strcpy(filepath,time_file_path);
    strcpy(folderpath,filepath);
    //create a folder each time we run the program
    if(folder_state==1)
    if(access(folderpath, F_OK)==-1)
    {
        int flag0=mkdir(folderpath,0755);
        if(flag0)
        {
            printf("Failed to create the folder!!!\n");
            exit(-1);
        }
    }
    strcat(filepath,"/");

    folder_state = 0;
}


void gen_datafile()
{
    char filename[27];
    char data_path0[150];
    strcpy(data_path0,filepath);

    sprintf(filename, "lz1_%05d.hdf5", file_count);
    strcat(data_path0,filename);

    herr_t status;

    // Create a new hdf5 file using the default properties.
    file_id = H5Fcreate (data_path0, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    // Create the compound datatype for memory.
    memtype = H5Tcreate (H5T_COMPOUND, sizeof (complex_t));
    status = H5Tinsert (memtype, "r", HOFFSET(complex_t, r), H5T_NATIVE_FLOAT);
    status = H5Tinsert (memtype, "i", HOFFSET(complex_t, i), H5T_NATIVE_FLOAT);
    // Create the compound datatype for the file.
    filetype = H5Tcreate (H5T_COMPOUND, 4 + 4);
    status = H5Tinsert (filetype, "r", 0, H5T_IEEE_F32LE);
    status = H5Tinsert (filetype, "i", 4, H5T_IEEE_F32LE);
    // Create dataspace.  Setting maximum size to NULL sets the maximum size to be the current size.
    dataspace_id = H5Screate_simple (3, dims, NULL);
    // Create the dataset to write the compound data to it later.
    dataset_id = H5Dcreate (file_id, "vis", filetype, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    file_count++;
}


void writeData()
{
    complex_t * cbuf;
    herr_t      status;
    hid_t       sub_dataspace_id;

    gen_datafile();
    int buf_init_iter;
    while (1)
    {
        if( buf01_state == 1 )
        {
            offset[0] = buf_cnt*N_INTEGRA_TIME;
            // Create memory space with size of subset.
            sub_dataspace_id = H5Screate_simple (3, sub_dims, NULL);
            // Select subset from file dataspace.
            status = H5Sselect_hyperslab (dataspace_id, H5S_SELECT_SET, offset, stride, count, block);
            // Write a subset of data to the dataset.
            cbuf = (complex_t *)buf01;
            status = H5Dwrite (dataset_id, memtype, sub_dataspace_id, dataspace_id, H5P_DEFAULT, cbuf);
            if (buf_cnt == N_BUFFER_PER_FILE - 1)
            {
                // Close and release resources.
                status = H5Dclose (dataset_id);
                status = H5Sclose (dataspace_id);
                status = H5Tclose (memtype);
                status = H5Tclose (filetype);
                status = H5Fclose (file_id);

                buf_cnt = 0;
                gen_datafile();
            }
            else
            {
                buf_cnt++;
            }

            // re-initialize the buffer
            for (buf_init_iter=0; buf_init_iter<buflen; buf_init_iter++)
                buf01[buf_init_iter] = 0xFF;
            buf01_state = 0;
        }
        else if( buf02_state == 1 )
        {
            offset[0] = buf_cnt*N_INTEGRA_TIME;
            // Create memory space with size of subset.
            sub_dataspace_id = H5Screate_simple (3, sub_dims, NULL);
            // Select subset from file dataspace.
            status = H5Sselect_hyperslab (dataspace_id, H5S_SELECT_SET, offset, stride, count, block);
            // Write a subset of data to the dataset.
            cbuf = (complex_t *)buf02;
            status = H5Dwrite (dataset_id, memtype, sub_dataspace_id, dataspace_id, H5P_DEFAULT, cbuf);
            if (buf_cnt == N_BUFFER_PER_FILE - 1)
            {
                // Close and release resources.
                status = H5Dclose (dataset_id);
                status = H5Sclose (dataspace_id);
                status = H5Tclose (memtype);
                status = H5Tclose (filetype);
                status = H5Fclose (file_id);

                buf_cnt = 0;
                gen_datafile();
            }
            else
            {
                buf_cnt++;
            }

            // re-initialize the buffer
            for (buf_init_iter=0; buf_init_iter<buflen; buf_init_iter++)
                buf02[buf_init_iter] = 0xFF;
            buf02_state = 0;
        }
        if (buf01_state == 0 && buf02_state == 0 && DataExist == 0)
        {
            // Close and release resources.
            status = H5Dclose (dataset_id);
            status = H5Sclose (dataspace_id);
            status = H5Tclose (memtype);
            status = H5Tclose (filetype);
            status = H5Fclose (file_id);

            break;
        }
    }
}


void recvData()
{
    char *opt = "enp131s0d1";
    register int packet_len ;
    char log_path0[150];
    register int row = 0;
    register int init_cnt, current_cnt, freq_ind, pkt_id, pkt_id_old=-1;
    register int row_in_buf = N_FREQUENCY * N_INTEGRA_TIME;
    register long row_size = 8 * N_BASELINE;

    strcpy(log_path0, filepath);
    strcat(log_path0, "datadev0.log");
    fp= fopen(log_path0, "wb");

    u_char frame_buff[BUFSIZE];
    u_char * frame_buff_p = frame_buff;
    u_char * start_buf_p;
    u_char * start_frame_p;
    int copy_len;

    int recv_fd;
    recv_fd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL));
    struct sockaddr_ll sll;
    struct ifreq ifr;
    bzero(&sll, sizeof(sll));
    bzero(&ifr, sizeof(ifr));
    strncpy((char *)ifr.ifr_name, opt, IFNAMSIZ);
    ioctl(recv_fd, SIOCGIFINDEX, &ifr);
    sll.sll_family   = AF_PACKET;
    sll.sll_protocol = htons(ETH_P_ALL);
    sll.sll_ifindex  = ifr.ifr_ifindex;
    bind(recv_fd, (struct sockaddr *) &sll, sizeof(sll));

    printf("Begin to recv data ... \n");
    fflush(stdout);

    int i = 0;
    int old_cnt;
    while (Running) // Find packet zero.
    {
        packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
        //        if (packet_len == MAX_RAWPACKET_SIZE || packet_len == MIN_RAWPACKET_SIZE)
        if (*(int *)(frame_buff_p + 18) == 0) //find pkt 0.
        {
            if (i == 0)
                old_cnt = *(int *)(frame_buff_p + 22);
            else
            {
                init_cnt = *(int *)(frame_buff_p + 22);
                if (init_cnt != old_cnt)
                {
                    pkt_id = 0;
                    break;
                }
            }
            i++;
        }
    }

    while(Running)
    {
        row = *(int *)(frame_buff_p + 26);
        if (buf01_state == 0)
        {
            while (row < row_in_buf)
            {
                if (pkt_id == 0)
                {
                    start_buf_p = buf01 + row*row_size;
                    start_frame_p = frame_buff_p + 30;
                    copy_len = packet_len - 30;
                }
                else if (pkt_id == 99)
                {
                    start_buf_p = buf01 + row*row_size + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22 - 80;
                }
                else
                {
                    start_buf_p = buf01 + row*row_size + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22;
                }

                memcpy(start_buf_p, start_frame_p, copy_len);
                //                while (1)
                //                {
                packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
                //                    if (packet_len == MAX_RAWPACKET_SIZE || packet_len == MIN_RAWPACKET_SIZE)
                //                    {
                //                      pkt_id = (int)frame_buff[18];
                //                      if (pkt_id < pkt_id_old) row++;
                pkt_id = *(int *)(frame_buff_p + 18);
                if (pkt_id == 0)
                {
                    current_cnt = *(int *)(frame_buff_p + 22);
                    freq_ind = *(int *)(frame_buff_p + 26);
                    row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind;
                    printf("%d ", row);
                }
                else if (pkt_id < pkt_id_old)
                {
                    while(1)
                    {
                        packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
                        pkt_id = *(int *)(frame_buff_p + 18);
                        if (pkt_id == 0)
                        {
                            current_cnt = *(int *)(frame_buff_p + 22);
                            freq_ind = *(int *)(frame_buff_p + 26);
                            row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind;
                            printf("%d ", row);
                            break;
                        }
                    }
                }

                if (((pkt_id + MAX_PACKET_ID - pkt_id_old) % MAX_PACKET_ID) != 1)
                    fprintf(fp, "Jump from %d to %d.\n", pkt_id_old, pkt_id);
                pkt_id_old = pkt_id;
                //                        break;
                //                    }
                //                }
            }
            buf01_state = 1;
            init_cnt = current_cnt;
        }
        else if (buf02_state == 0)
        {
            while (row < row_in_buf)
            {
                if (pkt_id == 0)
                {
                    start_buf_p = buf02 + row*row_size;
                    start_frame_p = frame_buff_p + 30;
                    copy_len = packet_len - 30;
                }
                else if (pkt_id == 99)
                {
                    start_buf_p = buf02 + row*row_size + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22 - 80;
                }
                else
                {
                    start_buf_p = buf02 + row*row_size + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22;
                }

                memcpy(start_buf_p, start_frame_p, copy_len);
                //                while (1)
                //                {
                packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
                //                    if (packet_len == MAX_RAWPACKET_SIZE || packet_len == MIN_RAWPACKET_SIZE)
                //                    {
                //                      pkt_id = (int)frame_buff[18];
                //                      if (pkt_id < pkt_id_old) row++;
                pkt_id = *(int *)(frame_buff_p + 18);
                if (pkt_id == 0)
                {
                    current_cnt = *(int *)(frame_buff_p + 22);
                    freq_ind = *(int *)(frame_buff_p + 26);
                    row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind;
                    printf("%d ", row);
                }
                else if (pkt_id < pkt_id_old)
                {
                    while(1)
                    {
                        packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
                        pkt_id = *(int *)(frame_buff_p + 18);
                        if (pkt_id == 0)
                        {
                            current_cnt = *(int *)(frame_buff_p + 22);
                            freq_ind = *(int *)(frame_buff_p + 26);
                            row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind;
                            printf("%d ", row);
                            break;
                        }
                    }
                }

                if (((pkt_id + MAX_PACKET_ID - pkt_id_old) % MAX_PACKET_ID) != 1)
                    fprintf(fp, "Jump from %d to %d.\n", pkt_id_old, pkt_id);
                pkt_id_old = pkt_id;
                //                        break;
                //                    }
                //                }
            }
            buf02_state = 1;
            init_cnt = current_cnt;
        }
        else
        {
            printf("Buf01 and Buf02 are both full.\n");
            fflush(stdout);
            Running = 0;
        }
    }
    sleep(0.2);
    DataExist = 0;

    fclose(fp);
}


int main(int argc, char* argv[])
{
    int i;
    int thread_id;

    signal(SIGUSR1, kill_handler);

    // Receiver MAC.
    //Src[0]=0xf4;
    //Src[1]=0x52;
    //Src[2]=0x14;
    //Src[3]=0x1f;
    //Src[4]=0x36;
    //Src[5]=0x51;
    ////Sender MAC
    //Src[6]=0xAA;
    //Src[7]=0xBB;
    //Src[8]=0xCC;
    //Src[9]=0x04;
    //Src[10]=0xDD;
    //Src[11]=0xEE;

    //Flags[0]=0xff ;
    //Flags[1]=0xff ;
    //Flags[2]=0xff ;
    //Flags[3]=0xff ;


    buf01=(unsigned char *)malloc( sizeof(unsigned char)*buflen );
    buf02=(unsigned char *)malloc( sizeof(unsigned char)*buflen );
    for (i=0; i<buflen; i++)
    {
        buf01[i] = 0xFF;
        buf02[i] = 0xFF;
    }

    get_filepath(argv[1]);

    #pragma omp parallel num_threads(2) private(thread_id)
    {
        thread_id = omp_get_thread_num();
        if(thread_id == 0)
            recvData();
        //else if(thread_id == 1)
        //    checkData();
        else if(thread_id == 1)
            writeData();
    }

    free(buf01);
    free(buf02);

    printf("Over.\n");
    fflush(stdout);
    return 0;
}

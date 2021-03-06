#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <argp.h>
#include <omp.h>
//#include <math.h>
#include <time.h>
//#include <pcap.h>
//#include <net/if.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <linux/if_packet.h>
#include <linux/if_ether.h>
#include <linux/if_arp.h>
#include "hdf5.h"
#include "hdf5_hl.h"
#include "ini.h"
#include <time.h>

#define DEVICE_NAME "enp131s0d1"  // the name of the network device

#define BUFSIZE            2048
#define MAX_PACKET_ID      100
#define MAX_PACKET_SIZE    1488
#define FIRST_PACKET_SIZE  1488 - 8
#define MIN_PACKET_SIZE    1000
#define MAX_RAWPACKET_SIZE 1510
#define MIN_RAWPACKET_SIZE 1022

#define N_BASELINE ((MAX_PACKET_ID - 1)*MAX_PACKET_SIZE + MIN_PACKET_SIZE - 88) / 8 // 4 bytes real and 4 bytes imag
#define N_FREQUENCY 1008 //(1008 - 42)
#define FREQ_OFFSET 257
#define N_INTEGRA_TIME 10       // N_INTEGRA_TIME integration times in one buf
#define buflen 8 * N_BASELINE * N_FREQUENCY * N_INTEGRA_TIME // Bytes
//#define bufethlen MAX_RAWPACKET_SIZE // Bytes
#define bufethlen BUFSIZE // Bytes
#define bufethsize MAX_PACKET_ID * N_FREQUENCY * 2 //* N_INTEGRA_TIME 
//#define N_BUFFER_PER_FILE 45   // 30 min data per file
#define N_BUFFER_PER_FILE 15   // 3 min data per file
#define N_TIME_PER_FILE N_INTEGRA_TIME * N_BUFFER_PER_FILE

// weather related
#define WEATHER_PATH     "cosm2_share/newest_weather_data.txt"


/* Used to save data as complex float number in HDF5 file. */
typedef struct {
    float   r;
    float   i;
} complex_t;

/* Used by main to communicate with parse_opt. */
typedef struct
{
  char *args[2];                /* data_path & config_file */
  int verbose;
  int gen_obslog;
} arguments;

/* Used by main to save parsed parameters set in the input configure file. */
typedef struct
{
    const char* nickname;
    const char* comment;
    const char* observer;
    int nns;
    // const char* nsstart;
    // const char* nsstop;
    // const char* nscycle;
    char* nsstart;
    char* nsstop;
    char* nscycle;
    float inttime;
    float weatherperiod;
    const char* keywordver;
    const char* recvver;
    const char* corrver;
    const char* telescope;
    const char* history;
    const char* sitename;
    float sitelat;
    float sitelon;
    float siteelev;
    const char* timezone;
    const char* epoch;
    float dishdiam;
    int nants;
    int nfeeds;
    int npols;
    float cylen;
    float cywid;
    float lofreq;
    int samplingbits;
    int corrmode;
    int nfreq;
    float freqstart;
    float freqstep;
} configuration;


arguments agmts;
configuration config;
int Running = 1, DataExist = 1;
//u_char Src[12];
//u_char Flags[4];
hid_t file_id, filetype, memtype, dataspace_id, dataset_id; /* HDF5 handles */
hid_t weather_dset;
hsize_t dims[3] = {N_TIME_PER_FILE, N_FREQUENCY, N_BASELINE};
hsize_t sub_dims[3] = {N_INTEGRA_TIME, N_FREQUENCY, N_BASELINE};
hsize_t offset[3] = {0, 0, 0}; /* subset offset in the file */
hsize_t count[3] = {N_INTEGRA_TIME, N_FREQUENCY, N_BASELINE}; /* size of subset in the file */
hsize_t stride[3] = {1, 1, 1}; /* subset stride in the file */
hsize_t block[3] = {1, 1, 1}; /* subset block in the file */

int buf_cnt = 0;
int file_count = 0;
int pkt_id = -1; // jx
int weather_exist = 0;
char weather_file[1024];
struct itimerval new_value, old_value; // for timer use
int timer_cnt = 0;
float accurate_inttime; // integration time, Unit: second
int nfeeds, nchans, nbls, nns, nweather;
int *feedno, *channo, *blorder;
float *feedpos, *antpointing, *pointingtime, *polerr, *nspos, *noisesource, *weather;
const float *transitsource[] = {}; // no transit source for cylinder array

unsigned char * buf01;
unsigned char * buf02;
int buf01_state = 0 ;
int buf02_state = 0 ;

// the bufeth is used for catching the online data from ethernet.
//unsigned char bufeth[bufethsize][bufethlen] = {};
//int bufeth_state[bufethsize] = {};
unsigned char **bufeth;
int *bufeth_state;
//omp_lock_t w_lock[bufethsize];
//omp_lock_t r_lock[bufethsize];
omp_lock_t w_lock[2];
omp_lock_t r_lock[2];

PyObject *pMainDict = NULL;

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
    if (sig_no == SIGUSR1)
        Running = 0;
}


// timer handler to get weather data
void timer_handler(int sig_no)
{
    int i;
    FILE *fp;
    char str[13][20];

    if (sig_no == SIGALRM && timer_cnt < nweather)
    {
        timer_cnt++;

        fp = fopen(weather_file, "r");
        if (fp == NULL)
        {
            printf("Error: Fail to open file %s, no weather data will get for this time\n", weather_file);
            return;
        }

        while (fscanf(fp, "%s %s %s %s %s %s %s %s %s %s %s %s %s", str[0], str[1], str[2], str[3], str[4], str[5], str[6], str[7], str[8], str[9], str[10], str[11], str[12]) == 13)
        {
            if (str[0][0] != '#')
            {
                // maybe should check if data is up to date
                // only use values 1, 2, 3, 4, 5, 6, 7, 8, 10, 12
                for (i=1; i<9; i++)
                    weather[10*timer_cnt+i-1] = strtod(str[i], (char **)&str[i]);
                weather[10*timer_cnt+8] = strtod(str[10], (char **)&str[10]);
                weather[10*timer_cnt+9] = strtod(str[12], (char **)&str[12]);

                break;
            }
        }

        fclose(fp);
    }
}


void create_data_path(const char *data_path)
{
    if( access(data_path, F_OK) == -1 )
    {
        if (agmts.verbose)
            printf("Data path %s does not exists, create it...\n", data_path);
        if( mkdir(data_path, 0755) == -1 )
        {
            printf("Error: Failed to create the data path %s!!!\n", data_path);
            exit(1);
        }
    }
    else if( access(data_path, W_OK) == -1 )
    {
        printf("Error: Data path %s does not writable!!!\n", data_path);
        exit(1);
    }
}


void init_buf()
{
    int i, j;
    char data_dir[1024], feedpos_dir[1024], blorder_dir[1024], nspos_dir[1024];
    FILE *data_file;

    // allocate and initialize data receiving buffers
    buf01 = (unsigned char *)malloc( sizeof(unsigned char)*buflen );
    buf02 = (unsigned char *)malloc( sizeof(unsigned char)*buflen );
    for (i=0; i<buflen; i++)
    {
        buf01[i] = 0xFF;
        buf02[i] = 0xFF;
    }

    bufeth    = (unsigned char **)malloc( sizeof(unsigned char *) * bufethsize );
    bufeth_state = (int *)malloc( sizeof(int) * bufethsize);
    for (i=0; i<bufethsize; i++)
    {
        bufeth[i] = (unsigned char  *)malloc( sizeof(unsigned char) * bufethlen );
        bufeth_state[i] = 0;
        for(j=0; j<bufethlen; j++)
        {
            bufeth[i][j] = 0xFF;
        }
        //omp_init_lock(&(w_lock[i]));
        //omp_init_lock(&(r_lock[i]));
        //omp_set_lock(&(r_lock[i]));

    }
    omp_init_lock(&(w_lock[0]));
    omp_init_lock(&(r_lock[0]));
    omp_set_lock(&(r_lock[0]));
    omp_init_lock(&(w_lock[1]));
    omp_init_lock(&(r_lock[1]));
    omp_set_lock(&(r_lock[1]));


    nfeeds = config.nfeeds;
    nchans = 2 * nfeeds;
    nbls = nchans * (nchans + 1) / 2;
    nns = config.nns;
    accurate_inttime = (long)(1.0e9*config.inttime) / (2048*4) / (8*16*2*3) * (8*16*2*3) * (2048*4) * 1.0e-9; // integration time, Unit: second
    nweather = (int) (accurate_inttime * N_TIME_PER_FILE / config.weatherperiod);
    if (nbls != N_BASELINE)
    {
        printf("Error: Number of baselines %d unequal to N_BASELINE!!!\n", nbls);
        exit(1);
    }

    // get data dir
    getcwd(data_dir, sizeof(data_dir));
    strcat(data_dir, "/data");

    /* allocate and fill buffers */
    // feedno
    feedno = (int *)malloc( sizeof(int)*nfeeds );
    for (i=0; i<nfeeds; i++)
    {
        feedno[i] = i + 1;
    }

    // channo
    channo = (int *)malloc( sizeof(int)*nchans );
    for (i=0; i<nchans; i++)
    {
        channo[i] = i + 1;
    }

    // blorder
    blorder = (int *)malloc( sizeof(int)*2*nbls );
    strcpy(blorder_dir, data_dir);
    strcat(blorder_dir, "/blorder.dat");
    data_file = fopen(blorder_dir, "r");
    if (data_file == NULL)
    {
        printf("Error: Fail to open file %s\n", blorder_dir);
        exit (1);
    }
    for (i=0; i<2*nbls; i++)
    {
        fscanf(data_file, "%d", &blorder[i] );
    }
    fclose(data_file);

    // feedpos
    feedpos = (float *)malloc( sizeof(float)*3*nfeeds );
    strcpy(feedpos_dir, data_dir);
    strcat(feedpos_dir, "/feedpos.dat");
    data_file = fopen(feedpos_dir, "r");
    if (data_file == NULL)
    {
        printf("Error: Fail to open file %s\n", feedpos_dir);
        exit (1);
    }
    for (i=0; i<3*nfeeds; i++)
    {
        fscanf(data_file, "%f", &feedpos[i] );
    }
    fclose(data_file);

    // antpointing
    antpointing = (float *)malloc( sizeof(float)*4*nfeeds );
    for (i=0; i<nfeeds; i++)
    {
        antpointing[4*i] = 0.0;
        antpointing[4*i+1] = 90.0;
        antpointing[4*i+2] = 0.0; // should be correct AzErr
        antpointing[4*i+3] = 0.0; // should be correct AltErr
    }

    // pointingtime
    pointingtime = (float *)malloc( sizeof(float)*2 );
    for (i=0; i<2; i++)
    {
        pointingtime[0] = 0.0; // fill the correct value when saving the data set to file
        pointingtime[1] = -1.0; // negative means the end observation time
    }

    // polerr
    polerr = (float *)malloc( sizeof(float)*2*nfeeds );
    for (i=0; i<2*nfeeds; i++)
    {
        // should be the correct pol err
        polerr[i] = 0.0;
    }

    // nspos
    nspos = (float *)malloc( sizeof(float)*3*nns );
    strcpy(nspos_dir, data_dir);
    strcat(nspos_dir, "/nspos.dat");
    data_file = fopen(nspos_dir, "r");
    if (data_file == NULL)
    {
        printf("Error: Fail to open file %s\n", nspos_dir);
        exit (1);
    }
    for (i=0; i<3*nns; i++)
    {
        fscanf(data_file, "%f", &nspos[i] );
    }
    fclose(data_file);

    // noisesource
    noisesource = (float *)malloc( sizeof(float)*3*nns );
    float start, stop, cycle;
    char *p1=config.nsstart;
    char *p2=config.nsstop;
    char *p3=config.nscycle;
    for (i=0; i<nns; i++)
    {
        // better to have some error checking
        start = strtod(p1, &p1);
        noisesource[3*i] = start;

        stop = strtod(p2, &p2);
        noisesource[3*i+1] = stop;

        cycle = strtod(p3, &p3);
        noisesource[3*i+2] = cycle;
    }

    //weather
    weather = (float *)malloc( sizeof(float)*10*nweather );
    // initialize weather data to nan
    u_char *uc = (u_char *)weather;
    for (i=0; i<10*nweather*sizeof(float)/sizeof(u_char); i++)
    {
        uc[i] = 0xFF;
    }

}


void free_buf()
{
    free(buf01);
    free(buf02);
    free(bufeth);
    free(bufeth_state);
    free(feedno);
    free(channo);
    free(blorder);
    free(feedpos);
    free(antpointing);
    free(pointingtime);
    free(polerr);
    free(nspos);
    free(noisesource);
    free(weather);
}


void gen_obs_log()
{
    printf("%s", "Generate observation log function not implemented yet!!!\n");
}

void gen_datafile(const char *data_path)
{
    float span, start_offset, end_offset;
    float sec1970;
    char *obs_time;
    char *stime, *etime;
    char tmp_str[150];
    char file_name[35];
    char file_path[150];
    PyObject *pObj = NULL;
    hsize_t attr_dims[] = {};
    hid_t attr_space, attr_id;
    hid_t space, dset, dcpl; /* Handles */
    herr_t status;

    span = accurate_inttime * N_TIME_PER_FILE; // time span in one file, Unit: second
    start_offset = file_count * span; // offset from start time for this file, second
    end_offset = (file_count + 1) * span - accurate_inttime; // offset from start time for this file, second

    // wait until start_time has been set, that is we have began to receive data
    while (pkt_id == -1)
        ;

    // start and end time for this hdf5 file
    if (file_count == 0)
        PyRun_SimpleString("start_time = datetime.datetime.fromtimestamp(start_timestamp)");
    snprintf(tmp_str, sizeof(tmp_str), "stime = start_time + datetime.timedelta(seconds=%f)", start_offset);
    PyRun_SimpleString(tmp_str);
    snprintf(tmp_str, sizeof(tmp_str), "etime = start_time + datetime.timedelta(seconds=%f)", end_offset);
    PyRun_SimpleString(tmp_str);
    PyRun_SimpleString("obs_time = str(stime).replace('-', '/')"); // in format like 2016/05/14 17:04:53.014335
    // get value from python, better to have error checking
    pObj = PyMapping_GetItemString(pMainDict, "obs_time");
    obs_time = PyString_AsString(pObj);
    Py_DECREF(pObj);
    pObj = PyMapping_GetItemString(pMainDict, "start_timestamp");
    sec1970 = start_offset + (float) PyFloat_AsDouble(pObj); // Seconds since epoch 1970 Jan. 1st; Equals “obstime”
    Py_DECREF(pObj);
    PyRun_SimpleString("stime = '%04d%02d%02d%02d%02d%02d' % (stime.year, stime.month, stime.day, stime.hour, stime.minute, stime.second)");
    pObj = PyMapping_GetItemString(pMainDict, "stime");
    stime = PyString_AsString(pObj);
    Py_DECREF(pObj);
    PyRun_SimpleString("etime = '%04d%02d%02d%02d%02d%02d' % (etime.year, etime.month, etime.day, etime.hour, etime.minute, etime.second)");
    pObj = PyMapping_GetItemString(pMainDict, "etime");
    etime = PyString_AsString(pObj);
    Py_DECREF(pObj);

    // generate the observation log if required
    if (agmts.gen_obslog && file_count == 0)
    {
        gen_obs_log();
    }

    // data file name
    snprintf(file_name, sizeof(file_name), "%s_%s.hdf5", stime, etime);
    strcpy(file_path, data_path);
    strcat(file_path, "/");
    strcat(file_path, file_name);

    // Create a new hdf5 file using the default properties.
    file_id = H5Fcreate (file_path, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    // Create a 0 dimension space for scalar attributes
    attr_space = H5Screate_simple (0, attr_dims, NULL);

    // Create attributes
    // Type A: Common
    H5LTset_attribute_string(file_id, "/", "nickname", config.nickname);
    H5LTset_attribute_string(file_id, "/", "comment", config.comment);
    H5LTset_attribute_string(file_id, "/", "observer", config.observer);
    H5LTset_attribute_string(file_id, "/", "history", config.history);
    H5LTset_attribute_string(file_id, "/", "keywordver", config.keywordver);
    // Type B: Site
    H5LTset_attribute_string(file_id, "/", "sitename", config.sitename);
    attr_id = H5Acreate2(file_id, "sitelat", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &config.sitelat);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "sitelon", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &config.sitelon);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "siteelev", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &config.siteelev);
    status = H5Aclose (attr_id);
    H5LTset_attribute_string(file_id, "/", "timezone", config.timezone);
    H5LTset_attribute_string(file_id, "/", "epoch", config.epoch);
    // Type C: Antenna
    H5LTset_attribute_string(file_id, "/", "telescope", config.telescope);
    attr_id = H5Acreate2(file_id, "dishdiam", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &config.dishdiam);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "nants", H5T_STD_I32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_INT, &config.nants);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "npols", H5T_STD_I32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_INT, &config.npols);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "nfeeds", H5T_STD_I32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_INT, &config.nfeeds);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "cylen", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &config.cylen);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "cywid", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &config.cywid);
    status = H5Aclose (attr_id);
    // Type D: Receiver
    H5LTset_attribute_string(file_id, "/", "recvver", config.recvver);
    attr_id = H5Acreate2(file_id, "lofreq", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &config.lofreq);
    status = H5Aclose (attr_id);
    // Type E: Correlator
    H5LTset_attribute_string(file_id, "/", "corrver", config.corrver);
    attr_id = H5Acreate2(file_id, "samplingbits", H5T_STD_I32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_INT, &config.samplingbits);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "corrmode", H5T_STD_I32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_INT, &config.corrmode);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "inttime", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &accurate_inttime);
    status = H5Aclose (attr_id);
    H5LTset_attribute_string(file_id, "/", "obstime", obs_time);
    attr_id = H5Acreate2(file_id, "sec1970", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &sec1970);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "nfreq", H5T_STD_I32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_INT, &config.nfreq);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "freqstart", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &config.freqstart);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "freqstep", H5T_IEEE_F32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_FLOAT, &config.freqstep);
    status = H5Aclose (attr_id);

    // vis
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
    // Attributes for vis
    H5LTset_attribute_string(file_id, "vis", "dimname", "Time, Frequency, Baseline");

    // for other datasets
    // Create the dataset creation property list, set the layout to contiguous.
    dcpl = H5Pcreate (H5P_DATASET_CREATE);
    status = H5Pset_layout (dcpl, H5D_CONTIGUOUS);

    // feedno
    // Create dataspace
    hsize_t feedno_dims[1] = {nfeeds};
    space = H5Screate_simple (1, feedno_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "feedno", H5T_STD_I32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, feedno);
    status = H5Sclose (space);
    status = H5Dclose (dset);
    // Attributes for feedno

    // channo
    // Create dataspace
    hsize_t channo_dims[2] = {nfeeds, 2};
    space = H5Screate_simple (2, channo_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "channo", H5T_STD_I32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, channo);
    // Attributes for channo
    H5LTset_attribute_string(file_id, "channo", "dimname", "Feed No., (Channel No. of XPol, Channel No. of YPol)");
    status = H5Sclose (space);
    status = H5Dclose (dset);
    // attribute badchn, how to?

    // blorder
    // Create dataspace
    hsize_t blorder_dims[2] = {nbls, 2};
    space = H5Screate_simple (2, blorder_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "blorder", H5T_STD_I32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, blorder);
    // Attributes for blorder
    H5LTset_attribute_string(file_id, "blorder", "dimname", "Baselines, Baseline Name");
    status = H5Sclose (space);
    status = H5Dclose (dset);

    // feedpos
    // Create dataspace
    hsize_t feedpos_dims[2] = {nfeeds, 3};
    space = H5Screate_simple (2, feedpos_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "feedpos", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, feedpos);
    // Attributes for feedpos
    H5LTset_attribute_string(file_id, "feedpos", "dimname", "Feed No., (X, Y, Z) coordinate");
    H5LTset_attribute_string(file_id, "feedpos", "unit", "meter");
    status = H5Sclose (space);
    status = H5Dclose (dset);

    // antpointing
    // Create dataspace
    hsize_t antp_dims[3] = {1, nfeeds, 4}; // 1 for Source No.
    space = H5Screate_simple (3, antp_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "antpointing", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, antpointing);
    // Attributes for antpointing
    H5LTset_attribute_string(file_id, "antpointing", "dimname", "Source No., Feed No., (Az, Alt, AzErr, AltErr)");
    H5LTset_attribute_string(file_id, "antpointing", "unit", "degree");
    status = H5Sclose (space);
    status = H5Dclose (dset);

    // pointingtime
    // Create dataspace
    hsize_t pt_dims[3] = {1, 2}; // 1 for Source No.
    space = H5Screate_simple (2, pt_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "pointingtime", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    pointingtime[0] = sec1970; // fill the correct start time here before saving
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, pointingtime);
    // Attributes for antpointing
    H5LTset_attribute_string(file_id, "pointingtime", "dimname", "Source No., (starttime, endtime)");
    H5LTset_attribute_string(file_id, "pointingtime", "unit", "second");
    status = H5Sclose (space);
    status = H5Dclose (dset);

    // polerr
    // Create dataspace
    hsize_t polerr_dims[2] = {nfeeds, 2};
    space = H5Screate_simple (2, polerr_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "polerr", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, polerr);
    // Attributes for polerr
    H5LTset_attribute_string(file_id, "polerr", "dimname", "Feed No., (XPolErr, YPolErr)");
    H5LTset_attribute_string(file_id, "polerr", "unit", "degree");
    status = H5Sclose (space);
    status = H5Dclose (dset);

    // nspos
    // Create dataspace
    hsize_t nspos_dims[2] = {nns, 3};
    space = H5Screate_simple (2, nspos_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "nspos", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, nspos);
    // Attributes for feedpos
    H5LTset_attribute_string(file_id, "nspos", "dimname", "NoiseSource No., (X, Y, Z) coordinate");
    H5LTset_attribute_string(file_id, "nspos", "unit", "meter");
    status = H5Sclose (space);
    status = H5Dclose (dset);

    // noisesource
    // Create dataspace
    hsize_t ns_dims[2] = {nns, 3};
    space = H5Screate_simple (2, ns_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "noisesource", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, noisesource);
    // Attributes for noisesource
    H5LTset_attribute_string(file_id, "noisesource", "dimname", "NoiseSource No., (Start, Stop, Cycle)");
    H5LTset_attribute_string(file_id, "noisesource", "unit", "second");
    status = H5Sclose (space);
    status = H5Dclose (dset);

    // transitsource
    // Create dataspace
    hsize_t ts_dims[2] = {0, 5}; // no transit source for cylinder array
    space = H5Screate_simple (2, ts_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "transitsource", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, transitsource);
    // Attributes for feedpos
    H5LTset_attribute_string(file_id, "transitsource", "dimname", "Sources, (time, SourceRA, SourceDec, SourceAz, SourceAlt)");
    H5LTset_attribute_string(file_id, "transitsource", "unit", "(second, degree, degree, degree, degree)");
    H5LTset_attribute_string(file_id, "transitsource", "srcname", "None");
    status = H5Sclose (space);
    status = H5Dclose (dset);

    // weather
    // Create dataspace
    hsize_t weather_dims[2] = {nweather, 10};
    space = H5Screate_simple (2, weather_dims, NULL);
    // Create the dataset
    weather_dset = H5Dcreate (file_id, "weather", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    // NOTE: here we write the initial weather buffer to this data set, but the weather buffer haven't filled by the real weather data yet, we will fill it before the close of the hdf5 file
    status = H5Dwrite (weather_dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);
    // Attributes for weather
    H5LTset_attribute_string(file_id, "weather", "dimname", "Weather Data, (Sec1970, RoomTemperature, RoomHumidity, Temperature, Dewpoint, Humidity, Precipitation, WindDirection, WindSpeed, Pressure)");
    /* H5LTset_attribute_string(file_id, "weather", "unit", "second, Celcius, %, Celcius, Celcius, %, millimeter, degree (0 to 360; 0 for North, 90 for East), m/s, Pa; Note: WindSpeed is a 2-minute-average value."); */
    H5LTset_attribute_string(file_id, "weather", "unit", "(second, Celcius, %, Celcius, Celcius, %, millimeter, degree (0 to 360; 0 for North, 90 for East), m/s, Pa)");

    // Close and release resources.
    //status = H5Pclose (attr_space);
    status = H5Sclose (attr_space);
    //printf("attr_space close %d", status);
    //status = H5Aclose (attr_id);
    //printf("attr_id close %d", status);
    status = H5Pclose (dcpl);
    //printf("dcpl close %d", status);
    //status = H5Dclose (dset);
    //printf("dset close %d", status);
    status = H5Sclose (space);
    //printf("space %d", status);

    file_count++;

}


void writeData(const char *data_path)
{
    int i;
    complex_t *cbuf;
    herr_t     status;
    hid_t      sub_dataspace_id;

    // pre-generate data file for data storage
    gen_datafile(data_path);

    while (1)
    {
        if( buf01_state == 1 )
        {
            //printf("writing Buff01\n");
            offset[0] = buf_cnt*N_INTEGRA_TIME;
            // Create memory space with size of subset.
            sub_dataspace_id = H5Screate_simple (3, sub_dims, NULL);
            // Select subset from file dataspace.
            status = H5Sselect_hyperslab (dataspace_id, H5S_SELECT_SET, offset, stride, count, block);
            // Write a subset of data to the dataset.
            cbuf = (complex_t *)buf01;
            status = H5Dwrite (dataset_id, memtype, sub_dataspace_id, dataspace_id, H5P_DEFAULT, cbuf);

            // re-initialize the buffer
            for (i=0; i<buflen; i++)
                buf01[i] = 0xFF;
            buf01_state = 0;

            if (buf_cnt == N_BUFFER_PER_FILE - 1)
            {
                buf_cnt = 0;

                if (weather_exist) // while weather data file exists
                {
                    // Write weather data to the dataset before file close
                    status = H5Dwrite (weather_dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);

                    // alarm(0); // kill the timer
                    timer_cnt = 0; // not kill timer, but reset timer counter

                    // re-initialize weather buffer to nan
                    u_char *uc = (u_char *)weather;
                    for (i=0; i<9*nweather*sizeof(float)/sizeof(u_char); i++)
                    {
                        uc[i] = 0xFF;
                    }
                }

                // Close and release resources.
                status = H5Dclose (dataset_id);
                status = H5Dclose (weather_dset);
                status = H5Sclose (sub_dataspace_id);
                status = H5Sclose (dataspace_id);
                status = H5Tclose (memtype);
                status = H5Tclose (filetype);
                status = H5Fclose (file_id);

                gen_datafile(data_path);
            }
            else
            {
                buf_cnt++;
            }

        }
        else if( buf02_state == 1 )
        {
            //printf("writing Buff02\n");
            offset[0] = buf_cnt*N_INTEGRA_TIME;
            // Create memory space with size of subset.
            sub_dataspace_id = H5Screate_simple (3, sub_dims, NULL);
            // Select subset from file dataspace.
            status = H5Sselect_hyperslab (dataspace_id, H5S_SELECT_SET, offset, stride, count, block);
            // Write a subset of data to the dataset.
            cbuf = (complex_t *)buf02;
            status = H5Dwrite (dataset_id, memtype, sub_dataspace_id, dataspace_id, H5P_DEFAULT, cbuf);

            // re-initialize the buffer
            for (i=0; i<buflen; i++)
                buf02[i] = 0xFF;
            buf02_state = 0;

            if (buf_cnt == N_BUFFER_PER_FILE - 1)
            {
                buf_cnt = 0;

                if (weather_exist) // while weather data file exists
                {
                    // Write weather data to the dataset before file close
                    status = H5Dwrite (weather_dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);

                    // alarm(0); // kill the timer
                    timer_cnt = 0; // not kill timer, but reset timer counter

                    // re-initialize weather buffer to nan
                    u_char *uc = (u_char *)weather;
                    for (i=0; i<9*nweather*sizeof(float)/sizeof(u_char); i++)
                    {
                        uc[i] = 0xFF;
                    }
                }

                // Close and release resources.
                status = H5Dclose (dataset_id);
                status = H5Dclose (weather_dset);
                status = H5Sclose (sub_dataspace_id);
                status = H5Sclose (dataspace_id);
                status = H5Tclose (memtype);
                status = H5Tclose (filetype);
                status = H5Fclose (file_id);

                gen_datafile(data_path);
            }
            else
            {
                buf_cnt++;
            }

        }
        if (buf01_state == 0 && buf02_state == 0 && DataExist == 0)
        {
            // Write weather data to the dataset before file close
            status = H5Dwrite (weather_dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);

            // Close and release resources.
            status = H5Dclose (dataset_id);
            status = H5Dclose (weather_dset);
            status = H5Sclose (sub_dataspace_id);
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
    //char log_path[150];
    char tmp_str[150];
    register int packet_len ;
    //register int row = 0;
    register int init_cnt, bufeth_index=0;
    //register int pkt_id_old=-1, current_cnt, freq_ind;
    //register int init_cnt, current_cnt, freq_ind, pkt_id = -1, pkt_id_old=-1;
    //register int row_in_buf = N_FREQUENCY * N_INTEGRA_TIME;
    //register long row_size = 8 * N_BASELINE;
    // register int pkt_id_old = 0;
    //u_char frame_buff[BUFSIZE];
    //u_char * frame_buff_p = frame_buff;
    u_char * frame_buff_p = *bufeth;
    //u_char * start_buf_p;
    //u_char * start_frame_p;
    //int copy_len;
    int recv_fd;
    int old_cnt, i = 0;
    struct sockaddr_ll sll;
    struct ifreq ifr;
    //FILE *fp;

    // clock_t t1, t2;

    // open log file
    //strcpy(log_path, data_path);
    //strcat(log_path, "/recv_data.log");
    //fp = fopen(log_path, "wb");

    // check if weather data file exits
    getcwd(weather_file, sizeof(weather_file));
    strcat(weather_file, "/");
    strcat(weather_file, WEATHER_PATH);
    if( access(weather_file, F_OK) == -1 )
    {
        printf("Error: Weather data file %s does not exist, so weather data will not get\n", weather_file);
    }
    else
    {
        weather_exist = 1;

        // set timer period
        new_value.it_value.tv_sec = 0;
        new_value.it_value.tv_usec = 1; // first setup after 1 micro second after timer
        new_value.it_interval.tv_sec = config.weatherperiod; // then after this time period each time
        new_value.it_interval.tv_usec = 0;
    }

    // initialize network related things
    recv_fd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL));
    bzero(&sll, sizeof(sll));
    bzero(&ifr, sizeof(ifr));
    strncpy((char *)ifr.ifr_name, DEVICE_NAME, IFNAMSIZ);
    ioctl(recv_fd, SIOCGIFINDEX, &ifr);
    sll.sll_family   = AF_PACKET;
    sll.sll_protocol = htons(ETH_P_ALL);
    sll.sll_ifindex  = ifr.ifr_ifindex;
    bind(recv_fd, (struct sockaddr *) &sll, sizeof(sll));

    printf("Begin to receive data ... \n");
    fflush(stdout);

    while (Running) // Find packet zero.
    {
        //packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
        packet_len = recv(recv_fd, *bufeth, BUFSIZE, 0);
        if (*(int *)(frame_buff_p + 18) == 0) //find pkt 0.
        {
            if (i == 0)
            {
                old_cnt = *(int *)(frame_buff_p + 22);
                i=1;
            }
            else
            {
                init_cnt = *(int *)(frame_buff_p + 22);
                // find where time count changes as the starting point 
                // to receive data to buffer
                if (init_cnt != old_cnt)
                {
                    // use this time as the data receiving start time 
                    // (may need more accurate start time, but how to get?)
                    snprintf(tmp_str, sizeof(tmp_str), "start_timestamp = time.time() - %f", 0.5*accurate_inttime); // minus half integration time
                    PyRun_SimpleString(tmp_str);
                    // Seconds since epoch 1970 Jan. 1st
                    pkt_id = 0;
                    // now setup the timer and begin to get weather data
                    if (weather_exist) // while weather data file exists
                    {
                        setitimer(ITIMER_REAL, &new_value, &old_value);
                        // initialize timer count to 0
                        timer_cnt = 0;
                    }
                    break;
                }
            }
        }
    }

    omp_set_lock(&(w_lock[0]));
    bufeth_state[0]=packet_len;
    for (bufeth_index=1; bufeth_index<bufethsize/2; bufeth_index++)
    {
        //omp_set_lock(&(w_lock[bufeth_index]));
        bufeth_state[bufeth_index] = 
            recv(recv_fd, *(bufeth + bufeth_index), BUFSIZE, 0);
        //omp_unset_lock(&(r_lock[bufeth_index]));
    }
    omp_unset_lock(&(r_lock[0]));
    omp_set_lock(&(w_lock[1]));
    for (bufeth_index=bufethsize/2; bufeth_index<bufethsize; bufeth_index++)
    {
        //omp_set_lock(&(w_lock[bufeth_index]));
        bufeth_state[bufeth_index] = 
            recv(recv_fd, *(bufeth + bufeth_index), BUFSIZE, 0);
        //omp_unset_lock(&(r_lock[bufeth_index]));
    }
    omp_unset_lock(&(r_lock[1]));

    while(Running)
    {

        omp_set_lock(&(w_lock[0]));
        for (bufeth_index=0; bufeth_index<bufethsize/2; bufeth_index++)
        {
            //omp_set_lock(&(w_lock[bufeth_index]));
            bufeth_state[bufeth_index] = 
                recv(recv_fd, *(bufeth + bufeth_index), BUFSIZE, 0);
            //omp_unset_lock(&(r_lock[bufeth_index]));
        }
        omp_unset_lock(&(r_lock[0]));

        omp_set_lock(&(w_lock[1]));
        for (bufeth_index=bufethsize/2; bufeth_index<bufethsize; bufeth_index++)
        {
            //omp_set_lock(&(w_lock[bufeth_index]));
            bufeth_state[bufeth_index] = 
                recv(recv_fd, *(bufeth + bufeth_index), BUFSIZE, 0);
            //omp_unset_lock(&(r_lock[bufeth_index]));
        }
        printf("recv: pkt_id %d\t", *(int *)(*(bufeth + bufethsize/2 - 1) + 18));
        printf("pkt_id %d\n", *(int *)(*(bufeth + bufethsize - 1) + 18));
        omp_unset_lock(&(r_lock[1]));

    }
    bufeth_state[0] = -1; // set the 1st eth buff flag to -1, the check thread break
    //for (i=0; i<bufethsize; i++)
    //{
    //    omp_unset_lock(&(r_lock[i]));
    //    omp_destroy_lock(&(r_lock[i]));
    //    omp_destroy_lock(&(w_lock[i]));
    //}
    omp_unset_lock(&(r_lock[0]));
    omp_destroy_lock(&(r_lock[0]));
    omp_destroy_lock(&(w_lock[0]));
    omp_unset_lock(&(r_lock[1]));
    omp_destroy_lock(&(r_lock[1]));
    omp_destroy_lock(&(w_lock[1]));
    //fclose(fp);
}

void checkData(const char *data_path)
{
    //sleep(40);
    char log_path[150];
    register int packet_len, row = 0;
    register int init_cnt = -1, bufeth_index=0, pkt_id_old=-1;
    register int current_cnt, freq_ind;
    register int row_in_buf = N_FREQUENCY * N_INTEGRA_TIME;
    register long row_size = 8 * N_BASELINE;
    u_char * frame_buff_p;
    u_char * start_buf_p;
    u_char * start_frame_p;
    int buf01_state_local = 0 ;
    int buf02_state_local = 0 ;
    int copy_len;
    int lost_pkt_n = 0;
    FILE *fp;

    // open log file
    strcpy(log_path, data_path);
    strcat(log_path, "/recv_data.log");
    fp = fopen(log_path, "wb");

    while(bufeth_state[bufeth_index]==0)
    {}
    if (init_cnt == -1)
    {
        init_cnt = *(int *)(*(bufeth) + 22);
    }

    while(Running)
    {
        //while(bufeth_index<bufethsize)
        if(bufeth_state[0]==-1)
        {
            // the recv thread already break, break here.
            break;
        }

        omp_set_lock(&(r_lock[0]));
        printf("check: pkt_id %d\t", *(int *)(*(bufeth + bufethsize/2 - 1) + 18));
        printf("pkt_id %d\n\n", *(int *)(*(bufeth + bufethsize - 1) + 18));
        for (bufeth_index=0; bufeth_index<bufethsize/2; bufeth_index++)
        {
            //omp_set_lock(&(r_lock[bufeth_index]));
            //while(bufeth_state[bufeth_index]==0)
            //{}
            //printf("\nchecking %d package", bufeth_index);
            frame_buff_p = *(bufeth + bufeth_index);
            pkt_id = *(int *)(frame_buff_p  + 18);
            packet_len = bufeth_state[bufeth_index];
            //printf("checking %d pkt_id\t%d\n", bufeth_index, pkt_id);
            if (pkt_id == 0)
            {
                current_cnt = *(int *)(frame_buff_p + 22);
                freq_ind = *(int *)(frame_buff_p + 26) + FREQ_OFFSET;
                row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind;
                //printf("%d %d %d %d\n", row, row_in_buf, buf01_state, buf02_state);

                if (row >= row_in_buf)
                {
                    //printf("switch buf %d %d\n", buf01_state, buf02_state);
                    //fflush(stdout);
                    if (buf01_state_local == 0)
                    {
                        buf01_state_local = 1;
                        buf01_state       = 1;
                        buf02_state_local = buf02_state;
                    }
                    else if (buf02_state_local == 0)
                    {
                        buf02_state_local = 1;
                        buf02_state       = 1;
                        buf01_state_local = buf01_state;
                    }
                    init_cnt = current_cnt;
                    row = freq_ind;
                }

            }
            else if (pkt_id != pkt_id_old + 1) // have packet lost
            {
                // drop packets until find packet 0
                //printf("drop packets until find packet 0, %d\n", pkt_id);
                //fflush(stdout);
                pkt_id_old = 100;
                lost_pkt_n ++;
                bufeth_state[bufeth_index] = 0;
                omp_unset_lock(&(w_lock[bufeth_index]));
                continue;
            }

            //if (((pkt_id + MAX_PACKET_ID - pkt_id_old) % MAX_PACKET_ID) != 1)
            if (pkt_id_old == 100)
            {
                printf("Jump from %d to %d. %d packet lost\n", 
                        pkt_id_old, pkt_id, lost_pkt_n);
                fflush(stdout);
                lost_pkt_n = 0;
                //fprintf(fp, "Jump from %d to %d.\n", pkt_id_old, pkt_id);
            }
            pkt_id_old = pkt_id;

            if (buf01_state_local == 0)
            {
                if (pkt_id == 0)
                {
                    start_buf_p = buf01 + row*row_size;
                    start_frame_p = frame_buff_p + 30;
                    copy_len = packet_len - 30;
                }
                else if (pkt_id == 99)
                {
                    start_buf_p = buf01 + row*row_size 
                        + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22 - 80;
                }
                else
                {
                    start_buf_p = buf01 + row*row_size 
                        + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22;
                }

                memcpy(start_buf_p, start_frame_p, copy_len);

            }
            else if (buf02_state_local == 0)
            {
                if (pkt_id == 0)
                {
                    start_buf_p = buf02 + row*row_size;
                    start_frame_p = frame_buff_p + 30;
                    copy_len = packet_len - 30;
                }
                else if (pkt_id == 99)
                {
                    start_buf_p = buf02 + row*row_size 
                        + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22 - 80;
                }
                else
                {
                    start_buf_p = buf02 + row*row_size 
                        + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22;
                }

                memcpy(start_buf_p, start_frame_p, copy_len);

            }
            else
            {
                printf("Buf01 and Buf02 are both full.\n");
                fflush(stdout);
                Running = 0;
            }
            bufeth_state[bufeth_index] = 0;
            //bufeth_index++;
        }
        omp_unset_lock(&(w_lock[0]));

        omp_set_lock(&(r_lock[1]));
        for (bufeth_index=bufethsize/2; bufeth_index<bufethsize; bufeth_index++)
        {
            //omp_set_lock(&(r_lock[bufeth_index]));
            //while(bufeth_state[bufeth_index]==0)
            //{}
            //printf("\nchecking %d package", bufeth_index);
            frame_buff_p = *(bufeth + bufeth_index);
            pkt_id = *(int *)(frame_buff_p  + 18);
            packet_len = bufeth_state[bufeth_index];
            //printf("checking %d pkt_id\t%d\n", bufeth_index, pkt_id);
            if (pkt_id == 0)
            {
                current_cnt = *(int *)(frame_buff_p + 22);
                freq_ind = *(int *)(frame_buff_p + 26) + FREQ_OFFSET;
                row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind;
                //printf("%d %d %d %d\n", row, row_in_buf, buf01_state, buf02_state);

                if (row >= row_in_buf)
                {
                    //printf("switch buf %d %d\n", buf01_state, buf02_state);
                    //fflush(stdout);
                    if (buf01_state_local == 0)
                    {
                        buf01_state_local = 1;
                        buf01_state       = 1;
                        buf02_state_local = buf02_state;
                    }
                    else if (buf02_state_local == 0)
                    {
                        buf02_state_local = 1;
                        buf02_state       = 1;
                        buf01_state_local = buf01_state;
                    }
                    init_cnt = current_cnt;
                    row = freq_ind;
                }

            }
            else if (pkt_id != pkt_id_old + 1) // have packet lost
            {
                // drop packets until find packet 0
                //printf("drop packets until find packet 0, %d\n", pkt_id);
                //fflush(stdout);
                pkt_id_old = 100;
                lost_pkt_n ++;
                bufeth_state[bufeth_index] = 0;
                omp_unset_lock(&(w_lock[bufeth_index]));
                continue;
            }

            //if (((pkt_id + MAX_PACKET_ID - pkt_id_old) % MAX_PACKET_ID) != 1)
            if (pkt_id_old == 100)
            {
                printf("Jump from %d to %d. %d packet lost\n", 
                        pkt_id_old, pkt_id, lost_pkt_n);
                fflush(stdout);
                lost_pkt_n = 0;
                //fprintf(fp, "Jump from %d to %d.\n", pkt_id_old, pkt_id);
            }
            pkt_id_old = pkt_id;

            if (buf01_state_local == 0)
            {
                if (pkt_id == 0)
                {
                    start_buf_p = buf01 + row*row_size;
                    start_frame_p = frame_buff_p + 30;
                    copy_len = packet_len - 30;
                }
                else if (pkt_id == 99)
                {
                    start_buf_p = buf01 + row*row_size 
                        + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22 - 80;
                }
                else
                {
                    start_buf_p = buf01 + row*row_size 
                        + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22;
                }

                memcpy(start_buf_p, start_frame_p, copy_len);

            }
            else if (buf02_state_local == 0)
            {
                if (pkt_id == 0)
                {
                    start_buf_p = buf02 + row*row_size;
                    start_frame_p = frame_buff_p + 30;
                    copy_len = packet_len - 30;
                }
                else if (pkt_id == 99)
                {
                    start_buf_p = buf02 + row*row_size 
                        + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22 - 80;
                }
                else
                {
                    start_buf_p = buf02 + row*row_size 
                        + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE;
                    start_frame_p = frame_buff_p + 22;
                    copy_len = packet_len - 22;
                }

                memcpy(start_buf_p, start_frame_p, copy_len);

            }
            else
            {
                printf("Buf01 and Buf02 are both full.\n");
                fflush(stdout);
                Running = 0;
            }
            bufeth_state[bufeth_index] = 0;
            //bufeth_index++;
        }
        omp_unset_lock(&(w_lock[1]));
        //bufeth_index = 0;
    }
    sleep(0.2);
    DataExist = 0;

    fclose(fp);
}


/* void recv_checkData(const char *data_path) */
/* { */
/*     char log_path[150]; */
/*     register int packet_len; */
/*     register int row = 0; */
/*     register int init_cnt, current_cnt, freq_ind, pkt_id_old=-1; */
/*     register int row_in_buf = N_FREQUENCY * N_INTEGRA_TIME; */
/*     register long row_size = 8 * N_BASELINE; */
/*     u_char frame_buff[BUFSIZE]; */
/*     u_char * frame_buff_p = frame_buff; */
/*     u_char * start_buf_p; */
/*     u_char * start_frame_p; */
/*     int copy_len; */
/*     int recv_fd; */
/*     int old_cnt, i = 0; */
/*     struct sockaddr_ll sll; */
/*     struct ifreq ifr; */
/*     FILE *fp; */

/*     // open log file */
/*     strcpy(log_path, data_path); */
/*     strcat(log_path, "/recv_data.log"); */
/*     fp = fopen(log_path, "wb"); */

/*     // check if weather data file exits */
/*     getcwd(weather_file, sizeof(weather_file)); */
/*     strcat(weather_file, "/"); */
/*     strcat(weather_file, WEATHER_PATH); */
/*     if( access(weather_file, F_OK) == -1 ) */
/*     { */
/*         printf("Error: Weather data file %s does not exist, so weather data will not get\n", weather_file); */
/*     } */
/*     else */
/*     { */
/*         weather_exist = 1; */

/*         // set timer period */
/*         new_value.it_value.tv_sec = 0; */
/*         new_value.it_value.tv_usec = 1; // first setup after 1 micro second after timer */
/*         new_value.it_interval.tv_sec = config.weatherperiod; // then after this time period each time */
/*         new_value.it_interval.tv_usec = 0; */
/*     } */

/*     // initialize network related things */
/*     recv_fd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL)); */
/*     bzero(&sll, sizeof(sll)); */
/*     bzero(&ifr, sizeof(ifr)); */
/*     strncpy((char *)ifr.ifr_name, DEVICE_NAME, IFNAMSIZ); */
/*     ioctl(recv_fd, SIOCGIFINDEX, &ifr); */
/*     sll.sll_family   = AF_PACKET; */
/*     sll.sll_protocol = htons(ETH_P_ALL); */
/*     sll.sll_ifindex  = ifr.ifr_ifindex; */
/*     bind(recv_fd, (struct sockaddr *) &sll, sizeof(sll)); */

/*     printf("Begin to receive data ... \n"); */
/*     fflush(stdout); */

/*     while (Running) // Find packet zero. */
/*     { */
/*         packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0); */
/*         //        if (packet_len == MAX_RAWPACKET_SIZE || packet_len == MIN_RAWPACKET_SIZE) */
/*         if (*(int *)(frame_buff_p + 18) == 0) //find pkt 0. */
/*         { */
/*             if (i == 0) */
/*             { */
/*                 old_cnt = *(int *)(frame_buff_p + 22); */
/*                 i=1; */
/*             } */
/*             else */
/*             { */
/*                 init_cnt = *(int *)(frame_buff_p + 22); */
/*                 // find where time count changes as the starting point to receive data to buffer */
/*                 if (init_cnt != old_cnt) */
/*                 { */

/*                     // use this time as the data receiving start time (may need more accurate start time, but how to get?) */
/*                     PyRun_SimpleString("start_timestamp = time.time()"); // Seconds since epoch 1970 Jan. 1st */
/*                     pkt_id = 0; */

/*                     // now setup the timer and begin to get weather data */
/*                     if (weather_exist) // while weather data file exists */
/*                     { */
/*                         setitimer(ITIMER_REAL, &new_value, &old_value); */
/*                         // initialize timer count to 0 */
/*                         timer_cnt = 0; */
/*                     } */

/*                     break; */
/*                 } */
/*             } */
/*         } */
/*     } */

/*     while(Running) */
/*     { */
/*         row = *(int *)(frame_buff_p + 26) + FREQ_OFFSET; */
/*         if (buf01_state == 0) */
/*         { */
/*             while (row < row_in_buf) */
/*             { */
/*                 if (pkt_id == 0) */
/*                 { */
/*                     start_buf_p = buf01 + row*row_size; */
/*                     start_frame_p = frame_buff_p + 30; */
/*                     copy_len = packet_len - 30; */
/*                 } */
/*                 else if (pkt_id == 99) */
/*                 { */
/*                     start_buf_p = buf01 + row*row_size + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE; */
/*                     start_frame_p = frame_buff_p + 22; */
/*                     copy_len = packet_len - 22 - 80; */
/*                 } */
/*                 else */
/*                 { */
/*                     start_buf_p = buf01 + row*row_size + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE; */
/*                     start_frame_p = frame_buff_p + 22; */
/*                     copy_len = packet_len - 22; */
/*                 } */

/*                 memcpy(start_buf_p, start_frame_p, copy_len); */
/*                 //                while (1) */
/*                 //                { */
/*                 packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0); */
/*                 //                    if (packet_len == MAX_RAWPACKET_SIZE || packet_len == MIN_RAWPACKET_SIZE) */
/*                 //                    { */
/*                 //                      pkt_id = (int)frame_buff[18]; */
/*                 //                      if (pkt_id < pkt_id_old) row++; */
/*                 pkt_id = *(int *)(frame_buff_p + 18); */
/*                 if (pkt_id == 0) */
/*                 { */
/*                     current_cnt = *(int *)(frame_buff_p + 22); */
/*                     freq_ind = *(int *)(frame_buff_p + 26) + FREQ_OFFSET; */
/*                     row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind; */
/*                     // printf("%d ", row); */
/*                 } */
/*                 else if (pkt_id < pkt_id_old) // have packet lost */
/*                 { */
/*                     // drop packets until find packet 0 */
/*                     while(1) */
/*                     { */
/*                         packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0); */
/*                         pkt_id = *(int *)(frame_buff_p + 18); */
/*                         if (pkt_id == 0) */
/*                         { */
/*                             current_cnt = *(int *)(frame_buff_p + 22); */
/*                             freq_ind = *(int *)(frame_buff_p + 26) + FREQ_OFFSET; */
/*                             row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind; */
/*                             // printf("%d ", row); */
/*                             break; */
/*                         } */
/*                     } */
/*                 } */

/*                 if (((pkt_id + MAX_PACKET_ID - pkt_id_old) % MAX_PACKET_ID) != 1) */
/*                     fprintf(fp, "Jump from %d to %d.\n", pkt_id_old, pkt_id); */
/*                 pkt_id_old = pkt_id; */
/*                 //                        break; */
/*                 //                    } */
/*                 //                } */
/*             } */
/*             buf01_state = 1; */
/*             init_cnt = current_cnt; */
/*         } */
/*         else if (buf02_state == 0) */
/*         { */
/*             while (row < row_in_buf) */
/*             { */
/*                 if (pkt_id == 0) */
/*                 { */
/*                     start_buf_p = buf02 + row*row_size; */
/*                     start_frame_p = frame_buff_p + 30; */
/*                     copy_len = packet_len - 30; */
/*                 } */
/*                 else if (pkt_id == 99) */
/*                 { */
/*                     start_buf_p = buf02 + row*row_size + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE; */
/*                     start_frame_p = frame_buff_p + 22; */
/*                     copy_len = packet_len - 22 - 80; */
/*                 } */
/*                 else */
/*                 { */
/*                     start_buf_p = buf02 + row*row_size + FIRST_PACKET_SIZE + (pkt_id - 1)*MAX_PACKET_SIZE; */
/*                     start_frame_p = frame_buff_p + 22; */
/*                     copy_len = packet_len - 22; */
/*                 } */

/*                 memcpy(start_buf_p, start_frame_p, copy_len); */
/*                 //                while (1) */
/*                 //                { */
/*                 packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0); */
/*                 //                    if (packet_len == MAX_RAWPACKET_SIZE || packet_len == MIN_RAWPACKET_SIZE) */
/*                 //                    { */
/*                 //                      pkt_id = (int)frame_buff[18]; */
/*                 //                      if (pkt_id < pkt_id_old) row++; */
/*                 pkt_id = *(int *)(frame_buff_p + 18); */
/*                 if (pkt_id == 0) */
/*                 { */
/*                     current_cnt = *(int *)(frame_buff_p + 22); */
/*                     freq_ind = *(int *)(frame_buff_p + 26) + FREQ_OFFSET; */
/*                     row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind; */
/*                     // printf("%d ", row); */
/*                 } */
/*                 else if (pkt_id < pkt_id_old) // have packet lost */
/*                 { */
/*                     // drop packets until find packet 0 */
/*                     while(1) */
/*                     { */
/*                         packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0); */
/*                         pkt_id = *(int *)(frame_buff_p + 18); */
/*                         if (pkt_id == 0) */
/*                         { */
/*                             current_cnt = *(int *)(frame_buff_p + 22); */
/*                             freq_ind = *(int *)(frame_buff_p + 26) + FREQ_OFFSET; */
/*                             row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind; */
/*                             // printf("%d ", row); */
/*                             break; */
/*                         } */
/*                     } */
/*                 } */

/*                 if (((pkt_id + MAX_PACKET_ID - pkt_id_old) % MAX_PACKET_ID) != 1) */
/*                     fprintf(fp, "Jump from %d to %d.\n", pkt_id_old, pkt_id); */
/*                 pkt_id_old = pkt_id; */
/*                 //                        break; */
/*                 //                    } */
/*                 //                } */
/*             } */
/*             buf02_state = 1; */
/*             init_cnt = current_cnt; */
/*         } */
/*         else */
/*         { */
/*             printf("Buf01 and Buf02 are both full.\n"); */
/*             fflush(stdout); */
/*             Running = 0; */
/*         } */
/*     } */
/*     sleep(0.2); */
/*     DataExist = 0; */

/*     fclose(fp); */
/* } */


const char *argp_program_version =
  "0.0";
const char *argp_program_bug_address =
  "<sfzuo@bao.ac.cn>";

/* Program documentation. */
static char doc[] =
  "Program that receive and save data for the Tianlai cylinder array";

/* A description of the arguments we accept. */
static char args_doc[] = "DATA_PATH CONFIG_FILE";

/* Keys for options without short-options. */
#define GEN_OBSLOG  1            /* Generate observation log */

/* The options we understand. */
static struct argp_option options[] = {
  {"verbose",  'v', 0,      0,  "Produce verbose output" },
  {"gen_obslog", GEN_OBSLOG, 0, 0, "Generate observation log"},
  { 0 }
};

/* Parse a single option. */
static error_t parse_opt (int key, char *arg, struct argp_state *state)
{
  /* Get the input argument from argp_parse, which we
     know is a pointer to our arguments structure. */
  arguments *agmts = state->input;

  switch (key)
    {
    case 'v':
      agmts->verbose = 1;
      break;
    case GEN_OBSLOG:
      agmts->gen_obslog = 1;
      break;

    case ARGP_KEY_ARG:
      if (state->arg_num >= 2)
        /* Too many arguments. */
        argp_usage (state);

      agmts->args[state->arg_num] = arg;

      break;

    case ARGP_KEY_END:
      if (state->arg_num < 2)
        /* Not enough arguments. */
        argp_usage (state);
      break;

    default:
      return ARGP_ERR_UNKNOWN;
    }
  return 0;
}

/* Our argp parser. */
static struct argp argp = { options, parse_opt, args_doc, doc };


/* Parce ini handler function. */
static int handler(void* config, const char* section, const char* name,
                   const char* value)
{
    configuration* pconfig = (configuration*)config;

    #define MATCH(s, n) strcmp(section, s) == 0 && strcmp(name, n) == 0
    if (MATCH("change", "nickname")) {
        pconfig->nickname = strdup(value);
    } else if (MATCH("change", "comment")) {
        pconfig->comment = strdup(value);
    } else if (MATCH("change", "observer")) {
        pconfig->observer = strdup(value);
    } else if (MATCH("change", "nns")) {
        pconfig->nns = atoi(value);
    } else if (MATCH("change", "nsstart")) {
        pconfig->nsstart = strdup(value);
    } else if (MATCH("change", "nsstop")) {
        pconfig->nsstop = strdup(value);
    } else if (MATCH("change", "nscycle")) {
        pconfig->nscycle = strdup(value);
    } else if (MATCH("fix", "inttime")) {
        pconfig->inttime = atof(value);
    } else if (MATCH("fix", "weatherperiod")) {
        pconfig->weatherperiod = atof(value);
    } else if (MATCH("fix", "keywordver")) {
        pconfig->keywordver = strdup(value);
    } else if (MATCH("fix", "recvver")) {
        pconfig->recvver = strdup(value);
    } else if (MATCH("fix", "corrver")) {
        pconfig->corrver = strdup(value);
    } else if (MATCH("fix", "telescope")) {
        pconfig->telescope = strdup(value);
    } else if (MATCH("fix", "history")) {
        pconfig->history = strdup(value);
    } else if (MATCH("fix", "sitename")) {
        pconfig->sitename = strdup(value);
    } else if (MATCH("fix", "sitelat")) {
        pconfig->sitelat = atof(value);
    } else if (MATCH("fix", "sitelon")) {
        pconfig->sitelon = atof(value);
    } else if (MATCH("fix", "siteelev")) {
        pconfig->siteelev = atof(value);
    } else if (MATCH("fix", "timezone")) {
        pconfig->timezone = strdup(value);
    } else if (MATCH("fix", "epoch")) {
        pconfig->epoch = strdup(value);
    } else if (MATCH("fix", "dishdiam")) {
        pconfig->dishdiam = atof(value);
    } else if (MATCH("fix", "nants")) {
        pconfig->nants = atoi(value);
    } else if (MATCH("fix", "nfeeds")) {
        pconfig->nfeeds = atoi(value);
    } else if (MATCH("fix", "npols")) {
        pconfig->npols = atoi(value);
    } else if (MATCH("fix", "cylen")) {
        pconfig->cylen = atof(value);
    } else if (MATCH("fix", "cywid")) {
        pconfig->cywid = atof(value);
    } else if (MATCH("fix", "lofreq")) {
        pconfig->lofreq = atof(value);
    } else if (MATCH("fix", "samplingbits")) {
        pconfig->samplingbits = atoi(value);
    } else if (MATCH("fix", "corrmode")) {
        pconfig->corrmode = atoi(value);
    } else if (MATCH("fix", "nfreq")) {
        pconfig->nfreq = atoi(value);
    } else if (MATCH("fix", "freqstart")) {
        pconfig->freqstart = atof(value);
    } else if (MATCH("fix", "freqstep")) {
        pconfig->freqstep = atof(value);
    } else {
        return 0;  /* unknown section/name, error */
    }
    return 1;
}


int main(int argc, char* argv[])
{
    PyObject *pMain = NULL;

    Py_Initialize(); //initialize python
    // PySys_SetArgv(argc, argv);
    pMain = PyImport_AddModule("__main__");
    if (pMain == NULL)
    {
        printf("%s\n", "Error: Can not add module __main__");
        exit(1);
    }
    pMainDict = PyModule_GetDict(pMain);
    /* Py_DECREF(pMain); */ // can not have this, otherwise ImportError: __import__ not found, also core dumped
    if (pMainDict == NULL)
    {
        printf("%s\n", "Error: Can not get the dict of module __main__");
        exit(1);
    }

    PyRun_SimpleString("import time");
    PyRun_SimpleString("import datetime");

    int thread_id;
    const char *config_file;
    const char *data_path;

    /* Signal handle. */
    signal(SIGUSR1, kill_handler);
    signal(SIGALRM, timer_handler);


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


    /* Default values. */
    agmts.verbose = 0;
    agmts.gen_obslog = 0;

    /* Parse our arguments; every option seen by parse_opt will be reflected in arguments. */
    argp_parse(&argp, argc, argv, 0, 0, &agmts);

    /* parse the configuration file */
    config_file = agmts.args[1];
    if (ini_parse(config_file, handler, &config) < 0) {
        printf("Error: Can't load configuration file '%s'\n", config_file);
        exit(1);
    }
    if (agmts.verbose) {
        printf("Configure parameters loaded from '%s'\n", config_file);
    }

    /* create data path if it does not exist */
    data_path = agmts.args[0];
    create_data_path(data_path);

    /* allocate and initialize buffer */
    init_buf();

    #pragma omp parallel num_threads(3) private(thread_id)
    {
        thread_id = omp_get_thread_num();
        if(thread_id == 0)
            //recvData(data_path);
            recvData();
        else if(thread_id == 1)
            checkData(data_path);
        else if(thread_id == 2)
            writeData(data_path);
    }

    // release resources
    free_buf();

    printf("Over.\n");
    fflush(stdout);

    Py_DECREF(pMainDict);
    Py_Finalize();
    return 0;
}

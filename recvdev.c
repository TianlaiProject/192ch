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

#define DEVICE_NAME "enp131s0d1"  // the name of the network device

#define BUFSIZE            2000
#define MAX_PACKET_ID      100
#define MAX_PACKET_SIZE    1488
#define FIRST_PACKET_SIZE  1488 - 8
#define MIN_PACKET_SIZE    1000
#define MAX_RAWPACKET_SIZE 1510
#define MIN_RAWPACKET_SIZE 1022

#define N_BASELINE ((MAX_PACKET_ID - 1)*MAX_PACKET_SIZE + MIN_PACKET_SIZE - 88) / 8 // 4 bytes real and 4 bytes imag
#define N_FREQUENCY 1008 //(1008 - 42)
#define N_INTEGRA_TIME 10       // N_INTEGRA_TIME integration times in one buf
#define buflen 8 * N_BASELINE * N_FREQUENCY * N_INTEGRA_TIME // Bytes
#define N_BUFFER_PER_FILE 45   // 30 min data per file
#define N_TIME_PER_FILE N_INTEGRA_TIME * N_BUFFER_PER_FILE

// weater station related
#define WS_REMOTE_IP       "192.168.1.109"
#define WS_REMOTE_PORT     5555
#define WS_HEADER          "#"
#define WS_DEVNO           "9FCF"
#define WS_CMDGETDATA      "030C"
#define WS_CMDSETTIME      "010A"
#define WS_GETTAILER       "GG"
#define WS_SETTAILER       "G"
#define WS_GETDATA_CMD     WS_HEADER WS_DEVNO WS_CMDGETDATA WS_SETTAILER

#define SECOND_PER_DAY     24 * 3600


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
    // const char* nscycle;
    // const char* nsduration;
    char* nscycle;
    char* nsduration;
    double inttime;
    double weatherperiod;
    const char* keywordver;
    const char* recvver;
    const char* corrver;
    const char* telescope;
    const char* history;
    const char* sitename;
    double sitelat;
    double sitelon;
    double siteelev;
    const char* timezone;
    const char* epoch;
    double dishdiam;
    int nants;
    int nfeeds;
    int npols;
    double cylen;
    double cywid;
    double lofreq;
    int samplingbits;
    int corrmode;
    int nfreq;
    double freqstart;
    double freqstep;
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

struct itimerval new_value, old_value; // for timer use
int timer_cnt = 0;
int sockfd = -1;
int conn_status = -1;
int nfeeds, nchans, nbls, nns, nweather;
int *feedno, *channo, *blorder;
float *feedpos, *antpointing, *polerr, *noisesource, *weather;
const char *transitsource[] = {"", ""}; // no transit source for cylinder array

unsigned char * buf01;
unsigned char * buf02;
int buf01_state = 0 ;
int buf02_state = 0 ;

PyObject *pMain = NULL;
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
    int len;
    char buf[BUFSIZ];
    char header[2];
    char tailer[3];
    char substr[strlen(buf)];

    if (sig_no == SIGALRM && timer_cnt < nweather)
    {
        // better to request several times when failure occurs
        len = send(sockfd, WS_GETDATA_CMD, strlen(WS_GETDATA_CMD), 0);
        len = recv(sockfd, buf, BUFSIZ, 0); // receive weather data
        if (len == 117)
        {
            buf[len] = '\0';

            strncpy(header, buf, 1);
            header[1] = '\0';
            strncpy(tailer, buf+len-2, 2);
            tailer[2] = '\0';
            if (strcmp(header, WS_HEADER) || strcmp(tailer, WS_GETTAILER))
            {
                printf("Error: Get invalid weather data!!!\n");
            }
            else
            {
                strncpy(substr, buf+1, 4);
                substr[4] = '\0';
                // to upper case
                for (i = 0; substr[i]; i++)
                {
                    substr[i] = toupper(substr[i]);
                }
                if (strcmp(substr, WS_DEVNO))
                {
                    printf("Error: Weather data does not get from the required device!!!\n");
                }
                else
                {
                    strncpy(substr, buf+5, 4);
                    substr[4] = '\0';
                    if (strcmp(substr, WS_CMDGETDATA))
                    {
                        printf("Error: Something wrong happend for the weather data!!!\n");
                    }
                    else
                    {
                        /*
                          Information is:
                          5116160414 Time:MMHHddmmyy
                          00000000   Total radiation 1
                          00000000   Total radiation 2
                          80008000800080008000 Temperature 1,2,3,4,5
                          0106       Temperature
                          022d       Humidity
                          0681       Dew point
                          24f5       Pressure
                          024b       Height
                          0000       Current wind speed
                          0000       Wind speed in 2 minute average
                          0000       Wind speed in 10minute average
                          0008       Wind direction
                          0000       Current radiation 1
                          0000       Current radiation 2
                          0000       Precipitation
                          000000     Evaporation
                          0086       Power capacity
                          00         Sunshine hours
                        */
                        // get time offset
                        char min[3], hour[3], day[3], mon[3], year[3];
                        char dev_time[25];
                        char tmp_str[50];
                        int number;
                        strncpy(min, buf+9, 2);
                        min[2] = '\0';
                        strncpy(hour, buf+11, 2);
                        hour[2] = '\0';
                        strncpy(day, buf+13, 2);
                        day[2] = '\0';
                        strncpy(mon, buf+15, 2);
                        mon[2] = '\0';
                        strncpy(year, buf+17, 2);
                        year[2] = '\0';
                        snprintf(dev_time, sizeof(dev_time), "20%s/%s/%s %s:%s:00", year, mon, day, hour, min);
                        snprintf(tmp_str, sizeof(tmp_str), "offset = (ephem.Date(%s) - stime) * %d", dev_time, SECOND_PER_DAY);
                        PyRun_SimpleString(tmp_str);
                        weather[9*timer_cnt] = PyFloat_AsDouble(PyMapping_GetItemString(pMainDict, "offset")); // time offset in second

                        // temperature
                        strncpy(substr, buf+55, 4);
                        substr[4] = '\0';
                        number = (int)strtol(substr, NULL, 16);
                        number = ((number > 32767) ? (0x8000 - number) : number);
                        weather[9*timer_cnt+1] = number / 10.0;

                        // humidity
                        strncpy(substr, buf+59, 4);
                        substr[4] = '\0';
                        number = (int)strtol(substr, NULL, 16);
                        weather[9*timer_cnt+2] = number / 10.0;

                        // dew_point
                        strncpy(substr, buf+63, 4);
                        substr[4] = '\0';
                        number = (int)strtol(substr, NULL, 16);
                        number = ((number > 32767) ? (0x8000 - number) : number);
                        weather[9*timer_cnt+3] = number / 100.0;

                        // windspeed_current
                        strncpy(substr, buf+75, 4);
                        substr[4] = '\0';
                        number = (int)strtol(substr, NULL, 16);
                        weather[9*timer_cnt+4] = number / 10.0;

                        // windspeed_2minaverage
                        strncpy(substr, buf+79, 4);
                        substr[4] = '\0';
                        number = (int)strtol(substr, NULL, 16);
                        weather[9*timer_cnt+5] = number / 10.0;

                        // windspeed_10minaverage
                        strncpy(substr, buf+83, 4);
                        substr[4] = '\0';
                        number = (int)strtol(substr, NULL, 16);
                        weather[9*timer_cnt+6] = number / 10.0;

                        // windspeed_direction
                        strncpy(substr, buf+87, 4);
                        substr[4] = '\0';
                        number = (int)strtol(substr, NULL, 16);
                        weather[9*timer_cnt+7] = number;

                        // precipitation
                        strncpy(substr, buf+99, 4);
                        substr[4] = '\0';
                        number = (int)strtol(substr, NULL, 16);
                        weather[9*timer_cnt+8] = number / 10.0;

                    }
                }
            }
        }
        else
        {
            printf("Error: Get wrong weather data!!!\n");
        }
    }

    timer_cnt++;
}


void create_data_path(const char *data_path)
{
    if( access(data_path, F_OK) == -1 )
    {
        if (agmts.verbose)
            printf("Data path %s does not exists, create it...", data_path);
        if( mkdir(data_path, 0755) == -1 )
        {
            printf("Error: Failed to create the data path %s!!!\n", data_path);
            exit(-1);
        }
    }
    else if( access(data_path, W_OK) == -1 )
    {
        printf("Error: Data path %s does not writable!!!\n", data_path);
        exit(-1);
    }
}


void init_buf()
{
    int i;
    char data_dir[1024], feedpos_dir[1024], blorder_dir[1024];
    FILE *data_file;

    // allocate and initialize data receiving buffers
    buf01=(unsigned char *)malloc( sizeof(unsigned char)*buflen );
    buf02=(unsigned char *)malloc( sizeof(unsigned char)*buflen );
    for (i=0; i<buflen; i++)
    {
        buf01[i] = 0xFF;
        buf02[i] = 0xFF;
    }

    nfeeds = config.nfeeds;
    nchans = 2 * nfeeds;
    nbls = nchans * (nchans + 1) / 2;
    nns = config.nns;
    nweather = (int) (config.inttime * N_TIME_PER_FILE / config.weatherperiod);
    if (nbls != N_BASELINE)
    {
        printf("Error: Number of baselines %d unequal to N_BASELINE!!!\n", nbls);
        exit(-1);
    }

    // get data dir
    getcwd(data_dir, sizeof(data_dir));
    strcat(data_dir, "/data");

    /* allocate and fill weather related buffers */
    feedno=(int *)malloc( sizeof(int)*nfeeds );
    for (i=0; i<nfeeds; i++)
    {
        feedno[i] = i + 1;
    }
    channo=(int *)malloc( sizeof(int)*nchans );
    for (i=0; i<nchans; i++)
    {
        channo[i] = i + 1;
    }
    blorder=(int *)malloc( sizeof(int)*2*nbls );
    strcpy(blorder_dir, data_dir);
    strcat(blorder_dir, "/blorder.dat");
    data_file = fopen(blorder_dir, "r");
    if (data_file == NULL)
    {
        printf("Error: Fail to open file %s\n", blorder_dir);
        exit (-1);
    }
    for (i=0; i<2*nbls; i++)
    {
        fscanf(data_file, "%d", &blorder[i] );
    }
    fclose(data_file);
    feedpos=(float *)malloc( sizeof(float)*3*nfeeds );
    strcpy(feedpos_dir, data_dir);
    strcat(feedpos_dir, "/feedpos.dat");
    data_file = fopen(feedpos_dir, "r");
    if (data_file == NULL)
    {
        printf("Error: Fail to open file %s\n", feedpos_dir);
        exit (-1);
    }
    for (i=0; i<3*nfeeds; i++)
    {
        fscanf(data_file, "%f", &feedpos[i] );
    }
    fclose(data_file);
    antpointing=(float *)malloc( sizeof(float)*4*nfeeds );
    for (i=0; i<nfeeds; i++)
    {
        antpointing[4*i] = 0.0;
        antpointing[4*i+1] = 90.0;
        antpointing[4*i+2] = 0.0; // should be correct AzErr
        antpointing[4*i+3] = 0.0; // should be correct AltErr
    }
    polerr=(float *)malloc( sizeof(float)*2*nfeeds );
    for (i=0; i<2*nfeeds; i++)
    {
        // should be the correct pol err
        polerr[i] = 0.0;
    }
    noisesource=(float *)malloc( sizeof(float)*2*nns );
    float cycle, duration;
    char *p1=config.nscycle;
    char *p2=config.nsduration;
    for (i=0; i<nns; i++)
    {
        // better to have some error checking
        cycle = strtod(p1, &p1);
        noisesource[2*i] = cycle;

        duration = strtod(p2, &p2);
        noisesource[2*i+1] = duration;
    }
    weather=(float *)malloc( sizeof(float)*9*nweather );
    // initialize weather data to nan
    u_char *uc = (u_char *)weather;
    for (i=0; i<9*nweather*sizeof(float)/sizeof(u_char); i++)
    {
        uc[i] = 0xFF;
    }

}


void free_buf()
{
    free(buf01);
    free(buf02);
    free(feedno);
    free(channo);
    free(blorder);
    free(feedpos);
    free(polerr);
    free(noisesource);
    free(weather);
}


void gen_obs_log()
{
}

void gen_datafile(const char *data_path)
{
    double int_time, span, start_offset, end_offset;
    time_t t;
    struct tm tm;
    char cur_time[20];
    char settime_cmd[50];
    char *time_fmt;
    int len;
    char buf[BUFSIZ];  // buffer for receiving
    char *obs_time;
    char *stime, *etime;
    char tmp_str[150];
    char file_name[35];
    char file_path[150];
    PyObject *pObj = NULL;
    hid_t space, dset, dcpl, dsettype, mtype;    /* Handles */
    herr_t status;

    int_time = config.inttime; // integration time, Unit: second
    span = int_time * N_TIME_PER_FILE; // time span in one file, Unit: second
    start_offset = file_count * span; // offset from start time for this file, second
    end_offset = (file_count + 1) * span - int_time; // offset from start time for this file, second

    // calibrate the device time of the weather station
    if (conn_status == 0) // while successfully connected to the weather station
    {
        t = time(NULL);
        tm = *localtime(&t); // get current local time
        time_fmt = "%02d%02d%02d%02d%02d%02d%02d";
        snprintf(cur_time, sizeof(cur_time), time_fmt, tm.tm_sec, tm.tm_min, tm.tm_hour, tm.tm_mday, tm.tm_mon, tm.tm_wday, (tm.tm_year + 1900) % 2000);
        snprintf(settime_cmd, sizeof(settime_cmd), "%s%s%s%s%s", WS_HEADER, WS_DEVNO, WS_CMDSETTIME, cur_time, WS_SETTAILER);
        len = send(sockfd, settime_cmd, strlen(settime_cmd), 0);
        len = recv(sockfd, buf, BUFSIZ, 0); // receive data from weather station
        if (len == 11)
        {
            buf[len] = '\0';
        }
        else
        {
            printf("Error: Failed to set time!!!\n");
        }
    }

    // if (file_count == 0)
    //     PyRun_SimpleString("start_time = ephem.Date(ephem.localtime(ephem.now()))");
    // wait until start_time has been set, that is we have began to receive data
    while (pObj == NULL)
    {
        pObj = PyMapping_GetItemString(pMainDict, "start_time");
    }
    // now setup the timer and begin to get weather data
    if (conn_status == 0) // while successfully connected to the weather station
    {
        setitimer(ITIMER_REAL, &new_value, &old_value);
        // initalize timer count to 0
        timer_cnt = 0;
    }
    // start and end time for this hdf5 file
    snprintf(tmp_str, sizeof(tmp_str), "stime = ephem.Date(start_time + %f * ephem.second)", start_offset);
    PyRun_SimpleString(tmp_str);
    snprintf(tmp_str, sizeof(tmp_str), "etime = ephem.Date(start_time + %f * ephem.second)", end_offset);
    PyRun_SimpleString(tmp_str);
    PyRun_SimpleString("obs_time = str(stime)");
    // get value from python, better to have error checking
    obs_time = PyString_AsString(PyMapping_GetItemString(pMainDict, "obs_time"));
    PyRun_SimpleString("stime = '%04d%02d%02d%02d%02d%02d' % stime.tuple()");
    stime = PyString_AsString(PyMapping_GetItemString(pMainDict, "stime"));
    PyRun_SimpleString("etime = '%04d%02d%02d%02d%02d%02d' % etime.tuple()");
    etime = PyString_AsString(PyMapping_GetItemString(pMainDict, "etime"));

    // generate the observation log if required
    if (agmts.gen_obslog && file_count == 0)
    {
        gen_obs_log();
    }

    // data file name
    snprintf(file_name, sizeof(file_name), "%s_%s.hdf5", stime, etime);
    strcpy(file_path, data_path);
    strcat(file_path, file_name);

    // Create a new hdf5 file using the default properties.
    file_id = H5Fcreate (file_path, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    // Create attributes
    // Type A: Common
    H5LTset_attribute_string(file_id, "/", "nickname", config.nickname);
    H5LTset_attribute_string(file_id, "/", "comment", config.comment);
    H5LTset_attribute_string(file_id, "/", "observer", config.observer);
    H5LTset_attribute_string(file_id, "/", "history", config.history);
    H5LTset_attribute_string(file_id, "/", "keywordver", config.keywordver);
    // Type B: Site
    H5LTset_attribute_string(file_id, "/", "sitename", config.sitename);
    H5LTset_attribute_double(file_id, "/", "sitelat", &config.sitelat, 1);
    H5LTset_attribute_double(file_id, "/", "sitelon", &config.sitelon, 1);
    H5LTset_attribute_double(file_id, "/", "siteelev", &config.siteelev, 1);
    H5LTset_attribute_string(file_id, "/", "timezone", config.timezone);
    H5LTset_attribute_string(file_id, "/", "epoch", config.epoch);
    // Type C: Antenna
    H5LTset_attribute_string(file_id, "/", "telescope", config.telescope);
    H5LTset_attribute_double(file_id, "/", "dishdiam", &config.dishdiam, 1);
    H5LTset_attribute_int(file_id, "/", "nants", &config.nants, 1);
    H5LTset_attribute_int(file_id, "/", "npols", &config.npols, 1);
    H5LTset_attribute_double(file_id, "/", "cylen", &config.cylen, 1);
    H5LTset_attribute_double(file_id, "/", "cywid", &config.cywid, 1);
    // Type D: Receiver
    H5LTset_attribute_string(file_id, "/", "recvver", config.recvver);
    H5LTset_attribute_double(file_id, "/", "lofreq", &config.lofreq, 1);
    // Type E: Correlator
    H5LTset_attribute_string(file_id, "/", "corrver", config.corrver);
    H5LTset_attribute_int(file_id, "/", "samplingbits", &config.samplingbits, 1);
    H5LTset_attribute_int(file_id, "/", "corrmode", &config.corrmode, 1);
    H5LTset_attribute_double(file_id, "/", "inttime", &config.inttime, 1);
    H5LTset_attribute_string(file_id, "/", "obstime", obs_time);
    H5LTset_attribute_int(file_id, "/", "nfreq", &config.nfreq, 1);
    H5LTset_attribute_int(file_id, "/", "nfreq", &config.nfreq, 1);
    H5LTset_attribute_double(file_id, "/", "freqstep", &config.freqstep, 1);

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
    // Create the dataset creation property list, set the layout to compact.
    dcpl = H5Pcreate (H5P_DATASET_CREATE);
    status = H5Pset_layout (dcpl, H5D_COMPACT);

    // feedno
    // Create dataspace
    hsize_t feedno_dims[1] = {nfeeds};
    space = H5Screate_simple (1, feedno_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "feedno", H5T_STD_I32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, feedno);
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
    H5LTset_attribute_string(file_id, "channo", "dimname", "Feed No., (XPolarization YPolarization)");

    // blorder
    // Create dataspace
    hsize_t blorder_dims[2] = {nbls, 2};
    space = H5Screate_simple (2, blorder_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "blorder", H5T_STD_I32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, blorder);
    // Attributes for blorder
    H5LTset_attribute_string(file_id, "blorder", "dimname", "Baselines, BaselineName");

    // feedpos
    // Create dataspace
    hsize_t feedpos_dims[2] = {nfeeds, 3};
    space = H5Screate_simple (2, feedpos_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "feedpos", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, feedpos);
    // Attributes for feedpos
    H5LTset_attribute_string(file_id, "feedpos", "dimname", "Feed No., (X,Y,Z) coordinate");
    H5LTset_attribute_string(file_id, "feedpos", "unit", "meter");

    // antpointing
    // Create dataspace
    hsize_t antp_dims[2] = {nfeeds, 4};
    space = H5Screate_simple (2, antp_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "antpointing", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, antpointing);
    // Attributes for antpointing
    H5LTset_attribute_string(file_id, "antpointing", "dimname", "Feed No., (Az,Alt,AzErr,AltErr)");
    H5LTset_attribute_string(file_id, "antpointing", "unit", "degree");

    // polerr
    // Create dataspace
    hsize_t polerr_dims[2] = {nfeeds, 2};
    space = H5Screate_simple (2, polerr_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "polerr", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, polerr);
    // Attributes for polerr
    H5LTset_attribute_string(file_id, "polerr", "dimname", "Feed No., (XPolErr,YPolErr)");
    H5LTset_attribute_string(file_id, "polerr", "unit", "degree");

    // noisesource
    // Create dataspace
    hsize_t ns_dims[2] = {nns, 2};
    space = H5Screate_simple (2, ns_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "noisesource", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, noisesource);
    // Attributes for noisesource
    H5LTset_attribute_string(file_id, "noisesource", "dimname", "Source No., (Cycle Duration)");
    H5LTset_attribute_string(file_id, "noisesource", "unit", "second");

    // transitsource
    // Create file and memory datatypes, save the strings as FORTRAN strings.
    dsettype = H5Tcopy (H5T_FORTRAN_S1);
    status = H5Tset_size (dsettype, H5T_VARIABLE);
    mtype = H5Tcopy (H5T_C_S1);
    status = H5Tset_size (mtype, H5T_VARIABLE);
    // Create dataspace
    hsize_t ts_dims[2] = {1, 2};
    space = H5Screate_simple (2, ts_dims, NULL);
    // Create the dataset
    dset = H5Dcreate (file_id, "transitsource", dsettype, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, mtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, transitsource);
    // Attributes for transitsource
    H5LTset_attribute_string(file_id, "transitsource", "dimname", "Source, (DateTime, SourceName)");

    // weather
    // Create dataspace
    hsize_t weather_dims[2] = {nweather, 9};
    space = H5Screate_simple (2, weather_dims, NULL);
    // Create the dataset
    weather_dset = H5Dcreate (file_id, "weather", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    // NOTE: here we write the initial weather buffer to this data set, but the weather buffer haven't filled by the real weather data yet, we will fill it before the close of the hdf5 file
    status = H5Dwrite (weather_dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);
    // Attributes for weather
    H5LTset_attribute_string(file_id, "weather", "dimname", "Weather Data,(TimeOffset, RoomTemperature, SiteTemperature, Dewpoint, Humidity, Precipitation, WindDirection, WindSpeed, Pressure)");
    H5LTset_attribute_string(file_id, "weather", "unit", "second, Celcius, Celcius, Celcius, %, millimeter, degree, m/s, mmHg");

    // Close and release resources.
    status = H5Pclose (dcpl);
    status = H5Dclose (dset);
    status = H5Sclose (space);
    status = H5Tclose (dsettype);
    status = H5Tclose (mtype);

    file_count++;

}


void writeData(const char *data_path)
{
    int i;
    complex_t *cbuf;
    herr_t     status;
    hid_t      sub_dataspace_id;

    // set timer period
    new_value.it_value.tv_sec = 0;
    new_value.it_value.tv_usec = 1; // first setup after 1 micro second after timer
    new_value.it_interval.tv_sec = config.weatherperiod; // then after this time period each time
    new_value.it_interval.tv_usec = 0;

    // create socket and bind it to the weather station
    struct sockaddr_in remote_addr; // IP address
    memset(&remote_addr, 0, sizeof(remote_addr)); // initialize to 0
    remote_addr.sin_family = AF_INET; // set to communicate via IP
    remote_addr.sin_addr.s_addr = inet_addr(WS_REMOTE_IP); // set IP address
    remote_addr.sin_port = htons(WS_REMOTE_PORT); // IP port
    // create socket use IPv4 TCP protocol
    if((sockfd = socket(PF_INET , SOCK_STREAM , 0)) < 0)
    {
        perror("Socket");
        printf("Error: Weather data will not get");
    }
    else
    {
        // connect to the weather station
        if((conn_status = connect(sockfd, (struct sockaddr *)&remote_addr, sizeof(struct sockaddr))) < 0)
        {
            char err_str[200];
            snprintf(err_str, sizeof(err_str), "Connect to IP %s", WS_REMOTE_IP);
            perror(err_str);
            printf("Error: Weather data will not get");
        }
    }

    // pre-generate data file for data storage
    gen_datafile(data_path);

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
                buf_cnt = 0;

                // kill the timer
                if (conn_status == 0) // while successfully connected to the weather station
                {
                    alarm(0);
                }

                // Write weather data to the dataset before file close
                status = H5Dwrite (weather_dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);

                // Close and release resources.
                status = H5Dclose (dataset_id);
                status = H5Dclose (weather_dset);
                status = H5Sclose (dataspace_id);
                status = H5Tclose (memtype);
                status = H5Tclose (filetype);
                status = H5Fclose (file_id);

                // re-initialize weather buffer to nan
                u_char *uc = (u_char *)weather;
                for (i=0; i<9*nweather*sizeof(float)/sizeof(u_char); i++)
                {
                    uc[i] = 0xFF;
                }

                gen_datafile(data_path);
            }
            else
            {
                buf_cnt++;
            }

            // re-initialize the buffer
            for (i=0; i<buflen; i++)
                buf01[i] = 0xFF;
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
                buf_cnt = 0;

                // kill the timer
                if (conn_status == 0) // while successfully connected to the weather station
                {
                    alarm(0);
                }

                // Write weather data to the dataset before file close
                status = H5Dwrite (weather_dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);

                // Close and release resources.
                status = H5Dclose (dataset_id);
                status = H5Dclose (weather_dset);
                status = H5Sclose (dataspace_id);
                status = H5Tclose (memtype);
                status = H5Tclose (filetype);
                status = H5Fclose (file_id);

                // re-initialize weather buffer to nan
                u_char *uc = (u_char *)weather;
                for (i=0; i<9*nweather*sizeof(float)/sizeof(u_char); i++)
                {
                    uc[i] = 0xFF;
                }

                gen_datafile(data_path);
            }
            else
            {
                buf_cnt++;
            }

            // re-initialize the buffer
            for (i=0; i<buflen; i++)
                buf02[i] = 0xFF;
            buf02_state = 0;
        }
        if (buf01_state == 0 && buf02_state == 0 && DataExist == 0)
        {
            // Write weather data to the dataset before file close
            status = H5Dwrite (weather_dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);

            // Close and release resources.
            status = H5Dclose (dataset_id);
            status = H5Dclose (weather_dset);
            status = H5Sclose (dataspace_id);
            status = H5Tclose (memtype);
            status = H5Tclose (filetype);
            status = H5Fclose (file_id);

            if (sockfd == 0)
                close(sockfd);

            break;
        }
    }
}


void recvData(const char *data_path)
{
    char log_path[150];
    register int packet_len ;
    register int row = 0;
    register int init_cnt, current_cnt, freq_ind, pkt_id, pkt_id_old=-1;
    register int row_in_buf = N_FREQUENCY * N_INTEGRA_TIME;
    register long row_size = 8 * N_BASELINE;
    u_char frame_buff[BUFSIZE];
    u_char * frame_buff_p = frame_buff;
    u_char * start_buf_p;
    u_char * start_frame_p;
    int copy_len;
    int recv_fd;
    int old_cnt, i = 0;
    struct sockaddr_ll sll;
    struct ifreq ifr;
    FILE *fp;

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

    strcpy(log_path, data_path);
    strcat(log_path, "recv_data.log");
    fp = fopen(log_path, "wb");

    printf("Begin to receive data ... \n");
    fflush(stdout);

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
                // find where time count changes as the starting point to receive data to buffer
                if (init_cnt != old_cnt)
                {
                    pkt_id = 0;
                    // use this time as the data receiving start time (may need more accurate start time, but how to get?)
                    PyRun_SimpleString("start_time = ephem.Date(ephem.localtime(ephem.now()))");
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
                    // printf("%d ", row);
                }
                else if (pkt_id < pkt_id_old) // have packet lost
                {
                    // drop packets until find packet 0
                    while(1)
                    {
                        packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
                        pkt_id = *(int *)(frame_buff_p + 18);
                        if (pkt_id == 0)
                        {
                            current_cnt = *(int *)(frame_buff_p + 22);
                            freq_ind = *(int *)(frame_buff_p + 26);
                            row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind;
                            // printf("%d ", row);
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
                    // printf("%d ", row);
                }
                else if (pkt_id < pkt_id_old) // have packet lost
                {
                    // drop packets until find packet 0
                    while(1)
                    {
                        packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
                        pkt_id = *(int *)(frame_buff_p + 18);
                        if (pkt_id == 0)
                        {
                            current_cnt = *(int *)(frame_buff_p + 22);
                            freq_ind = *(int *)(frame_buff_p + 26);
                            row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind;
                            // printf("%d ", row);
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
    } else if (MATCH("change", "nscycle")) {
        pconfig->nscycle = strdup(value);
    } else if (MATCH("change", "nsduration")) {
        pconfig->nsduration = strdup(value);
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
    Py_Initialize(); //initialize python
    PySys_SetArgv(argc, argv);
    pMain = PyImport_AddModule("__main__");
    pMainDict = PyModule_GetDict(pMain);
    PyRun_SimpleString("import ephem");

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
        return 1;
    }
    if (agmts.verbose) {
        printf("Configure parameters loaded from '%s'", config_file);
    }

    /* create data path if it does not exist */
    data_path = agmts.args[0];
    create_data_path(data_path);

    /* allocate and initialize buffer */
    init_buf();

    #pragma omp parallel num_threads(2) private(thread_id)
    {
        thread_id = omp_get_thread_num();
        if(thread_id == 0)
            recvData(data_path);
        else if(thread_id == 1)
            writeData(data_path);
    }

    // release resources
    free_buf();

    printf("Over.\n");
    fflush(stdout);

    Py_Finalize();
    return 0;
}

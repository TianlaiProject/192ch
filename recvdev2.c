#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <argp.h>
#include <omp.h>
#include <pthread.h>
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
//#include <limits.h>
#include <netdb.h>
#include "hdf5.h"
#include "hdf5_hl.h"
#include "ini.h"

#define DEVICE_NAME "enp2s0d1"  // the name of the network device

#define BUFSIZE            2048
#define MAX_PACKET_ID      100
#define MAX_PACKET_SIZE    1488
#define FIRST_PACKET_SIZE  1488 - 8
#define MIN_PACKET_SIZE    1000
#define MAX_RAWPACKET_SIZE 1510
#define MIN_RAWPACKET_SIZE 1022

#define N_BASELINE ((MAX_PACKET_ID - 1)*MAX_PACKET_SIZE + MIN_PACKET_SIZE - 88) / 8 // 4 bytes real and 4 bytes imag
#define N_FREQUENCY 1008 //(1008 - 42)
#define FREQ_OFFSET 216 //257
#define N_INTEGRA_TIME 10       // N_INTEGRA_TIME integration times in one buf
#define buflen 8 * N_BASELINE * N_FREQUENCY * N_INTEGRA_TIME // Bytes
#define N_BUFFER_PER_FILE 15   // 30 min data per file
#define N_TIME_PER_FILE N_INTEGRA_TIME * N_BUFFER_PER_FILE
#define ONE_HDF5_SIZE (1L * buflen * N_BUFFER_PER_FILE) // Note this size is minimum, lack of weather and other data.

// Disk loop related
#define DISKLOOP_FLAG "dloop" // Represents one disk. A number is behind it, representing disk number.
#define DISKNUM         8
#define DISKLOOP_MIN  1
#define DISKLOOP_MAX  (DISKNUM+1)
#define DISKFULL_NOTICE_INTERVAL 7200   // seconds

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
    //float inttime;
    double inttime;
    //float weatherperiod;
    double weatherperiod;
    const char* keywordver;
    const char* recvver;
    const char* corrver;
    const char* telescope;
    const char* history;
    const char* sitename;
    //float sitelat;
    double sitelat;
    //float sitelon;
    double sitelon;
    //float siteelev;
    double siteelev;
    const char* timezone;
    const char* epoch;
    //float dishdiam;
    double dishdiam;
    int nants;
    int nfeeds;
    int npols;
    //float cylen;
    double cylen;
    //float cywid;
    double cywid;
    //float lofreq;
    double lofreq;
    int samplingbits;
    int corrmode;
    int nfreq;
    //float freqstart;
    double freqstart;
    //float freqstep;
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
int pkt_id = -1; // jx
int weather_exist = 0;
char weather_file[1024];
struct itimerval new_value, old_value; // for timer use
int timer_cnt = 0;
//float accurate_inttime; // integration time, Unit: second
double accurate_inttime; // integration time, Unit: second
int nfeeds, nchans, nbls, nns, nweather;
int *feedno, *channo, *blorder;
//float *feedpos, *antpointing, *pointingtime, *polerr, *nspos, *noisesource, *weather;
double *feedpos, *antpointing, *pointingtime, *polerr, *nspos, *noisesource, *weather;
//const float *transitsource[] = {}; // no transit source for cylinder array
const double *transitsource[] = {}; // no transit source for cylinder array

unsigned char * buf01;
unsigned char * buf02;
int buf01_state = 0 ;
int buf02_state = 0 ;

// Disk loop related
char * data_path;
int InsertDiskHours = 5;        // Insert disk within 5 hours.
int DiskN = -1;                 // Current disk in use.
int DiskNext = -1;              // Next disk to use.
int NextDiskStat;               // 0 enough; 1 not enough; -1 SSD.
time_t DiskFullNoticeTime = 0L; // Last notice time; time_t is long type.
time_t CurrTime;                // Current time; time_t is long type.
pthread_t MailThread;           // Sub thread to send email.
//int MailThreadAlive;            // If mailing thread is alive.
char MailRecipients[150] = "wufq@bao.ac.cn,tianlaisite@163.com,"; //  Add emails here; Use comma ',' to add more emails. E.g.: a@b.com,c@d.com,e@f.com,   The string should ends with a comma. Do not insert blank space.
char MailSubject[40] = "[ERGENT] No Disk for Observation ! ! !";
char MailMessage[][250] = {"No disk any more. Insert more disks within ",
                            " hours into disk slot ",
			    " of Node3, please. \n",
			    "This email will be sent every 2 hours until you have inserted empty disk.\n\nThis is an auto-generated email. Please DO NOT REPLY.\nIf you have any questions, please contact jxli@bao.ac.cn\n\nThanks!\n\nTianlai Site\n"
			    };

PyObject *pMainDict = NULL;

/////////////////////////////////////////////
//int cmpSrcAddress( u_char *SrcA , u_char *SrcB); // Use strnicmp() function.
//int cmpFlags(u_char *SrcA);
//////////////////////////////////////////////


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


//void create_data_path(const char *data_path)
void create_data_path(char *data_path)
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
    int i;
    char data_dir[1024], feedpos_dir[1024], blorder_dir[1024], nspos_dir[1024];
    FILE *data_file;

    // allocate and initialize data receiving buffers
    buf01 = (unsigned char *)malloc( sizeof(unsigned char)*buflen );
    buf02 = (unsigned char *)malloc( sizeof(unsigned char)*buflen );
    //for (i=0; i<buflen; i++)
    //{
    //    buf01[i] = 0xFF;
    //    buf02[i] = 0xFF;
    //}
    memset(buf01, 0xFF, sizeof(unsigned char)*buflen);
    memset(buf02, 0xFF, sizeof(unsigned char)*buflen);

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
    //feedpos = (float *)malloc( sizeof(float)*3*nfeeds );
    feedpos = (double *)malloc( sizeof(double)*3*nfeeds );
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
        //fscanf(data_file, "%f", &feedpos[i] );
        fscanf(data_file, "%lf", &feedpos[i] );
    }
    fclose(data_file);

    // antpointing
    //antpointing = (float *)malloc( sizeof(float)*4*nfeeds );
    antpointing = (double *)malloc( sizeof(double)*4*nfeeds );
    for (i=0; i<nfeeds; i++)
    {
        antpointing[4*i] = 0.0;
        antpointing[4*i+1] = 90.0;
        antpointing[4*i+2] = 0.0; // should be correct AzErr
        antpointing[4*i+3] = 0.0; // should be correct AltErr
    }

    // pointingtime
    //pointingtime = (float *)malloc( sizeof(float)*2 );
    pointingtime = (double *)malloc( sizeof(double)*2 );
    for (i=0; i<2; i++)
    {
        pointingtime[0] = 0.0; // fill the correct value when saving the data set to file
        pointingtime[1] = -1.0; // negative means the end observation time
    }

    // polerr
    //polerr = (float *)malloc( sizeof(float)*2*nfeeds );
    polerr = (double *)malloc( sizeof(double)*2*nfeeds );
    for (i=0; i<2*nfeeds; i++)
    {
        // should be the correct pol err
        polerr[i] = 0.0;
    }

    // nspos
    //nspos = (float *)malloc( sizeof(float)*3*nns );
    nspos = (double *)malloc( sizeof(double)*3*nns );
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
        //fscanf(data_file, "%f", &nspos[i] );
        fscanf(data_file, "%lf", &nspos[i] );
    }
    fclose(data_file);

    // noisesource
    //noisesource = (float *)malloc( sizeof(float)*3*nns );
    noisesource = (double *)malloc( sizeof(double)*3*nns );
    //float start, stop, cycle;
    double start, stop, cycle;
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
    //weather = (float *)malloc( sizeof(float)*10*nweather );
    weather = (double *)malloc( sizeof(double)*10*nweather );
    // initialize weather data to nan
    u_char *uc = (u_char *)weather;
    //for (i=0; i<10*nweather*sizeof(float)/sizeof(u_char); i++)
    for (i=0; i<10*nweather*sizeof(double)/sizeof(u_char); i++)
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

//void gen_datafile(const char *data_path)
void gen_datafile(char *data_path)
{
    //float span, start_offset, end_offset;
    double span, start_offset, end_offset;
    //float sec1970;
    double sec1970;
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
    //sec1970 = start_offset + (float) PyFloat_AsDouble(pObj); // Seconds since epoch 1970 Jan. 1st; Equals “obstime”
    sec1970 = start_offset + (double) PyFloat_AsDouble(pObj); // Seconds since epoch 1970 Jan. 1st; Equals “obstime”
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
    attr_id = H5Acreate2(file_id, "sitelat", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &config.sitelat);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "sitelon", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &config.sitelon);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "siteelev", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &config.siteelev);
    status = H5Aclose (attr_id);
    H5LTset_attribute_string(file_id, "/", "timezone", config.timezone);
    H5LTset_attribute_string(file_id, "/", "epoch", config.epoch);
    // Type C: Antenna
    H5LTset_attribute_string(file_id, "/", "telescope", config.telescope);
    attr_id = H5Acreate2(file_id, "dishdiam", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &config.dishdiam);
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
    attr_id = H5Acreate2(file_id, "cylen", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &config.cylen);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "cywid", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &config.cywid);
    status = H5Aclose (attr_id);
    // Type D: Receiver
    H5LTset_attribute_string(file_id, "/", "recvver", config.recvver);
    attr_id = H5Acreate2(file_id, "lofreq", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &config.lofreq);
    status = H5Aclose (attr_id);
    // Type E: Correlator
    H5LTset_attribute_string(file_id, "/", "corrver", config.corrver);
    attr_id = H5Acreate2(file_id, "samplingbits", H5T_STD_I32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_INT, &config.samplingbits);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "corrmode", H5T_STD_I32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_INT, &config.corrmode);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "inttime", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &accurate_inttime);
    status = H5Aclose (attr_id);
    H5LTset_attribute_string(file_id, "/", "obstime", obs_time);
    attr_id = H5Acreate2(file_id, "sec1970", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &sec1970);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "nfreq", H5T_STD_I32LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_INT, &config.nfreq);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "freqstart", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &config.freqstart);
    status = H5Aclose (attr_id);
    attr_id = H5Acreate2(file_id, "freqstep", H5T_IEEE_F64LE, attr_space,  H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, H5T_NATIVE_DOUBLE, &config.freqstep);
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
    dset = H5Dcreate (file_id, "feedpos", H5T_IEEE_F64LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, feedpos);
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
    dset = H5Dcreate (file_id, "antpointing", H5T_IEEE_F64LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, antpointing);
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
    dset = H5Dcreate (file_id, "pointingtime", H5T_IEEE_F64LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    pointingtime[0] = sec1970; // fill the correct start time here before saving
    status = H5Dwrite (dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, pointingtime);
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
    dset = H5Dcreate (file_id, "polerr", H5T_IEEE_F64LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, polerr);
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
    dset = H5Dcreate (file_id, "nspos", H5T_IEEE_F64LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, nspos);
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
    dset = H5Dcreate (file_id, "noisesource", H5T_IEEE_F64LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, noisesource);
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
    dset = H5Dcreate (file_id, "transitsource", H5T_IEEE_F64LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, transitsource);
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
    weather_dset = H5Dcreate (file_id, "weather", H5T_IEEE_F64LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    // NOTE: here we write the initial weather buffer to this data set, but the weather buffer haven't filled by the real weather data yet, we will fill it before the close of the hdf5 file
    status = H5Dwrite (weather_dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);
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

int getNumber(char * full_str, char * number_flag)
// Get non-negtive number following number_flag in full_str.
// e.g.: ssssssssssnumber_flagNNNNNsssssssss will return NNNNN
// return -1 if number_flag not found.
// NNNNN should be smaller than maximum value of int type.
{
    char * p = strstr(full_str, number_flag); // find number_flag
    if (p == NULL)
        return -1;
    p += strlen(number_flag);
    int sum=0;
    //while ((*p >=48) && (*p <= 57))
    while (isdigit(*p))
        sum = 10 * sum + (int) (*p++ - 48);
    return sum;
}

int changeNumber(char * full_str, char * number_flag, int number)
// Change number after number_flag with number in full_str.
// e.g.: full_str ssssssssssnumber_flagNNNNNsssssssss will become
//                ssssssssssnumber_flag(N+1)sssssssss.
{
    char * p_flag = strstr(full_str, number_flag); // find number_str
    if (p_flag == NULL)
        return -1;
    // p_flag points to number.
    p_flag += strlen(number_flag);
    char * p_tailer = p_flag;
    while (isdigit(*p_tailer)) p_tailer++;
    // take down tailer string.
    char tailer_str[100];
    sprintf(tailer_str, "%s", p_tailer);
    sprintf(p_flag, "%d%s", number, tailer_str);
    return 0;
}

int max(int a, int b)
    {return (a > b ? a : b);}

int setDataPath(char * data_path)
{
    printf("Current data_path = "); puts(data_path); //test

// Set data_path if available space is not enough, otherwise, do nothing.
    struct statfs disk_info;
    statfs(data_path, &disk_info);
    unsigned long total_blocks = disk_info.f_bsize;
    //unsigned long total_size = total_blocks * disk_info.f_blocks; //Disk total size
    unsigned long avail_size = disk_info.f_bavail*total_blocks;   //Disk available  size
    printf("Current disk space = %f GB\n", avail_size / 1024./1024./1024.);
    DiskN = getNumber(data_path, DISKLOOP_FLAG);
    if (avail_size > ONE_HDF5_SIZE + (1<<27)) // available space > one hdf5 file + 128MB
    {
        printf("No need to change disk.\n");
        return 0; // data_path is not modified.
    }
    else
    {
        DiskN    = max(DISKLOOP_MIN, (DiskN + 1)%DISKLOOP_MAX); //Change to next disk.
        DiskNext = max(DISKLOOP_MIN, (DiskN + 1)%DISKLOOP_MAX); //Set DiskNext.
        changeNumber(data_path, DISKLOOP_FLAG, DiskN);
        printf("Set New data_path = "); puts(data_path); // test
        return 1; // data_path is modified.
    }
}

/* Check if next disk has > 1TB space. */
int chkNextDisk(char * data_path)
{
    char dp[150];
    sprintf(dp, "%s", data_path);
    //printf("Current data_path = %s\n", dp);
    int curr_disk = getNumber(dp, DISKLOOP_FLAG);
    //int next_disk = max(DISKLOOP_MIN, (curr_disk + 1)%DISKLOOP_MAX);
    DiskNext = max(DISKLOOP_MIN, (curr_disk + 1)%DISKLOOP_MAX);
    char * p_flag = strstr(dp, DISKLOOP_FLAG);
    if (p_flag == NULL)
        return -1;
    // p_flag points to number.
    p_flag += strlen(DISKLOOP_FLAG);
    //sprintf(p_flag, "%d", next_disk);
    sprintf(p_flag, "%d", DiskNext);

//    changeNumber(dp, DISKLOOP_FLAG, next_disk);
    printf("Next data path = %s\n", dp);
    struct statfs disk_info;
    statfs(dp, &disk_info);
    unsigned long total_blocks = disk_info.f_bsize;
    //unsigned long total_size = total_blocks * disk_info.f_blocks; //Disk total size
    unsigned long avail_size = disk_info.f_bavail*total_blocks;   //Disk available  size in byte
    printf("Next disk space = %f GB\n", avail_size/1024./1024./1024.);
    return (avail_size > (1L<<40)) ? 0 : 1; // 0: >1TB; 1: <1TB
}

// Send Email
// mailto: Use comma to add multi emails. E.g. a@b.com,c@d.com,
//                 Should always end with a comma;
//                 Should not contain any blank space.
// mailSubject: The Subject of the mail.
// mailMessage: The Message of the mail.
//void * SendMail()
int SendMail()
{
    char * mail_rcpts = MailRecipients;
    char * mail_subject = MailSubject;
    char mail_msg[1500];
    sprintf(mail_msg, "%s%d%s%d%s%s", MailMessage[0], InsertDiskHours, MailMessage[1], DiskNext, MailMessage[2], MailMessage[3]);

    int sockfd;
    char smtpSvr[25] = "smtp.163.com";
    short smtpPort = 25;
    struct sockaddr_in server_addr;
    struct hostent *host;
    char recvBuf[2048];
    char sendBuf[2048];
    int rsBytes;
    
    if ((host = gethostbyname(smtpSvr)) == NULL) /*Get host IP.*/
    {
        fprintf(stderr, "Gethostname #rror: %s\n", strerror(errno));
	//pthread_exit(NULL);
        return 1;
    }
    /* Build socket and set socket. */
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        fprintf(stderr, "Socket Error: %s\n", strerror(errno));
	//pthread_exit(NULL);
        return 1;
    }
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(smtpPort);
    server_addr.sin_addr = *((struct in_addr *) host -> h_addr);
    int nNetTimeout = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&nNetTimeout, sizeof(int));
    
    /* Connect to server. */
    if(connect(sockfd, (struct sockaddr *) (&server_addr), sizeof(struct sockaddr)) == -1)
    {
        fprintf(stderr, "Connect Error: %s\n", strerror(errno));
	//pthread_exit(NULL);
        return 1;
    }
    
    //printf("Connected to server: %s on port %d\n", smtpSvr, smtpPort);
    rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf), 0);
    //printf("Recv connect: %s\n", recvBuf);
   
    // Send [HELO].
    sprintf(sendBuf, "HELO %s \r\n", smtpSvr);
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send HELO: %s", sendBuf);
    rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf), 0);
    recvBuf[rsBytes] = '\0';
    //printf("Recv HELO: %s\n", recvBuf);
    
    // Send [AUTH LOGIN]. 
    sprintf(sendBuf, "AUTH LOGIN\r\n");
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send AUTH LOGIN: %s", sendBuf);
    rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf), 0);
    recvBuf[rsBytes] = '\0';
    //printf("Recv AUTH LOGIN: %s\n", recvBuf);
    
    // Send user name - base64 encoded.
    sprintf(sendBuf, "dGlhbmxhaXNpdGU=\r\n");
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send USERNAME: %s", sendBuf);
    rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf), 0);
    recvBuf[rsBytes] = '\0';
    //printf("Recv USERNAME: %s\n", recvBuf);

    // Send password - base64 encoded.
    sprintf(sendBuf, "aGx4bW9uaXRvcjg=\r\n");
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send PASSWORD: %s", sendBuf);
    rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf),0);
    recvBuf[rsBytes] = '\0';
    //printf("Recv PASSWORD: %s\n", recvBuf);
    
    // Send [MAIL FROM]
    sprintf(sendBuf, "MAIL FROM: <tianlaisite@163.com>\r\n");
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send MAIL FROM: %s", sendBuf);
    rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf), 0);
    recvBuf[rsBytes] = '\0';
    //printf("Recv MAIL FROM: %s\n", recvBuf);
    
    // Send [RCPT TO]. Comma for multi recipients.
    char one_email_str[40]; // One email address.
    char * p_mail_recips = mail_rcpts;
    int i=0;
    while (* p_mail_recips)
    {
        if (* p_mail_recips == ',')
        {
            one_email_str[i] = '\0';
            i = 0; p_mail_recips++;
            sprintf(sendBuf, "RCPT TO: <%s>\r\n", one_email_str);
            rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
            //printf("Send RCPT TO: %s", sendBuf);
            rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf), 0);
            recvBuf[rsBytes] = '\0';
            //printf("Recv RCPT TO: %s\n", recvBuf);
        }
        else
            one_email_str[i++] = * p_mail_recips++;
    }

    // Send Cc
    sprintf(sendBuf, "RCPT TO: <jxli@bao.ac.cn>\r\n");
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send RCPT TO: %s", sendBuf);
    rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf), 0);
    recvBuf[rsBytes] = '\0';
    //printf("Recv RCPT TO: %s\n", recvBuf);

    // Send [DATA]
    sprintf(sendBuf, "DATA\r\n");
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send DATA: %s", sendBuf);
    rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf), 0);
    recvBuf[rsBytes] = '\0';
    //printf("Recv DATA: %s\n", recvBuf);

    // Send from to subject.
    sprintf(sendBuf, "From: \"Tianlai Auto Report\"<tianlaisite@163.com>\r\nTo: %s\r\nCc: <jxli@bao.ac.cn>\r\nSubject: %s\r\n\r\n", mail_rcpts, mail_subject); 
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send FromToSubject: \n%s\n", sendBuf);

    // Send message.
    sprintf(sendBuf, "%s\r\n", mail_msg);
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send Message: \n%s\n", sendBuf);

    // Send end flag.
    sprintf(sendBuf, "\r\n.\r\n");
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send End: %s\n", sendBuf);
    rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf), 0);
    recvBuf[rsBytes] = '\0';
    //printf("Recv End: %s\n", recvBuf);

    // Send [QUIT].
    sprintf(sendBuf, "QUIT\r\n");
    rsBytes = send(sockfd, sendBuf, strlen(sendBuf), 0);
    //printf("Send QUIT: %s", sendBuf);
    rsBytes = recv(sockfd, recvBuf, sizeof(recvBuf), 0);
    recvBuf[rsBytes] = '\0';
    //printf("Recv QUIT: %s\n", recvBuf);
    time(&DiskFullNoticeTime);
    close(sockfd);
    //pthread_exit(NULL);
    return 0;
}

//void writeData(const char *data_path)
void writeData(char *data_path)
{
    int i;
    complex_t *cbuf;
    herr_t     status;
    hid_t      sub_dataspace_id;

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

            // re-initialize the buffer
            for (i=0; i<buflen; i++)
                buf01[i] = 0xFF;
	    //memset(buf01, 0xFF, sizeof(unsigned char)*buflen);
            buf01_state = 0;

            if (buf_cnt == N_BUFFER_PER_FILE - 1)
            {
                buf_cnt = 0;

                if (weather_exist) // while weather data file exists
                {
                    // Write weather data to the dataset before file close
                    status = H5Dwrite (weather_dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);

                    // alarm(0); // kill the timer
                    timer_cnt = 0; // not kill timer, but reset timer counter

                    // re-initialize weather buffer to nan
                    u_char *uc = (u_char *)weather;
                    //for (i=0; i<9*nweather*sizeof(float)/sizeof(u_char); i++)
                    for (i=0; i<9*nweather*sizeof(double)/sizeof(u_char); i++)
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

                ///////// Disk Loop /////////////////////
                setDataPath(data_path);
                create_data_path(data_path);
                gen_datafile(data_path);
                time(&CurrTime);
                if ((difftime(CurrTime, DiskFullNoticeTime) > DISKFULL_NOTICE_INTERVAL) && (chkNextDisk(data_path) != 0)) // Available space on next disk is not enough.
                {
                    printf("%s%d%s%d%s\n", MailMessage[0], InsertDiskHours, MailMessage[1], DiskNext, MailMessage[2]);
                    //if ((pthread_create(&MailThread, NULL, SendMail, NULL)) != 0) //Create mailing subthread.
                    if (SendMail() != 0)
                        printf("ERROR: Tried to startup mailing thread, but failed. Warning: NO ENOUGH DISK SPACE!!!\n");
                    else
                        printf("Email has been sent.\n");
                    //pthread_join(MailThread, NULL);
                }
                /////////////////////////////////////////
            }
            else
            {
                buf_cnt++;
            }

        }

        if( buf02_state == 1 )
        {
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
	    //memset(buf01, 0xFF, sizeof(unsigned char)*buflen);
            buf02_state = 0;

            if (buf_cnt == N_BUFFER_PER_FILE - 1)
            {
                buf_cnt = 0;

                if (weather_exist) // while weather data file exists
                {
                    // Write weather data to the dataset before file close
                    status = H5Dwrite (weather_dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);

                    // alarm(0); // kill the timer
                    timer_cnt = 0; // not kill timer, but reset timer counter

                    // re-initialize weather buffer to nan
                    u_char *uc = (u_char *)weather;
                    //for (i=0; i<9*nweather*sizeof(float)/sizeof(u_char); i++)
                    for (i=0; i<9*nweather*sizeof(double)/sizeof(u_char); i++)
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

                ///////// Disk Loop /////////////////////
                setDataPath(data_path);
                create_data_path(data_path);
                gen_datafile(data_path);
                time(&CurrTime);
                if ((difftime(CurrTime, DiskFullNoticeTime) > DISKFULL_NOTICE_INTERVAL) && (chkNextDisk(data_path) != 0)) // Available space on next disk is not enough.
                {
                    printf("%s%d%s%d%s\n", MailMessage[0], InsertDiskHours, MailMessage[1], DiskNext, MailMessage[2]);
                    //if ((pthread_create(&MailThread, NULL, SendMail, NULL)) != 0) //Create mailing subthread.
                    if (SendMail() != 0)
                        printf("ERROR: Tried to startup mailing thread, but failed. Warning: NO ENOUGH DISK SPACE!!!\n");
                    else
                        printf("Email has been sent.\n");
                    //pthread_join(MailThread, NULL);
                }
                /////////////////////////////////////////
            }
            else
            {
                buf_cnt++;
            }

        }

        if (buf01_state == 0 && buf02_state == 0 && DataExist == 0)
        {
            // Write weather data to the dataset before file close
            status = H5Dwrite (weather_dset, H5T_IEEE_F64LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);

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


//void recvData(const char *data_path)
void recvData(char *data_path)
{
    char tmp_str[150];
    char log_path[150];
    register int packet_len ;
    register int row = 0;
    register int init_cnt, current_cnt, freq_ind, pkt_id_old=-1;
    register int row_in_buf = N_FREQUENCY * N_INTEGRA_TIME;
    register long row_size = 8 * N_BASELINE;
    u_char frame_buff[BUFSIZE];
    u_char * frame_buff_p = frame_buff;
    u_char * start_buf_p;
    u_char * start_frame_p;
    int copy_len;
    int recv_fd;
    //int old_cnt, i = 0;
    struct sockaddr_ll sll;
    struct ifreq ifr;
    FILE *fp;

    // open log file
    strcpy(log_path, data_path);
    strcat(log_path, "/recv_data.log");
    fp = fopen(log_path, "wb");

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

/* Frame structure:
 * 6B recv MAC, 6B send Mac, 2B Len, 4B pkt flag, 4B pkt id, DATA part
 * 0.........5, 6........11, 12, 13, 14.......17, 18......21, 22......
 *
 * recv MAC is local MAC; send Mac is correlator MAC; 2B Length is always 1510?TODO;
 * (Above MAC and Len are big-endian. Below are all small-endian.)
 * pkt flag is always 0xFFFFFFFF; pkt cnt is from 0 to 99;
 * time cnt is the total count of integration time since correlator has been started up;
 * freq id is frequency id, usually the sequence is:
 *  0, 12, 24, ......, 565,
 *  1, 13, 25, ......, 566,
 *  ......................
 * 11, 23, 25, ......, 576.
 * but sometimes may change during test period. 
 * 
 * DATA part structure:
 * When pkt id == 0, the first 8B are 4B time cnt and 4B freq id, the residuals are visibility data;
 * Otherwise, all of them are visibility data.
 * (Note: the length of DATA part are different. For pkt id == 99, it's TODO, otherwise it's TODO.)
 *
 * Visibility data structure:
 * When all of the visibility data part are assembled, the block structure is:
 * Time, Frequency, Baselines
 * (Note: The Baselines have a very complicated structure.)
 * Each visibility data is a complex number: 4B real part ahead and 4B imaginary part behind.
 * Both of real part and imaginary part are small-endian float number.
 */

    while (Running) // Find packet zero.
    {
        packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
        // Start when pkt id is 0, freq id is 0 and pkt flag is 0xffffffff.
        if ((*(int *)(frame_buff_p + 18) == 0) && (*(int *)(frame_buff_p + 26) == 0) && (*(int *)(frame_buff_p + 14) == -1))
        {
//        if (*(int *)(frame_buff_p + 18) == 0) //find pkt 0.
//        {
//            if (i == 0)
//            {
//                old_cnt = *(int *)(frame_buff_p + 22);
//                i = 1;
//            }
//            else
//            {
                init_cnt = *(int *)(frame_buff_p + 22);
                // find where time count changes as the starting point to receive data to buffer
//                if (init_cnt != old_cnt)
//                {
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
//                }
//            }
        }
    }

    while(Running)
    {
        if (buf01_state == 0)
        {
            printf("change buf\n");
            // the initial row when begin to write an empty bufffer
            row = *(int *)(frame_buff_p + 26) + FREQ_OFFSET;
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
                packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
                pkt_id = *(int *)(frame_buff_p + 18);
                if (pkt_id == 0)
                {
                    current_cnt = *(int *)(frame_buff_p + 22);
                    freq_ind = *(int *)(frame_buff_p + 26) + FREQ_OFFSET;
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
                            freq_ind = *(int *)(frame_buff_p + 26) + FREQ_OFFSET;
                            row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind;
                            // printf("%d ", row);
                            break;
                        }
                    }
                }

                if (((pkt_id + MAX_PACKET_ID - pkt_id_old) % MAX_PACKET_ID) != 1)
                    fprintf(fp, "Jump from %d to %d.\n", pkt_id_old, pkt_id);
                pkt_id_old = pkt_id;
            }
            buf01_state = 1;
            init_cnt = current_cnt;
        }

        if (Running == 0) continue;

        if (buf02_state == 0)
        {
            printf("change buf\n");
            // the initial row when begin to write an empty bufffer
            row = *(int *)(frame_buff_p + 26) + FREQ_OFFSET;
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
                packet_len = recv(recv_fd, frame_buff, BUFSIZE, 0);
                pkt_id = *(int *)(frame_buff_p + 18);
                if (pkt_id == 0)
                {
                    current_cnt = *(int *)(frame_buff_p + 22);
                    freq_ind = *(int *)(frame_buff_p + 26) + FREQ_OFFSET;
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
                            freq_ind = *(int *)(frame_buff_p + 26) + FREQ_OFFSET;
                            row = N_FREQUENCY*(current_cnt - init_cnt) + freq_ind;
                            // printf("%d ", row);
                            break;
                        }
                    }
                }

                if (((pkt_id + MAX_PACKET_ID - pkt_id_old) % MAX_PACKET_ID) != 1)
                    fprintf(fp, "Jump from %d to %d.\n", pkt_id_old, pkt_id);
                pkt_id_old = pkt_id;
            }
            buf02_state = 1;
            init_cnt = current_cnt;
        }

        if  (buf01_state == 1 && buf02_state == 1)
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
    //const char *data_path; not const any more. Change it to global variable.

    /* Signal handle. */
    signal(SIGUSR1, kill_handler);
    signal(SIGALRM, timer_handler);

    // Receiver MAC.
    //Src[0]=0xf4; Src[1]=0x52; Src[2]=0x14; Src[3]=0x1f; Src[4]=0x36; Src[5]=0x51;
    ////Sender MAC
    //Src[6]=0xAA; Src[7]=0xBB; Src[8]=0xCC; Src[9]=0x04; Src[10]=0xDD; Src[11]=0xEE;
    //Flags[0]=0xFF; Flags[1]=0xff; Flags[2]=0xFF; Flags[3]=0xFF;

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

    /* ljx:
 * We will use disks one by one.
 * The disks are mounted in a series of directories:
 * Example:
 * /obsdisks/dloop1, /obsdisks/dloop2..., /obsdisks/dloop8
 * where DISKLOOP_FLAG=dloop, and DISKLOOP_MIN=1, DISKLOOP_MAX=8
 *
 * The varialble data_path set in the start.sh shell will be the starting path.
 * Note that sometimes, the starting disk may not be the first one.
 * So at first, get DiskN from data_path, where DiskN is currently in use.
 * 
 * During running, after each hdf5 file is created, following jobs should be done:
 * if DiskN's available space > one hdf5 file's size + 128MB? // 128MB for weather data and so on.
 *     { Do nothing}
 * else
 *     { Modify data_path to next disk; // next disk always has enough space, it has been checked already. See below.
 *       Add "_partN" defined by DirPartN in the end of data_path  // Not exactly the end but before the final '/' symbol. E.g.: /obsdisks/dloop5/20160101012345_part3/   Note that only DiskN is changed while the date time keeps the same since we have _partN to avoid confusion.
 *       Create data path according to data_path;
 *       Over}
 * if Next disk's available space > 1TB? // 1TB can lasts some time for observer to insert more disks. 
 *     { Do nothing}
 * else // Send notices but do not send notices whenever a hdf5 file is finished (usually several minutes). Keep some interval according to DISKFULL_NOTICE_INTERVAL.
 *     if (current_time - DiskFullNoticeTime) < DISKFULL_NOTICE_INTERVAL?
 *         { Do not send notice }
 *     else
 *         { Printf disk space is not enough. Insert more empty disks.
 *           Send Emails to observer; // The observer cannot insert more disks as soon as possible, but we have 1TB being a buffer. So send emails every 2 hours or some interval defined by DISKFULL_NOTICE_INTERVAL.
 *           DiskFullNoticeTime = current_time
 *         }
 *  */
/////////////////////////////////////////
    setDataPath(data_path);
    create_data_path(data_path);
    time(&DiskFullNoticeTime);
    if (chkNextDisk(data_path) != 0) // Available space on next disk is not enough.
    {
        printf("%s%d%s%d%s\n", MailMessage[0], InsertDiskHours, MailMessage[1], DiskNext, MailMessage[2]);
        //if ((pthread_create(&MailThread, NULL, SendMail, NULL)) != 0) //Create mailing subthread.
        if (SendMail() != 0)
            printf("ERROR: Trying to send mail, but failed. Warning: NO ENOUGH DISK SPACE!!!\n");
        //pthread_join(MailThread, NULL);
        else
            printf("Email has been sent.\n");
    }
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

    Py_DECREF(pMainDict);
    Py_Finalize();
    return 0;
}

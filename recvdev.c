#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <argp.h>
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
hid_t file_id, filetype, memtype, dataspace_id, dataset_id; /* HDF% handles */
hsize_t dims[3] = {N_TIME_PER_FILE, N_FREQUENCY, N_BASELINE};
hsize_t sub_dims[3] = {N_INTEGRA_TIME, N_FREQUENCY, N_BASELINE};
hsize_t offset[3] = {0, 0, 0}; /* subset offset in the file */
hsize_t count[3] = {N_INTEGRA_TIME, N_FREQUENCY, N_BASELINE}; /* size of subset in the file */
hsize_t stride[3] = {1, 1, 1}; /* subset stride in the file */
hsize_t block[3] = {1, 1, 1}; /* subset block in the file */

int buf_cnt = 0;
int file_count = 0;

int nfeeds, nchans, nbls, nns, nweather = 3;
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


void gen_obs_log()
{
}

void gen_datafile(const char *data_path)
{
    double int_time, span, start_offset, end_offset;
    char *obs_time;
    char *stime, *etime;
    // long syear, smonth, sday, shour, sminute;
    // long eyear, emonth, eday, ehour, eminute;
    // double ssecond, esecond;
    char tmp_str[150];
    char file_name[35];
    char file_path[150];
    /* char time_str[20]; */
    /* char *time_fmt; */
    // PyObject *pObj = NULL;
    hid_t space, dset, dcpl, dsettype, mtype;    /* Handles */
    herr_t status;

    int_time = config.inttime; // integration time, Unit: second
    span = int_time * N_TIME_PER_FILE; // time span in one file, Unit: second
    start_offset = file_count * span; // offset from start time for this file, second
    end_offset = (file_count + 1) * span - int_time; // offset from start time for this file, second

    if (file_count == 0)
        PyRun_SimpleString("start_time = ephem.Date(ephem.localtime(ephem.now()))");
    snprintf(tmp_str, 50, "stime = ephem.Date(start_time + %f * ephem.second)", start_offset);
    PyRun_SimpleString(tmp_str);
    snprintf(tmp_str, 50, "etime = ephem.Date(start_time + %f * ephem.second)", end_offset);
    PyRun_SimpleString(tmp_str);
    PyRun_SimpleString("obs_time = str(stime)");
    // get value from python, better to have error checking
    obs_time = PyString_AsString(PyMapping_GetItemString(pMainDict, "obs_time"));
    PyRun_SimpleString("stime = '%04d%02d%02d%02d%02d%02d' % stime");
    stime = PyString_AsString(PyMapping_GetItemString(pMainDict, "stime"));
    PyRun_SimpleString("etime = '%04d%02d%02d%02d%02d%02d' % etime");
    etime = PyString_AsString(PyMapping_GetItemString(pMainDict, "etime"));
    /* obs_time = PyString_AsString(PyRun_SimpleString("str(stime)")); */
    /* PyRun_SimpleString("(syear, smonth, sday, shour, sminute, ssecond) = stime.tuple()"); */
    /* PyRun_SimpleString("(eyear, emonth, eday, ehour, eminute, esecond) = etime.tuple()"); */
    /* /\* pObj = PyMapping_GetItemString(pMainDict, "syear"); *\/ */
    /* /\* if (pObj) *\/ */
    /* /\* { *\/ */
    /* /\*     syear = PyInt_AsLong(pObj); *\/ */
    /* /\*     printf("%s\n", date); *\/ */
    /* /\* } *\/ */
    /* /\* else *\/ */
    /* /\* { *\/ */
    /* /\*     PyErr_Print(); *\/ */
    /* /\* } *\/ */
    /* // get value from python, better to have error checking */
    /* syear = PyInt_AsLong(PyMapping_GetItemString(pMainDict, "syear")); */
    /* smonth = PyInt_AsLong(PyMapping_GetItemString(pMainDict, "smonth")); */
    /* sday = PyInt_AsLong(PyMapping_GetItemString(pMainDict, "sday")); */
    /* shour = PyInt_AsLong(PyMapping_GetItemString(pMainDict, "shour")); */
    /* sminute = PyInt_AsLong(PyMapping_GetItemString(pMainDict, "sminute")); */
    /* ssecond = PyFloat_AsDouble(PyMapping_GetItemString(pMainDict, "ssecond")); */
    /* eyear = PyInt_AsLong(PyMapping_GetItemString(pMainDict, "eyear")); */
    /* emonth = PyInt_AsLong(PyMapping_GetItemString(pMainDict, "emonth")); */
    /* eday = PyInt_AsLong(PyMapping_GetItemString(pMainDict, "eday")); */
    /* ehour = PyInt_AsLong(PyMapping_GetItemString(pMainDict, "ehour")); */
    /* eminute = PyInt_AsLong(PyMapping_GetItemString(pMainDict, "eminute")); */
    /* esecond = PyFloat_AsDouble(PyMapping_GetItemString(pMainDict, "esecond")); */
    /* time_t t = time(NULL); */
    /* struct tm tm = *localtime(&t); */

    /* time_fmt = "%04d%02d%02d%02d%02d%02d"; */
    /* snprintf(time_str, 15, time_fmt, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec); */

    // generate the observation log if required
    if (agmts.gen_obslog && file_count == 0)
    {
        gen_obs_log();
    }

    // data file name
    snprintf(file_name, 35, "%s_%s.hdf5", stime, etime);
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
    dset = H5Dcreate (file_id, "weather", H5T_IEEE_F32LE, space, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    // Write the data to the dataset
    status = H5Dwrite (dset, H5T_IEEE_F32LE, H5S_ALL, H5S_ALL, H5P_DEFAULT, weather);
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
    char data_dir[1024], feedpos_dir[1024];
    FILE *data_file;
    complex_t *cbuf;
    herr_t     status;
    hid_t      sub_dataspace_id;

    nfeeds = config.nfeeds;
    nchans = 2 * nfeeds;
    nbls = nchans * (nchans + 1) / 2;
    nns = config.nns;
    if (nbls != N_BASELINE)
    {
        printf("Error: Number of baselines %d unequal to N_BASELINE!!!\n", nbls);
        exit(-1);
    }

    // get data dir
    getcwd(data_dir, sizeof(data_dir));
    strcat(data_dir, "/data");

    /* allocate and fill buffers */
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
    for (i=0; i<2*nbls; i++)
    {
        // should be the correct baseline orders
        blorder[i] = 0;
    }
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
    for (i=0; i<9*nweather; i++)
    {
        // should be the correct weather values
        weather[i] = 0.0;
    }

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
                // Close and release resources.
                status = H5Dclose (dataset_id);
                status = H5Sclose (dataspace_id);
                status = H5Tclose (memtype);
                status = H5Tclose (filetype);
                status = H5Fclose (file_id);

                buf_cnt = 0;
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
                // Close and release resources.
                status = H5Dclose (dataset_id);
                status = H5Sclose (dataspace_id);
                status = H5Tclose (memtype);
                status = H5Tclose (filetype);
                status = H5Fclose (file_id);

                buf_cnt = 0;
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
            // Close and release resources.
            free(feedno);
            free(channo);
            free(blorder);
            free(feedpos);
            free(polerr);
            free(noisesource);
            free(weather);

            status = H5Dclose (dataset_id);
            status = H5Sclose (dataspace_id);
            status = H5Tclose (memtype);
            status = H5Tclose (filetype);
            status = H5Fclose (file_id);

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

    // initalize network related things
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
    fp= fopen(log_path, "wb");

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

    int i;
    int thread_id;
    const char *config_file;
    const char *data_path;

    /* Signal handle. */
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


    /* Default values. */
    agmts.verbose = 0;
    agmts.gen_obslog = 0;

    /* Parse our arguments; every option seen by parse_opt will be reflected in arguments. */
    argp_parse(&argp, argc, argv, 0, 0, &agmts);

    /* parse the configuration file */
    config_file = agmts.args[1];
    if (ini_parse(config_file, handler, &config) < 0) {
        printf("Can't load '%s'\n", config_file);
        return 1;
    }
    if (agmts.verbose) {
        printf("Configure parameters loaded from '%s'", config_file);
    }

    /* create data path if it does not exit */
    data_path = agmts.args[0];
    create_data_path(data_path);

    /* allocate buffer */
    buf01=(unsigned char *)malloc( sizeof(unsigned char)*buflen );
    buf02=(unsigned char *)malloc( sizeof(unsigned char)*buflen );
    for (i=0; i<buflen; i++)
    {
        buf01[i] = 0xFF;
        buf02[i] = 0xFF;
    }

    #pragma omp parallel num_threads(2) private(thread_id)
    {
        thread_id = omp_get_thread_num();
        if(thread_id == 0)
            recvData(data_path);
        else if(thread_id == 1)
            writeData(data_path);
    }

    free(buf01);
    free(buf02);

    printf("Over.\n");
    fflush(stdout);

    Py_Finalize();
    return 0;
}

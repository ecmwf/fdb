# FDB Tools

Description of the most common FDB CLI tools: [fdb-write](##fdb-write), [fdb-list](##fdb-list) and  [fdb-read](##fdb-read)

## fdb-write

Inserts data into the FDB, creating a new databases if needed.  
The data is copied into the FDB, and the tool reports the location where it was inserted.  
This process is atomic and can be run concurrently to other processes reading or writing to the same FDB databases.

### Usage
``fdb-write [options] <gribfile1> [gribfile2] ...``

### Options

  ` `                                    | ` `
  -------------------------------------- | -----------------------------------------------
  **--verbose**	                         | Prints more information, namely the key of each datum and the information of which data was filtered out.
  **--statistics**	                     | Report timing statistics
  **--include-filter=string=string,...** | Filter out any data that **does not match** this key-value pairs.<br />Key-value pairs can be in the form of a MARS request, e.g.: `step=141/to/240/by/3`
  **--exclude-filter=string=string,...** | Filter out any data that **does match** this key-value pair.<br />Key-value pairs can be in the form of a MARS request, e.g.: `levelist=850/1000`

### Examples

You may pass multiple grib files. The tool will insert them sequentially.

```
% fdb-write data.grib
 
Processing data.grib
FDB archive 12 fields, size 37.5412 Mbytes, in 0.088939 second (422.091 Mbytes per second)
fdb::service::archive: 0.089006 second elapsed, 0.089005 second cpu
```


Use `--include-filter` you have a large data set of which you want to write a small sub-set easily identifiable from a few keys ...

```
% fdb-write --verbose --include-filter=time=0000 data.grib
 
Processing data.grib
Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=0000,type=an}
Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=155,step=0,stream=oper,time=0000,type=an}
Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=138,step=0,stream=oper,time=0000,type=an}
Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=155,step=0,stream=oper,time=0000,type=an}
Include key {time=0000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=1200,type=an}
Include key {time=0000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an}
Include key {time=0000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=138,step=0,stream=oper,time=1200,type=an}
Include key {time=0000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an}
Archiving {class=rd,date=20160401,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=0000,type=an}
...
FDB archive 8 fields, size 25.0275 Mbytes, in 0.129475 second (193.301 Mbytes per seconds)
fdb::service::archive: 0.129522 second elapsed, 0.129514 second cpu
```


Use `--exclude-filter` you have a large data set of which you want to filter out a small sub-set easily identifiable from a few keys ...

```
% fdb-write --verbose --exclude-filter=time=1200,levelist=1000 data.grib
 
Processing data.grib
Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=0000,type=an}
Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=155,step=0,stream=oper,time=0000,type=an}
Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=138,step=0,stream=oper,time=0000,type=an}
Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=155,step=0,stream=oper,time=0000,type=an}
Exclude key {time=1200,levelist=1000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=1200,type=an}
Exclude key {time=1200,levelist=1000} filtered out datum {class=rd,date=20160402,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an}
Archiving {class=rd,date=20160402,domain=g,expver=xxxx,levelist=850,levtype=pl,param=138,step=0,stream=oper,time=1200,type=an}
Archiving {class=rd,date=20160401,domain=g,expver=xxxx,levelist=850,levtype=pl,param=155,step=0,stream=oper,time=0000,type=an}
Exclude key {time=1200,levelist=1000} filtered out datum {class=rd,date=20160401,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=138,step=0,stream=oper,time=1200,type=an}
Exclude key {time=1200,levelist=1000} filtered out datum {class=rd,date=20160401,domain=g,expver=xxxx,levelist=1000,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an}
...
FDB archive 12 fields, size 37.5412 Mbytes, in 0.160719 second (233.584 Mbytes per seconds)
fdb::service::archive: 0.160764 second elapsed, 0.160724 second cpu
```




## fdb-list

Lists the contents of the FDB databases.  
In the body of the output, one line is given per field that has been archived. These (by default) present the fields that are available and will be retrievable - i.e. masked duplicates are skipped.  
The lines are broken into three segments, which represent the hierarchical nature of the schema:
* The first component identifies the FDB database containing the data
* The second component identifies the (set of) indexes
* The third component identifies entries collocated within an index

### Usage
``fdb-list [options] [request1] [request2] ...``

### Options

  ` `                                    | ` `
  -------------------------------------- | -----------------------------------------------
  **--location**                         | Also print the location of each field
  **--ignore-errors**                    | Ignore errors (report them as warnings) and continue processing wherever possible
  **--json**                             | Output in JSON form
  **--porcelain**                        | Streamlined and stable output for input into other tools
  **--raw**                              | 	Don't apply (contextual) expansion and checking on requests. <br />Keys and values passed must match those used internally to the FDB exactly. <br />This prevents the use of named parameters (such as t rather than param=130), dates (such as date=-1), or similar.
  **--full**                             | Include all entries (including masked duplicates)
  **--minimum-keys=string,string**       | Default is class,expver <br />Define the minimum set of keys that must be specified. This prevents inadvertently exploring the entire FDB. <br />**Note: Use this carefully as it may trigger exploring the entire FDB.**
  **--all**                              | **(Debug and testing only)** Visit all FDB databases


### Examples

You may pass a partial request (as a key) that will list all the fields in the FDB that match that key.
Note that this is a global search through all the databases of the FDB that match this key.

```
% fdb-list class=od,expver=0001,stream=oper,date=20151004
 
retrieve,
    class=od,
    expver=0001,
    stream=oper,
    date=20151004
 
{class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=700,param=155}
{class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=129}
{class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=130}
...
```

A JSON listing may be obtained for use in tools that parse the available data
```
% fdb-list --json class=od,expver=0001,stream=oper,date=20151004
 
[{"class":"od","stream":"oper","date":"20151004","time":"1200","domain":"g","type":"an","levtype":"pl","step":"0","levelist":"700","param":"155"},{...},...]
```

The `--location` option can be useful to identify exactly where the field is located within the database. 

```
% fdb-list --location class=od,expver=0001,stream=oper,date=20151004
 
retrieve,
    class=od,
    expver=0001,
    stream=oper,
    date=20151004
 
{class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=130} (/data/mars_p_d17_d17_1_15/fdb/od:0001:oper:20151004:1200:g/an:pl.20161103.120238.dhs1213.ecmwf.int.1739461754885.data,13121592,3280398)
...
```

The `--porcelain` option gives stable output for use in scripts and as input to other simple tools. This will only print (exactly) one line per entry, with no extraneous output. The output of this option will remain stable across versions.

```
% fdb-list --porcelain class=od,expver=0001,stream=oper,date=20151004
{class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=700,param=155}
{class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=129}
{class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=130}
...
```




## fdb-read

Read data from the FDB and write this data into a specified target file. This may involve visiting multiple databases if required by the request.

### Usage
```fdb-read request.mars target.grib
fdb-read --extract source.grib target.grib```

### Options

  ` `                                    | ` `
  -------------------------------------- | -----------------------------------------------
  **--extract**                          | Extract the request(s) from an existing GRIB file
  **--statistics**                       | Report timing statistics

### Examples

Specify the [MARS request](MARS.md) in a plain text file.  
Note that this MARS request must be fully expanded. For example, it may not contain the **/to/** or **/by/** statements that the MARS client is able to expand.

```
% cat myrequest
retrieve,class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g,type=an,levtype=pl,step=0,levelist=700,param=155
 
# this will retrieve 2 fields
% fdb-read myrequest foo.grib
retrieve,class=od,date=20151004,domain=g,expver=0001,levelist=500/700,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an
Compress handle: 3.3e-05 second elapsed, 3.2e-05 second cpu
Compress handle: 2e-06 second elapsed, 2e-06 second cpu
Read  rate: 4.7575 Gbytes per second
Write rate: 2.54081 Gbytes per second
Save into: 0.022224 second elapsed, 0.022182 second cpu
```

Obtain data from the FDB using the MARS request that would be implied by an existing GRIB file. In this example, foo.grib contains 2 fields that identify what needs to be retrieved.

```
% fdb-read --extract foo.grib out.grib
retrieve,class=od,date=20151004,domain=g,expver=0001,levelist=500,levtype=pl,param=155,step=0,stream=oper,time=1200,type=an
...
Compress handle: 3.3e-05 second elapsed, 3.2e-05 second cpu
Compress handle: 2e-06 second elapsed, 2e-06 second cpu
Read  rate: 4.7575 Gbytes per second
Write rate: 2.54081 Gbytes per second
Save into: 0.022224 second elapsed, 0.022182 second cpu
```


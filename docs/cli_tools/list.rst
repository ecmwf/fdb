fdb-list
========

Lists the contents of the FDB databases.  
In the body of the output, one line is given per field that has been archived. These (by default) present the fields that are available and will be retrievable - i.e. masked duplicates are skipped.  
The lines are broken into three segments, which represent the hierarchical nature of the schema:
* The first component identifies the FDB database containing the data
* The second component identifies the (set of) indexes
* The third component identifies entries collocated within an index

Usage
-----

``fdb-list [options] [request1] [request2] ...``

Options
-------

+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--location``                         | Also print the location of each field                                                                               |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--ignore-errors``                    | Ignore errors (report them as warnings) and continue processing wherever possible                                   |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--json``                             | Output in JSON form                                                                                                 |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--porcelain``                        | Streamlined and stable output for input into other tools                                                            |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--raw``                              | | Don't apply (contextual) expansion and checking on requests.                                                      |
|                                        | | Keys and values passed must match those used internally to the FDB exactly.                                       |
|                                        | | This prevents the use of named parameters (such as t rather than param=130), dates (such as date=-1), or similar  |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--full``                             | Include all entries (including masked duplicates)                                                                   |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--minimum-keys=string,string``       | | Default is ``class,expver``                                                                                       |
|                                        | | Define the minimum set of keys that must be specified. This prevents inadvertently exploring the entire FDB.      |
|                                        | | **Note: Use this carefully as it may trigger exploring the entire FDB.**                                          |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| ``--all``                              | **(Debug and testing only)** Visit all FDB databases                                                                |
+----------------------------------------+---------------------------------------------------------------------------------------------------------------------+


Examples
--------

You may pass a partial request (as a key) that will list all the fields in the FDB that match that key.
Note that this is a global search through all the databases of the FDB that match this key.
::

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


A JSON listing may be obtained for use in tools that parse the available data
::

  % fdb-list --json class=od,expver=0001,stream=oper,date=20151004
  
  [{"class":"od","stream":"oper","date":"20151004","time":"1200","domain":"g","type":"an","levtype":"pl","step":"0","levelist":"700","param":"155"},{...},...]


The ``--location`` option can be useful to identify exactly where the field is located within the database. 
::

  % fdb-list --location class=od,expver=0001,stream=oper,date=20151004
  
  retrieve,
      class=od,
      expver=0001,
      stream=oper,
      date=20151004
  
  {class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=130} (/data/mars_p_d17_d17_1_15/fdb/od:0001:oper:20151004:1200:g/an:pl.20161103.120238.dhs1213.ecmwf.int.1739461754885.data,13121592,3280398)
  ...

The ``--porcelain`` option gives stable output for use in scripts and as input to other simple tools. This will only print (exactly) one line per entry, with no extraneous output. The output of this option will remain stable across versions.
::

  % fdb-list --porcelain class=od,expver=0001,stream=oper,date=20151004
  {class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=700,param=155}
  {class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=129}
  {class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=130}
  ...



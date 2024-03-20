Config
======

Config files define a number of parameters for the FDB. There are several different
types, which can be described by a FDB config file, such as 
local, remote, distribute, and select. For an architectural overview, see
:ref:`architecture_frontend`.


Local
******

::

  type: local
  engine: toc
  schema: ./schema
  spaces:
  - handler: Default    
    roots:
    -path: /path/to/fdb/root


Local implements the passage of data from the frontend to storage backend, talk to the FDB Store and Catalogue. 
Depending on the backend, the data or metadata may not actually be local.


Select
*******

::

  type: select
  fdbs:
  - select: class=od
    type:local
    spaces:
      roots:
        -path: /path/to/fdb/od
  -select: class=rd,expver=xx.?.?
    type: local
    spaces:
      roots:
        - path: /path/to/fdb/rd

Select dispatches requests to different FDB's based on the metadata associated with the Messages, 
and can be used to send split requests operational data (OD) from research data (RD).

Remote
*******

::

  type: remote
  host: fdb-minus
  port: 36604

The remote type handles access to the remote FDB via TCP/IP.
It talks to the FDB server using an asynchronous protocol.
It only handles the transition. Not the distribution of data.
The distributed type implements the multi-lane access to multiple FDB's. 
It uses rendezvous hashing to avoid synchronisations.

Dist
*****

::

  type: dist
  lanes:
    -type: remote
      host: fdb-minus-1
      port: 36604
    -type: remote
      host: fdb-minus-2
      port: 36604

These types can be composed together in the config file when using FDB.

..
  _## TODO: Get this reviewed and add more information.

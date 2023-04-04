Config
======

Config files define a number of parameters for the FDB. 

The following is of the form local:
::
  type: local
  engine: toc
  schema: ./schema
  spaces:
  - hanlder: Default    
    roots:
    -path: /path/to/fdb/root

There a number of different types such as local, remote, distributed, and select.

Local implements the passage of dara from the frontend to storage backend, talk to the FDB Store and Catalogue. 
Depending on the backend, the data or metadata may not actually be local.

Select dispatches requests to different FDB's based on the metadata associated with the Messages, and can be used to send split requests OD from RD.

Select Type:
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

The remote type handles access to the remote FDB vis TCP/IP. It talks to the FDB server using an asynchronous protocol.
It only handles the transition. not the distribution of data.

Remote type:
::
  type: remote
  host: fdb-minus
  port: 36604

The distributed type implements the multi-lane access to multiple FDB's. It uses rendezvous hashing to avoid synchronisations.

Dist type:
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

## TODO: Get this reviewed and add more information.

Schema
======

The schema indexes structure and dala collocation policy.

## TODO: add more info on the schema
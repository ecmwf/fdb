##############
Setting up FDB
##############

Seting up FDB requires the user to write a configuration and a schema file.

***************************
Writing a FDB Configuration
***************************

Config files define a number of parameters for the FDB. 

The following is of the form local:
::
  type: local
  engine: toc
  schema: ./schema
  spaces:
  - handler: Default    
    roots:
    -path: /path/to/fdb/root

There a number of different types such as local, remote, distributed, and
select.

Local implements the passage of data from the frontend to storage backend, talk
to the FDB Store and Catalogue. Depending on the backend, the data or metadata
may not actually be local.

Select dispatches requests to different FDB's based on the metadata associated
with the Messages, and can be used to send split requests OD from RD.

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

The remote type handles access to the remote FDB vis TCP/IP. It talks to the
FDB server using an asynchronous protocol. It only handles the transition. not
the distribution of data.

Remote type:
::
  type: remote
  host: fdb-minus
  port: 36604

The distributed type implements the multi-lane access to multiple FDB's. It
uses rendezvous hashing to avoid synchronisations.

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

****************************
How to Setup Database Schema
****************************

The schema describes how data is partitioned in FDB. 

Minimum Working Example
=======================

.. code-block:: text
    :caption: A minimal schema

    [ class, expver, stream, date, time, domain?
        [ type, levtype
            [ step, levelist?, param ]]]

The above example shows a minimum configuration that configures FDB to ingest
forecast data from GRIB messages.

Structure of Schema
===================

Define Keyword Types
--------------------

By default all keywords use the type 'String'. If you want to use a different
type you can set a keyword type at the beginning of the schema file.
Alternatively a keywords datatype can also be defined inside the 'partitioning
rule' definition, see :ref:`Per Rule Keyword Datatype
<partition_rule_keyword_datatype>`.

TODO[kkratz]: Explain why you should use types here

.. code-block:: text
    :caption: Redfining datatypes for keywords

    param:     Param;
    step:      Step;
    date;      Date;
    latitude;  Double;
    longitude; Double;

Partitioning Rules
------------------

A Partitioning Rule defines how data is distributed inside FDB based on
keywords. Each rule is defined by three set of keys: 

The first set of keywords form the *database key*. The *database key*
identifies the directory where the data is stored.

The second set of keywords form the *co-location key*. The *co-location key*
identifies the file inside the database directory where the data will be
stored.

The third set of keywords form the *index key*. The *index key* identifies the
offset in the file storing the data.

.. code-block:: text
    :caption: Example rule

    # 'date' and 'time' attributes form the database key
    [ date, time,  
    # 'type' and 'levtype' form the co-location key
        [ type, levtype
    # 'step' and 'param' form the co-location key
            [ step, param ]]]

.. _partition_rule_keyword_datatype

Per Rule Keyword Datatype
^^^^^^^^^^^^^^^^^^^^^^^^^

Types for keywords can be redefined per rule inside the rules definition.
Inside the rule definition use ``keyword: type``, e.g.:

.. code-block:: text
    :caption: Example
    
    [date: Date, time
        [type, levtype
            [step, param]]]

Optional Keywords
^^^^^^^^^^^^^^^^^

Rules can declare keywords as optional with ``keyword?``. Optional keywords may
be omitted when archiving data and will be stored as empty values if not present.

TODO[kkratz]: How do optional keywords impact retrieval/listing?
TODO[kkratz]: Can this be combined with a local type declaration?

Matching on Keyword Values
^^^^^^^^^^^^^^^^^^^^^^^^^^

Rules can be further constraint to match only on specific values for keywords.
This can be defined with: ``keyword=val1/val2/val3``

.. code-block:: text
    :caption: Example
    
    [date, time, stream=enfo/efov
        [type, levtype
            [step, param]]]
    

Rule Grouping
^^^^^^^^^^^^^

Rules can be grouped to prevent repeating the same keywords for the database key or the co-location key. In the definition of the partitioning rule at the level of co-location and index keys multiple definitions of each are allowed

.. code-block:: text
    :caption: Example

    [a1, a2, a3
      [b1, b2, b3 
        [c1, c2, c3]]
      [bb1, bb2, bb3
        [cc1, cc2, cc3]]
      [bbb1, bbb2, bbb3
        [c1, c2, c3]
        [cc1, cc2, cc3]
        [ccc1,ccc2,ccc3]]
    ]

Keyword Types
=============

Keyword types describe how

.. list-table:: Types
    :widths: 25 75
    :header-rows: 1
    
    * - Type
      - Description
    * - ClimateDaily
      - 
    * - ClimateMonthly
      - 
    * - Date
      - 
    * - Default
      - 
    * - Double
      - 
    * - Expver
      - 
    * - First3
      - 
    * - Grid
      - 
    * - Ignore
      - 
    * - Integer
      - 
    * - Month
      - 
    * - MonthOfDate
      - 
    * - Param
      - 
    * - Step
      - 
    * - Time
      - 

Operational Considerations
==========================

TODO[kkratz]: Explain impacts of partition on operations, when to choose what
based on our experience.


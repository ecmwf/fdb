##############
Setting up FDB
##############

Seting up FDB requires the user to write a configuration and a schema file.

***************************
Writing a FDB Configuration
***************************

Config files define a number of parameters for the FDB.

The following is of the form local:

.. code-block:: yaml

  type: local
  engine: toc
  schema: ./schema
  spaces:
  - handler: Default
    roots:
    - path: /path/to/fdb/root

There a number of different types such as local, remote, distributed, and
select.

*Local* implements the passage of data from the frontend to storage backend, talk
to the FDB Store and Catalogue. Depending on the backend, the data or metadata
may not actually be local.

*Select* dispatches requests to different FDB's based on the metadata associated
with the Messages, and can be used to send split requests OD from RD.

Select Type:

.. code-block:: yaml

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

The *remote* type handles access to the remote FDB vis TCP/IP. It talks to the
FDB server using an asynchronous protocol. It only handles the transition. not
the distribution of data.

Remote type:

.. code-block:: yaml

  type: remote
  host: fdb-minus
  port: 36604

The *distributed* type implements the multi-lane access to multiple FDB's. It
uses rendezvous hashing to avoid synchronisations.

Dist type:

.. code-block:: yaml

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

***************
Database Schema
***************

The schema defines not only the valid field identifiers keys and values, but
also how the FDB will internally split the identifiers provided by the user
processes into sub-identifiers which control how the store backend partitions
data over inside the storage system.

Minimal Working Example
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

By default all keywords use the type 'String'. In most cases you will want to
set a specific type. Types can be set at the beginning of the file to be used
for all rules. Alternatively a keywords datatype can also be defined inside the
'partitioning rule' definition, see :ref:`Per Rule Keyword Datatype
<partition_rule_keyword_datatype>`.

Types define how input values are validated. For example for a keyword with the
type ``Double`` values that cannot be parsed as double values are rejected and
errors emitted.

.. code-block:: text
    :caption: Redfining datatypes for keywords

    param:     Param;
    step:      Step;
    date:      Date;
    latitude:  Double;
    longitude: Double;

Partitioning Rules
------------------

A Partitioning Rule defines how data is distributed inside FDB based on
keywords. Each rule is defined by three set of keys:

The first set of keywords form the *dataset key*. The *dataset key*
identifies the directory where the data is stored.

The second set of keywords form the *collocation key*. The *collocation key*
identifies the file inside the database directory where the data will be
stored.

The third set of keywords form the *element key*. The *element key* identifies the
offset in the file storing the data.

.. code-block:: text
    :caption: Example rule

    # 'date' and 'time' attributes form the database key
    [ date, time,
        # 'type' and 'levtype' form the co-location key
        [ type, levtype
            # 'step' and 'param' form the co-location key
            [ step, param ]]]

.. _partition_rule_keyword_datatype:

Each `Dataset Key` has to be unique, if you want to use a `Dataset Key` with
different `Collocation Keys` and/or `Element Keys` below you will need to use
:ref:`rule_grouping_label`.

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

In this example the data may contain the ``levelist`` keyword.

.. code-block:: text
    :caption: Example

    [stream, date, time
        [type, levtype
            [step, levellist?, param]]]

.. attention::

   To retrieve data matching all possible values including messages with
   omitted keyword you will need to write two requests. One request specifiying
   all possible values for the keayword and a second request with the keyword
   ommitted.

Matching on Keyword Values
^^^^^^^^^^^^^^^^^^^^^^^^^^

Rules can be further constrained to match only on specific values for keywords.
This can be defined with: ``keyword=val1/val2/val3``

.. code-block:: text
    :caption: Example

    [date, time, stream=enfo/efov
        [type, levtype
            [step, param]]]


.. _rule_grouping_label:

Rule Grouping
^^^^^^^^^^^^^

A ``Dataset keys`` can only appear once in rules, but there are use cases where
having different ``collocation keys`` under the same ``dataset key`` is
desired. This can be expressed by providing multiple ``collocation keys``
within the rule. This does not extend to ``element keys``, only one ``element
key`` may be specified on a ``collocation``.

.. code-block:: text
    :caption: Example

    [a1, a2, a3
      [b1, b2, b3
        [c1, c2, c3]]
      [bb1, bb2, bb3
        [cc1, cc2, cc3]]
      [bbb1, bbb2, bbb3
        [ccc1,ccc2,ccc3]]
    ]

Keyword Types
=============

Setting keyword type requires that all values can be parsed into this type.
Values that cannot be parsed into the defined type are considered an error.

Additionally types defined how the keyword values are represented in *dataset*,
*collocation* and *element* keys. While for some types multiple input
representations are accepted, all types use one canonical representation when
used to form a key.

.. important::

   Since types change how a key is formed specific types can be used to
   truncate values. A specific example for this is ``ClimateMonthly``
   which requires the value to be in date format and creates a three letter
   code for a month out of it, effectively truncating to date to just 'month'.

.. list-table:: Types
    :widths: 25 75
    :header-rows: 1

    * - Type
      - Description
    * - First3
      - Truncates values to the first 3 letters.
    * - ClimateDaily
      - Truncates dates to 'MMDD' with leading zeros. 
    * - ClimateMonthly
      - Represents months as 'M|MM', without leading zeros in the range [1-12].
    * - Date
      - Represents dates as 'YYYYMMDD'.
    * - Double
      - Represents values as double values.
    * - Expver
      - Represents values 4 characters padded with zeros.
    * - Grid
      - Encodes grid names to be safe to use in pathnames.
    * - Ignore
      - Always represent the values as empty string.
    * - Integer
      - Represents values as integers.
    * - Month
      - Represents month as 'M|MM' without leading zeros in the range [1-12].
    * - MonthOfDate
      - Represents month as 'MM' with leading zero in the range [01-12].
    * - Param
      - Represents param as paramid.
    * - Step
      - Represents steps as integers without leading zeros. 
    * - Time
      - Represents time as 'HHMM' with leading zeros.
    * - Year
      - Represents the year as digits without leading zeros.

Operational Considerations
==========================

The structure of the schema has a significant impact on the behaviour and performance of
the FDB. It is important to consider the type of data, how it will be written, how it
will be accessed, and what the lifetime and management of the data are.

Impact of Schema Configuration During Data Ingestion
----------------------------------------------------

The FDB is designed to support models outputting their data during runtime. Each process
writes data in a way which is as independent as possible from other writers.

There is a certain amount of overhead in creating and managing multiple different
datasets, and some overhead in managing multiple different sets of collocation keys,
within a single process.

As such, the optimal design places all output from a single run into one dataset,
and all output with compatible indexing into one set of collocation keys. The
keys whose values vary "quickly" in a model process should all be in the third
level of the schema.

Impact of Schema Configuration During Data Retrieval / Listing
--------------------------------------------------------------

The impact of schema configuration on data retrieval very much depends on read patterns.

Certain data access patterns access individual objects one-by-one. In this mode, there is
a meaningful overhead accessing a dataset for the first time. And a smaller overhead each
time an indexing unit corresponding to one set of collocation keys is opened for the first
time. Further individual accesses to the same datasets and collocation key tend to be
fast.

Queries which span many different datasets can be extremely costly and inefficient.
And if too many different datasets are accessed, the dataset overhead may have to be
repaid as the information may be expired from internal memory caches.

Note that this can come into conflict with the optimisation for write above. As a result,
the schema used for a secondary or archival copy of data (such as in the Destination
Earth data bridges) is likely to have keys reordered relative to that used for direct
output from model runs.

Forecast Data Example
---------------------

The primary characteristic of forecast runs is that there are discrete chunks of data produced
by a large number of processes in parallel. But these produce a well-defined, self-contained
dataset in which all the data are produced in a relatively narrow time window, in which the
bulk of data accesses to this data are occur at the same times and separate from access to
earlier or later forecast runs, and that the data are typically deleted together as the
forecast run becomes older and less valuable.

Note the existence of two axes of time. The keys date and time define the start time of the
forecast simulation, and the cutoff time for observations to be included in it. Progress
through the simulation is measured by the keyword step. And the combination of these keys
is required to obtain the real-time valid time for the resultant output. These are treated
very differently to each other.

The first axes of time is included in the first level of the schema. As a result, each forecast
run produces its own, self-contained dataset. The second axis of time is included in the third
level as a 'fast moving' key along side concepts such as the level in the atmosphere, and the
parameter being output.

The second level of the schema is used to partition data into discrete chunks of data which
are typically accessed together (e.g. model level data vs pressure level data), to align
the data files produced with the different output streams from the model, and to enable
indexing data with different metadata structures.

.. code-block:: text
    :caption: Example Configuration for Forecast Data
    
    # Default types
    channel:Integer;
    date:Date;
    diagnostic:Integer;
    direction:Integer;
    expver:Expver;
    fcmonth:Integer;
    frequency:Integer;
    grid:Grid;
    hdate:Date;
    ident:Integer;
    instrument:Integer;
    iteration:Integer;
    latitude:Double;
    levelist:Double;
    longitude:Double;
    method:Integer;
    number:Integer;
    param:Param;
    refdate:Date;
    step:Step;
    system:Integer;
    time:Time;

    [ class, expver, stream=enfo/efov/eefo, date, time, domain
           [ type, levtype=dp, product?, section?
                              [ step, number?, levelist?, latitude?, longitude?, range?, param ]]
           [ type=tu, levtype, reference
                   [ step, number, levelist?, param ]]
           [ type, levtype
                   [ step, quantile?, number?, levelist?, param ]]
    ]




Climate Reanalysis Data Example
-------------------------------

Climate reanalysis simulations are characteristically different to operational forecasts.
In particular, these data have only one axis of time - that is essentially valid time. A
long stretch of real time is split into chunks, which are output by individual runs of the
simulation. Each of these simulations provides output for *many* days.

Although the output could be grouped by the same first-level keys as for forecast output,
this would result in many different datasets being created (typically corresponding to
many folders), each containing relatively little data. Climate data are also frequently
accessed covering spans of many dates - and as such, the indexing structures generated
are also very poor for typical usage.

As such, we suggest a change to use a hierarchical scheme, where notably components of
date appear at multiple levels. The datasets are created per year of simulated output,
indexing structures per month, and then date itself is treated analagously to step
in typical forecast ouput (i.e. as a fast-changing key).

.. code-block:: text
    :caption: Example Configuration for Climate Data

    # Default types
    channel:Integer;
    date:Date;
    diagnostic:Integer;
    direction:Integer;
    expver:Expver;
    fcmonth:Integer;
    frequency:Integer;
    grid:Grid;
    hdate:Date;
    ident:Integer;
    instrument:Integer;
    iteration:Integer;
    latitude:Double;
    levelist:Double;
    longitude:Double;
    method:Integer;
    number:Integer;
    param:Param;
    refdate:Date;
    step:Step;
    system:Integer;
    time:Time;

    [ class=d1, dataset=climate-dt, activity, experiment, generation, model, realization, expver, stream=clte/wave, date: Year
           [ date: Month, resolution, type, levtype
                   [ date: Date, time, levelist?, param, frequency?, direction? ]]
    ]

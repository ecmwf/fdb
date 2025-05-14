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

Keyword Types
=============





Operational Considerations
==========================

TODO[kkratz]: Explain impacts of partition on operations, when to choose what based on our experience.


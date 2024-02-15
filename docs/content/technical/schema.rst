
Schema
======

The schema indexes structure and data collocation policy. It is used to uniquely describe the data that is being stored and indexed. 

The schema uses global attributes that describe the underlying data with each attribute having a name and datatype, these are added at the beginning of the file.

::

  param:     Param;
  step:      Step;
  date;      Date;
  latitude;  Double;
  longitude; Double;

The schema then describes rules for accessing all data stored by the FDB.

Each rule is described using three levels. The first level defines the attributes of the top level directory, the second level defines the attributes used to name the data files, and the third level attributes are used as index keys.
Example of a rule:

::

  [ class=ti/s2, expver, stream, date, time, model
    [ origin, type, levtype, hdate?
      [ step, number?, levelist?, param ]]
  ]

Rules can be grouped in the form:

::

  [a1, a2, a3 ...
    b1, b2, b3... [c1, c2, c3...]]
    B1, B2, B3... [C1, C2, C3...]]
  ]

A list of values can be given for an attribute.

::

  [ ..., stream=enfo/efov, ... ]

Attributes in rules can also be optional using the ? character.

::

  [ step, levelist?, param ]

Attributes can be removed using the - character.

::

  [grid-]

Rules are then matched if:
  * the attributes are present or marked optional
  * a list is provided, one of them matched

An example schema is provided schema_.

.. _schema: config-schema.html#schema

.. 
   _## TODO: add more info on the schema

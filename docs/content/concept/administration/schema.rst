The schema indexes structure and data collocation policy. It is used to uniquely 
describe the data that is being stored and indexed. 

The schema uses global attributes that describe the underlying data with each attribute having a name and datatype, these are added at the beginning of the file.

::

  param:        Param;
  step:         Step;
  date:         Date;
  latitude:     Double;
  longitude:    Double;

The schema then describes rules for accessing all data stored by the FDB.

Each rule is described using three levels. The first level defines the attributes 
of the top level directory, the second level defines the attributes used to name 
the data files, and the third level attributes are used as index keys. The levels
are subsets of the metadata describing the data. 

Example of a rule:

::

  [ class, expver, stream=oper/dcda/scda, date, time, domain?
       [ type, levtype
               [ step, levelist?, param ]]]

Take for example the following metadata:

::

  class         = od,
   expver       = 0001,
   stream       = oper,
   date         = 20240202,
   time         = 0000,
   domain       = g,
   type         = fc,
   levtype      = pl,
   step         = 1,
   levelist     = 150,
   param        = 130

With the rule from above, a message with the given metadata, retrieves the following key:

::

  {class=od,expver=0001,stream=oper,date=20240202,time=0000,domain=g}{type=fc,levtype=pl}{step=1,levelist=150,param=130}

As you can see, the three levels are represented in the final key, describing the data.
The individual sub-keys are:

.. code-block:: bash

  {class=od,expver=0001,stream=oper,date=20240202,time=0000,domain=g} # Dataset Key
  {type=fc,levtype=pl}                                                # Colloctation Key
  {step=1,levelist=150,param=130}                                     # Element Key


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

Example schema
**************

.. literalinclude:: administration/example-schema

.. 
   _## TODO: add more info on the schema

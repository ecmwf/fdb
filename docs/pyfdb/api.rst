
API
===========
The ``PyFDB`` API provides a Pythonic interface to ECMWF’s Fields DataBase (FDB).
It is designed to give Python users direct access to core ``FDB`` concepts
such as databases, archives, keys, and retrieval operations, while preserving
the performance characteristics of the native implementation. The API closely
mirrors the ``FDB`` data model, making it familiar to existing ``FDB`` users and
straightforward to integrate into Python-based workflows. This section
documents the main classes and functions available in ``PyFDB`` and explains how
they map to the underlying ``FDB`` components.

FDB
---
.. autoapiclass:: pyfdb.pyfdb.FDB  
   :members:

FDB - Auxiliary Objects
-------------
.. autoapiclass:: pyfdb.pyfdb.URI 
   :members:

.. autoapiclass:: pyfdb.pyfdb.DataHandle 
   :members:

.. autoapiclass:: pyfdb.pyfdb.MarsSelection 
   :members:

.. autoapiclass:: pyfdb.pyfdb.WildcardMarsSelection 
   :members:

.. autoapiclass:: pyfdb.pyfdb.ControlAction 
   :members:

.. autoapiclass:: pyfdb.pyfdb.ControlIdentifier 
   :members:

FDB - Iterator Objects
--------------------
.. autoapiclass:: pyfdb.pyfdb.ControlElement
    :members:

.. autoapiclass:: pyfdb.pyfdb.IndexAxis
    :members:

.. autoapiclass:: pyfdb.pyfdb.ListElement
    :members:
   
.. autoapiclass:: pyfdb.pyfdb.MoveElement
    :members:

.. autoapiclass:: pyfdb.pyfdb.PurgeElement
    :members:
   
.. autoapiclass:: pyfdb.pyfdb.StatsElement
    :members:

.. autoapiclass:: pyfdb.pyfdb.StatusElement
    :members:

.. autoapiclass:: pyfdb.pyfdb.WipeElement
    :members:

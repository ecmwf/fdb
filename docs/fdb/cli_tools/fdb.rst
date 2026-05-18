fdb
===

The ``fdb`` command is a wrapper that dispatches to the individual ``fdb-<command>`` tools
documented in this section. Rather than invoking each tool binary directly, you can use
``fdb <command>`` as a single entry point.

Usage
-----

.. code-block:: text

   fdb <command> [options] ...

Dispatching
^^^^^^^^^^^

``fdb <command>`` locates the corresponding ``fdb-<command>`` executable in the same
directory and forwards all remaining arguments to it. For example:

.. code-block:: bash

   fdb list class=od,stream=oper     # equivalent to: fdb-list class=od,stream=oper
   fdb write input.grib              # equivalent to: fdb-write input.grib

If ``<command>`` does not match a built-in or an installed ``fdb-<command>`` binary,
``fdb`` prints a usage summary and exits with an error.

Built-in Commands
^^^^^^^^^^^^^^^^^

A small number of commands are handled directly by the wrapper without dispatching to a
separate binary:

``fdb help``
   Print a usage summary listing available commands.

``fdb version``
   Print the FDB library version string.

``fdb home``
   Print the FDB home directory (queries ``fdb-info --home`` internally).

Example
-------

.. code-block:: bash

   % fdb list class=od,stream=oper,date=20151004
   {class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=700,param=155}
   {class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=129}
   {class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=130}
   ...

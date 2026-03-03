fdb
===

Description
-----------

A wrapper command for the tools described in this section (FDB5 Tools).

In general, the usage "fdb command arg1 arg2" will execute the tool fdb-command with arguments arg1 and arg2

Usage
-----

``fdb command [options]``

Example
-------

fdb list example
::
  
  % fdb list class="od",stream=oper,date=20151004
  {class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=700,param=155}
  {class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=129}
  {class=od,expver=0001,stream=oper,date=20151004,time=1200,domain=g}{type=an,levtype=pl}{step=0,levelist=850,param=130}
  ...

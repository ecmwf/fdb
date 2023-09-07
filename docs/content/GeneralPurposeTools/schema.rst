fdb schema
==========

Data is stored within the FDB5 according to a runtime specified schema. Print the schema that an instance of the FDB software is configured to use, the schema that was used to create a specific database or the schema contained in a specific schema file.
Usage
-----

``fdb schema``

``fdb schema path/to/schema``

Example 1
---------

Output the schema associated with the currently configured FDB:
::

  % fdb schema
  date:Date;
  ...
  [class=s2/ti,expver,stream,date,time,model[origin,type,levtype,hdate?[step,number?,levelist?,param]]]
  [class=ms,expver,stream,date,time,country=de[domain,type,levtype,dbase,rki,rty,ty[step,levelist?,param]]]
  ...

Example 2
---------

Parse and print the schema associated with a specific schema file. Useful for checking schema validity.
::
  
  % fdb schema /home/ma/fdbprod/etc/schema
  date:Date;
  ...
  [class=s2/ti,expver,stream,date,time,model[origin,type,levtype,hdate?[step,number?,levelist?,param]]]
  [class=ms,expver,stream,date,time,country=de[domain,type,levtype,dbase,rki,rty,ty[step,levelist?,param]]]
  ...
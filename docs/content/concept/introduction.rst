Introduction
============

Numerical weather prediction (NWP) and climate simulations are data heavy applications. 
Over the course of the last 40 years data output has increased by several orders 
of magnitude and are projected to continue. In 1995 ECMWF generated a total of
14 TiB per year, whereas the ensemble forecast output at the end of 2023 
totaled 60 TiB in just one hour.

The corresponding processing (archival, as well as archiving) of all output data,
are I/O intense operations, putting stress on all involved systems. Additionally weather 
forecasts decay in value rapidly after their creation (being superseded by newer
forecasts). There is a huge need to make this generated data available quickly and
cheaply for a general lifetime of the forecast data, which is typically 3-5 days.

The **Fields DataBase (FDB)** is a domain-specific object store developed at ECMWF for storing,
indexing and retrieving GRIB data, therefore playing the essential role of a hot-cache in
the context of NWP. 

The image below shows the **FDB** running in an operational forecast setting at ECMWF. The **Integrated
forecast system (IFS)** writes the output of ensemble forecasts to the FDB, leading to
approximately 1500 I/O nodes writing in parallel to the FDB file system. Numerous 
post-processing applications access the output of the IFS during the writing process to
enable the fastest-possible creation of corresponding products. 

The long-term storing of data into the **Meteorological Archiving and Retrieval System (MARS)**
is also triggered from the FDB side, resulting in an enormous I/O-induced stress on the file systems.

.. image:: /content/img/FDB_schema.png
   :width: 400
   :align: center
   :alt: Schematic overview of the FDB in a setup


Data in the **MARS** ecosystem consists out of concatenated **GRIB** files. **GRIB** files are collections
of self-contained records of 2D data without references to other records or an overall data schema. Therefore
a stream of valid **GRIB** messages is a **GRIB**-message itself.

.. image:: /content/img/Grib_msg.png
   :align: center
   :alt: Schematic of a stream of GRIB messages


Dealing with this amount of data in parallel poses a lot of questions, e.g. developing a description
which is small, as well as uniquely describing each data field in the entire ecosystem.

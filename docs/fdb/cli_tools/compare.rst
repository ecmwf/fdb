fdb-compare
=========

| Compares data of two different FDBs or different data version within one FDB.
| The comparison is accomplished in multiple steps. First all the index keys (typically MARS idefifiers) are compared. 
| Second, if all keys match, the messages itself are compared by header and data section.
| The message comparsion supports different methods, by default all keys are compared. 
| The data section can be compared given a numeric tolerance.

Usage
-----

``fdb-compare [options] [request1] [request2] ...``


Options
-------

+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--minimum-keys=string,string,...``   | Use these keywords as a minimum set which *must* be specified.                          |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--raw``                              | Don't apply (contextual) expansion and checking on requests.                            |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--ignore-errors``                    | Ignore errors (report them as warnings) and continue processing wherever possible.      |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--test-config=string``               | Path to a FDB config.                                                                   |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--reference-config=string``          | Path to a second FDB config.                                                            |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--reference-request=string``         | Specialized mars keys to request data from the reference FDB,                           |
|                                        | e.g. ``--reference-request="class=od,expver=abcd".``                                    |
|                                        | Allows comparing different MARS subtrees.                                               |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--test-request=string``              | Specialized mars keys to request data from the test FDB,                                |
|                                        | e.g. ``--test-request="class=rd,expver=1234"``.                                         |
|                                        | Allows comparing different MARS subtrees.                                               |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--scope=string``                     | Values: ``[mars (default)|header-only|all]``                                            |
|                                        | The FDBs can be compared in different scopes,                                           |
|                                        |  1) ``[mars]`` Mars Metadata only (default),                                            |
|                                        |  2) ``[header-only]`` includes Mars Key comparison                                      |
|                                        |     and the comparison of the data headers (e.g. grib headers)                          |
|                                        |  3) ``[all]`` includes Mars key and data header comparison but also                     |
|                                        |     the data sections up to a defined floating point tolerance.                         | 
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--grib-comparison-type=string``      | Values: ``[hash-keys|grib-keys(default)|bit-identical]``                                | 
|                                        | Comparing two Grib messages can be done via either (``grib-comparison-type=hash-keys``) |
|                                        | in a bit-identical way with hash-keys for each sections,                                |
|                                        | via grib keys in the reference FDB (``grib-comparison-type=grib-keys`` (default))       |
|                                        | or by directly comparing bit-identical memory segments                                  |
|                                        | (``grib-comparison-type=bit-identical``).                                               |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--fp-tolerance=real``                | Floating point tolerance for comparison default=machine tolerance epsilon.              |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--mars-keys-ignore=string``          | Format: ``Key1=Value1,Key2=Value2,...KeyN=ValueN``                                      |
|                                        | All Messages that contain any of the defined key value pairs will be omitted.           |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--grib-keys-select=string``          | Format: ``Key1,Key2,Key3...KeyN``                                                       |
|                                        | Only the specified grib keys will be compared                                           | 
|                                        | Only effective with ``grib-comparison-type=grib-keys``.                                 |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--grib-keys-ignore=string``          | Format: ``Key1,Key2,Key3...KeyN``                                                       | 
|                                        | The specified key words will be ignored.                                                |
|                                        | only effective with ``grib-comparison-type=grib-keys``.                                 | 
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--verbose``                          | Print additional output, including a progress bar for grib message comparisons.         |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--all``                              | Visit all FDB databases)                                                                |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--config=string``                    | FDB configuration filename)                                                             |
+----------------------------------------+-----------------------------------------------------------------------------------------+


Examples
--------

Compares two different fdbs for equality:
::
    % fdb-compare --reference-config=<path/to/reference/config.yaml> --test-config=<path/to/test/config.yaml --minimum-keys="" --all``


Compares different data versions within one fdb:
:: 
    % fdbcompare --config=test.yaml --reference-request="class=od,expver=abcd" --test-request="class=rd,expver=1234"  --grib-comparison-type="grib-keys" | tee out



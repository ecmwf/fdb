fdb-compare
===========

| Compares data of two different FDBs or different data version within one FDB.
| The comparison is accomplished in multiple steps. First all the index keys (typically MARS identifiers) are compared.
| Second, if all keys match, the messages itself are compared by header and data section.
| The message comparison supports different methods, by default all keys are compared.
| The data section can be compared given a numeric tolerance.

Usage
-----

Compares two entire FDBs for MARS id equality:
::
    % fdb-compare --config1=<path/to/config1.yaml> --config2=<path/to/config2.yaml> [options...]


Compare different data in the default (production) FDB:
::
    % fdb-compare --request1="..." --request2="..." [options...]


Options
-------

+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--config1=string``                   | Path to a FDB config. If not passed, FDB5 specific                                      |
|                                        | environment variables like FDB5_CONFIG_FILE will be evaluated. If ``--config1``         |
|                                        | is not provided, ``--request1`` must be provided.i                                      |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--config2=string``                   | Optional: Path to a second different FDB config. Default: Same as ``--config1``         |
|                                        | (if passed), otherwise FDB5 specific environment variables are evaluated.               |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--request1=string``                  | Optional: Specialized mars keys to request data from FDB1 or both FDBs                  |
|                                        | e.g. ``--request1="class=rd,expver=1234"``.                                             |
|                                        | Allows comparing different MARS subtrees if ``--request2`` is specified with            |
|                                        | different arguments.                                                                    |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--request2=string``                  | Optional: Specialized mars keys to request different data from the second FDB,          |
|                                        | e.g. ``--request2="class=od,expver=abcd".``                                             |
|                                        | Allows comparing different MARS subtrees. Only valid if ``--request1` has ben specified.| 
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--scope=string``                     | Optional - Values: ``[mars (default)|header-only|all]``                                 |
|                                        | The FDBs can be compared in different scopes,                                           |
|                                        |  1) ``[mars]`` Mars Metadata only (default),                                            |
|                                        |  2) ``[header-only]`` includes Mars Key comparison                                      |
|                                        |     and the comparison of the data headers (e.g. grib headers).                         |
|                                        |     Note: with ``grib-comparison-type=bit-identical``, the full message is              |
|                                        |     still compared byte-for-byte; ``header-only`` filtering only applies                |
|                                        |     to ``grib-keys`` comparison.                                                        |
|                                        |  3) ``[all]`` includes Mars key and data header comparison but also                     |
|                                        |     the data sections up to a defined floating point tolerance.                         |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--grib-comparison-type=string``      | Optional - Values: ``[hash-keys|grib-keys(default)|bit-identical]``                     |
|                                        | Comparing two Grib messages can be done via either (``grib-comparison-type=hash-keys``) |
|                                        | in a bit-identical way with hash-keys for each sections,                                |
|                                        | via grib keys in the message (``grib-comparison-type=grib-keys`` (default))             |
|                                        | or by directly comparing bit-identical memory segments                                  |
|                                        | (``grib-comparison-type=bit-identical``).                                               |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--fp-tolerance=real``                | Optional: Floating point tolerance for comparison (default=10.0*EPSILON).               |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--mars-keys-ignore=string``          | Optional: Format: ``Key1=Value1,Key2=Value2,...KeyN=ValueN``                            |
|                                        | All Messages that contain any of the defined key value pairs will be omitted.           |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--grib-keys-select=string``          | Optional: Format: ``Key1,Key2,Key3...KeyN``                                             |
|                                        | Only the specified grib keys will be compared.                                          |
|                                        | Only effective with ``grib-comparison-type=grib-keys``.                                 |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--grib-keys-ignore=string``          | Optional: Format: ``Key1,Key2,Key3...KeyN``                                             |
|                                        | The specified key words will be ignored.                                                |
|                                        | Only effective with ``grib-comparison-type=grib-keys``.                                 |
+----------------------------------------+-----------------------------------------------------------------------------------------+
| ``--verbose``                          | Optional: Print additional output,                                                      |
|                                        | including a progress bar for grib message comparisons                                   |
+----------------------------------------+-----------------------------------------------------------------------------------------+


Examples
--------

Compares two entire FDBs for equality. Using the `--scope=all` options, not only the headers but also the data is compared:
::
    % fdb-compare --config1=<path/to/config1.yaml> --config2=<path/to/config2.yaml> --scope=all

Compares a specific part (class=rd,expver=1234) of two different FDBs for equality:
::
    % fdb-compare --config1=<path/to/config1.yaml> --config2=<path/to/config2.yaml> --request1="class=rd,expver=1234"


Compares different data versions within one FDB using ``--request1`` and ``--request2``. In that case the only usable ``--grib-comparison-type`` is `grib-keys` (the default option):
::
    % fdb-compare --config1=<path/to/config1.yaml> --request1="class=od,expver=abcd" --request2="class=rd,expver=1234"  --grib-comparison-type="grib-keys" | tee out

Compares different data versions within the production FDB if executed on ATOS (the FDB5 config will be inferred from the environment):
::
    % fdb-compare --request1="class=od,expver=abcd" --request2="class=rd,expver=1234"  --grib-comparison-type="grib-keys" | tee out


Exit Codes
----------

The tool exits with code **0** on success (all compared entries match) and **non-zero** on failure
(any mismatch detected or an error occurred). This makes it suitable for use in scripts and CI pipelines.


Notes
-----

When comparing within a single FDB (using ``--config1`` is enough).
Minimum arguments are either ``--config1`` or ``--request1``. This is explicitly checked, 
otherwise the default behaviour would result in a comparison of the default FDB (prodfdb on ATOS) with itself.


fdb reconsolidate-toc
=====================

This is an advanced tool that exists to assist cleanup in a situation where databases have been poorly written. This can occur in a number of contexts such as:

    * Ensemble output has been written without the I/O server resulting in many, many index files.
    * Runtime flush and close configuration is incorrect.

This tool may be incomplete. In particular it may result in incomplete indexes if it is used in a context that has optional values in the schema.

In general it is preferable to wipe such databases, and rerun with correct experimental configuration.

Usage
-----
``fdb reconsolidate-toc [database path]``
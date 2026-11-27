.. _FDB_NFS:

FDB on NFS
==========

The TOC backend can keep its databases on an NFS mount that several clients write to. Appending to
a file is not atomic between NFS clients: two clients that append to the same TOC at the same time
can overwrite each other's records. On NFS the FDB therefore locks its TOC files.

There is nothing to switch on. The FDB asks the operating system for the type of the filesystem
holding each database, and locks the TOC where that type is ``nfs`` (on Linux, macOS and FreeBSD).
On every other filesystem nothing on this page applies.


What is locked
--------------

- **The TOC of a database and its sub-TOCs.** Each is locked as a whole with an advisory ``fcntl``
  lock: shared while it is read, exclusive while it is created or appended to. Open file
  description locks are used where the system has them, classic POSIX locks otherwise.
- **A reader** holds the lock while it copies the TOC into memory. With
  ``FDB_CACHE_TOCS_ON_READ=false`` it holds the lock for as long as it has the TOC open.
- **A writer** holds the lock for each append, from opening the TOC to closing it.
- **Handlers of the same TOC within one process** take turns, readers included.

Index and data files are not locked: each is written by a single process. On NFS their names, and
those of sub-TOCs, carry a random token in addition to the host name, the process id and the time,
so that two clients cannot choose the same name.

The locks are advisory. They keep FDB processes apart, not another program that writes to a TOC
without taking them.


What the mount has to provide
-----------------------------

- **Locks served by the NFS server.** NFSv4 mounts always have them. An NFSv3 mount needs the lock
  manager running on the server and the clients, and must not be mounted with ``nolock`` or
  ``local_lock=posix|all``: these options make the client grant the locks itself, so they no longer
  keep other clients out. The locking is tested with NFSv4.0 and NFSv4.1 mounts.
- **The same locking on every client.** A client running an FDB version without this locking takes
  no locks, and its appends can still collide with those of other clients. Upgrade all clients that
  write to a database before they share it.


Mounts with client-local locks
------------------------------

When a TOC is opened on a mount with client-local locks, for reading or for writing, the FDB
applies the policy given by ``nfsLocalLockPolicy``:

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Value
     - Behaviour
   * - ``error``
     - Throw an error, so no TOC is read or written on that mount. This is the default.
   * - ``allow``
     - Go ahead without locking between clients, logging a warning once per filesystem.

The key belongs to the configuration of the FDB that holds the database (see :doc:`config-schema`),
not to the :doc:`application config <app-config>`:

.. code-block:: yaml

   type: local
   engine: toc
   schema: ./schema
   nfsLocalLockPolicy: allow
   spaces:
   - handler: Default
     roots:
     - path: /path/on/nfs

The FDBs listed under ``fdbs`` in a ``select`` FDB do not inherit the key: set it on each of them
that needs it. The environment variable ``FDB_NFS_LOCAL_LOCK_POLICY`` (see
:doc:`environment-variables`) takes precedence over the configuration and applies to every FDB of
the process.

Client-local locks are detected on Linux only, from ``/proc/self/mountinfo``. On other systems the
policy has no effect.


Cost and limits
---------------

- **Readers and writers wait for each other.** A reader waits for a writer that is in the middle of
  an append, and a writer waits for the readers and the writer that hold the TOC.
- **There is no timeout.** A process waits until it is granted the lock. If the holder is stuck,
  every reader and writer of that TOC waits with it, on all clients, until the holder ends or, with
  NFSv4, the server gives up on a client it has lost contact with.
- **Sub-TOCs are read one after the other**, each under its own lock, instead of being loaded
  together ahead of the walk as on other filesystems. Listing and retrieving from a database with
  many sub-TOCs takes correspondingly longer.
- **A TOC is fetched from the server each time it is read**: the Linux NFS client drops what it has
  cached of a file when it locks it.

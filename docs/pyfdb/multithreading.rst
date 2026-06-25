.. _multithreading_label:

Multithreading
##############

Thread safety in PyFDB depends on two independent factors that must both be
considered:

1. **Whether threads share one** ``FDB`` **instance or each own a separate one.**
2. **Whether the FDB configuration targets a local or a remote store.**

The combination gives four distinct cases with very different safety profiles.

.. note::

    **For a local config, giving each thread its own** ``FDB`` **instance is
    fully safe and requires no extra care.** This is the recommended pattern.
    For a remote config, read rule 2 below before adopting this approach.


Quick reference
***************

**Per-thread** ``FDB`` **instances** (each thread creates and owns its own ``FDB``):

.. list-table::
   :header-rows: 1
   :widths: 45 27 28

   * - Operation
     - Per-thread, local config
     - Per-thread, remote config
   * - ``archive()``
     - Safe
     - Safe
   * - ``flush()``
     - Safe
     - Safe
   * - ``dirty()``
     - Safe
     - Safe
   * - ``retrieve()`` / ``inspect()``
     - Safe
     - Safe\ :sup:`†`
   * - ``list()`` / ``axes()`` / ``stats()`` / ``wipe()``
     - Safe
     - Safe\ :sup:`†`
   * - Reading a ``DataHandle`` on a different thread
     - Not supported
     - Not supported

:sup:`†` Safe when each thread uses its own instance sequentially. See rule 2.

**Shared** ``FDB`` **instance** (one ``FDB`` used by multiple threads simultaneously):

.. list-table::
   :header-rows: 1
   :widths: 45 27 28

   * - Operation
     - Shared instance, local config
     - Shared instance, remote config
   * - ``archive()``
     - Not safe
     - Not safe
   * - ``flush()``
     - Not safe
     - Not safe
   * - ``dirty()``
     - Not safe
     - Not safe
   * - ``retrieve()`` / ``inspect()``
     - Not safe
     - Not safe
   * - ``list()`` / ``axes()`` / ``stats()`` / ``wipe()``
     - Not safe
     - Not safe
   * - Reading a ``DataHandle`` on a different thread
     - Not supported
     - Not supported


Best-practice guide
*******************

1. **Per-thread FDB, local config — fully safe, no extra care needed.**

   Each thread creates and owns its own ``FDB`` object. All internal state is
   private to that instance and no shared mutable state exists between threads
   on the local path.

   .. code-block:: python

       import threading
       import pyfdb

       def worker(selection):
           fdb = pyfdb.FDB()          # local config; one instance per thread
           for element in fdb.list(selection):
               ...

       threads = [threading.Thread(target=worker, args=(sel,)) for sel in selections]
       for t in threads:
           t.start()
       for t in threads:
           t.join()

   If multiple per-thread instances all point at the **same FDB root directory**,
   they coordinate access to the shared on-disk catalogue through file-level
   locking — the same mechanism used for multi-process access. This is safe but
   may limit write throughput when many threads archive to the same location
   simultaneously.

2. **Per-thread FDB, remote config — safe when calls are sequential per instance.**

   Each remote ``FDB`` instance maintains an internal background connection
   thread. This background thread and the application thread that owns the
   instance share some internal state. In the current implementation that
   sharing is not fully synchronised, so issuing concurrent calls on the
   *same* instance from multiple application threads is unsafe.

   With a per-thread pattern each application thread is the sole caller on its
   own instance, so this internal overlap is not a concern in practice.

   .. code-block:: python

       import pyfdb

       def worker(selection):
           fdb = pyfdb.FDB(remote_config)   # remote config; one instance per thread
           # Make calls sequentially on this instance — do not share it.
           elements = list(fdb.list(selection))
           ...

3. **Shared FDB, local config — serialise every call.**

   When one ``FDB`` instance is used by multiple threads at the same time, all
   operations are currently unsafe due to unsynchronised internal state. This
   affects every method: write-side operations (``archive()``, ``flush()``,
   ``dirty()``), read-side operations (``retrieve()``, ``inspect()``), and
   query operations (``list()``, ``axes()``, ``stats()``, ``wipe()``).

   Wrap every call in a single shared lock:

   .. code-block:: python

       import threading
       import pyfdb

       fdb = pyfdb.FDB()          # local config, shared across threads
       fdb_lock = threading.Lock()

       def writer(key, data):
           with fdb_lock:
               fdb.archive(key, data)
               fdb.flush()

       def reader(selection):
           with fdb_lock:
               return list(fdb.list(selection))

4. **Shared FDB, remote config — serialise every call.**

   All the same shared-local restrictions apply, and the unsynchronised
   background connection thread makes the situation strictly worse: concurrent
   calls from multiple application threads compound the internal contention.
   A single lock covering every call is required:

   .. code-block:: python

       import threading
       import pyfdb

       fdb = pyfdb.FDB(remote_config)    # remote config, shared across threads
       fdb_lock = threading.Lock()

       def worker(selection):
           with fdb_lock:
               elements = list(fdb.list(selection))
           ...

5. **Never pass a** ``DataHandle`` **between threads.**

   A ``DataHandle`` returned by ``retrieve()`` must be opened, read, and closed
   on the thread that created it. Handing it off to another thread is not
   supported.

   .. code-block:: python

       def worker(selection):
           fdb = pyfdb.FDB()
           with fdb.retrieve(selection) as data_handle:
               payload = data_handle.readall()
           # Safe: the handle never leaves this thread.


Why per-thread FDB behaves differently for local and remote
***********************************************************

For a **local** config, all currently known synchronisation gaps are in state
that belongs exclusively to a single ``FDB`` instance. Give each thread its own
instance and there is nothing to race on. This is a clean separation: no shared
mutable state, no locks needed.

For a **remote** config the picture is different. Every remote ``FDB`` instance
opens an internal background thread to receive responses from the server. That
background thread and the calling application thread share internal bookkeeping
about which requests are in flight. This shared bookkeeping is not yet fully
synchronised, which means even a single instance used by a single application
thread is technically operating with two threads (the application thread and its
background receiver) touching the same data. Per-thread ``FDB`` prevents
*application threads* from sharing state, but cannot eliminate the instance's
own internal concurrency.


Current limitations and outlook
********************************

The known synchronisation gaps are tracked and will be addressed in upcoming
releases. Once the fixes land, the restrictions on shared instances for local
configs are expected to be lifted and the remote config story simplified.

In the meantime:

- For a **local** config, a per-thread ``FDB`` is the zero-friction solution.
- For a **remote** config, a per-thread ``FDB`` with sequential per-instance
  calls is the safest readily available approach.
- When sharing is unavoidable, a single ``threading.Lock()`` guarding every
  call is both correct and straightforward to add.

For further technical detail see :file:`tests/pyfdb/integration/test_threading.py`.

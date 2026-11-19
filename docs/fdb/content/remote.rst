.. _FDB_Remote:

Remote FDB
==========

How a client talks to remote FDB servers. The code lives in ``src/fdb5/remote``:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Path
     - Role
   * - ``Messages.h``, ``Connection.*``
     - wire format, framed read/write on a socket pair
   * - ``RemoteConfiguration.*``
     - capability negotiation at start-up
   * - ``client/``
     - ``Client``, ``ClientConnection``, ``ClientConnectionRouter``, ``RemoteStore``, ``RemoteCatalogue``, ``ReadLimiter``
   * - ``server/``
     - ``ServerConnection`` (base), ``CatalogueHandler``, ``StoreHandler``
   * - ``FdbServer.*``
     - accept loop, one forked process (or thread) per connection
   * - ``../api/RemoteFDB.*``
     - ``FDB`` backend that forwards API calls (list, axes, inspect, stats, wipe)

.. note::

   The diagrams use the ``sphinxcontrib-mermaid`` extension (``extensions = ["sphinxcontrib.mermaid"]`` in ``conf.py``).


Components
----------

A remote setup has two kinds of servers, selected by ``type:`` in the server configuration:

* **catalogue server** - serves the schema, indexes and ``list``/``axes``/``inspect``/``stats``/``wipe``;
* **store server** - stores and serves the field data.

.. mermaid::

   flowchart LR
       subgraph Client process
           FDB[FDB API]
           RFDB[RemoteFDB<br/>list / axes / inspect / stats / wipe]
           RC[RemoteCatalogue<br/>archive / flush]
           RS[RemoteStore<br/>archive / read / flush]
           Router[ClientConnectionRouter<br/>one ClientConnection per endpoint]
           CC1[ClientConnection<br/>to catalogue]
           CC2[ClientConnection<br/>to store]
           FDB --> RFDB
           FDB --> RC
           FDB --> RS
           RFDB --> Router
           RC --> Router
           RS --> Router
           Router --> CC1
           Router --> CC2
       end

       subgraph Catalogue host
           CS[fdb-server<br/>accept loop]
           CH[CatalogueHandler<br/>one forked process per connection]
           LC[(local Catalogue)]
           CS -- fork --> CH --> LC
       end

       subgraph Store host
           SS[fdb-server<br/>accept loop]
           SH[StoreHandler<br/>one forked process per connection]
           LS[(local Store)]
           SS -- fork --> SH --> LS
       end

       CC1 <-->|control + data sockets| CS
       CC2 <-->|control + data sockets| SS

Key points:

* Every ``Client`` (``RemoteFDB``, ``RemoteCatalogue``, ``RemoteStore``) gets a process-unique ``clientID``. Many
  clients **share one** ``ClientConnection`` per endpoint (the router hands out the existing valid connection), so
  messages are multiplexed by ``clientID`` and ``requestID``.
* The server creates one handler per TCP connection (``serverThreaded: true`` uses a thread instead of a fork).
  Handler state (``fdbs_``, ``catalogues_``, ``stores_``) is keyed by ``clientID``.
* ``RemoteFDB`` asks the catalogue server for ``Stores`` (store endpoints, mapped per client network via
  ``networks``/netmask) and the ``Schema``. ``RemoteCatalogue`` and ``RemoteStore`` then open their own connections.


Wire format
-----------

Every message is ``header | payload | "EFDB"``:

.. list-table::
   :header-rows: 1
   :widths: 20 10 70

   * - Field
     - Bytes
     - Notes
   * - marker
     - 4
     - ``"SFDB"``
   * - version
     - 2
     - ``MessageHeader::currentVersion``
   * - message
     - 2
     - ``Message`` enum
   * - clientID
     - 4
     - ``(clientID << 1) | controlBit``
   * - requestID
     - 4
     - unique per connection, never reused
   * - payloadSize
     - 4
     -
   * - hash
     - 16
     -

* The lowest bit of the ``clientID`` field says whether the message belongs to the **control** (1) or **data** (0)
  channel. ``clientID == 0`` is reserved for connection-level messages (``Startup``, ``Exit``).
* Message groups: server instructions (``Startup``, ``Error``, ``Stores``, ``Schema``, ``Stop``, ``Exit``), API calls
  (``Flush``, ``Archive``, ``List``, ``Read``, ``Store``, ``Wipe``, ...), wipe steps (``DoWipe*``,
  ``DoMaskIndexEntries``), responses (``Received``, ``Complete``, ``Unauthorised``) and payloads (``Blob``,
  ``MultiBlob``).


Connection modes and start-up
-----------------------------

Two modes, chosen by negotiation (``supportedConnections``, ``preferSingleConnection``):

* **dual connection** - a *control* socket for blocking request/response and a separate *data* socket for
  asynchronous bulk traffic;
* **single connection** - everything on the control socket (``singleConnection``).

.. mermaid::

   sequenceDiagram
       participant C as ClientConnection
       participant S as ServerConnection

       C->>S: connect (control)
       C->>S: Startup {sessionID, endpoint, protocol version, RemoteConfiguration}
       Note over S: check protocol version,<br/>RemoteConfiguration::common(client, server)
       alt dual connection
           Note over S: open ephemeral data port
           S->>C: Startup {clientSession, serverSession, dataEndpoint, agreedConf}
           C->>S: connect (data)
           C->>S: Startup {clientSession, serverSession} on data
           Note over S: verify both session IDs
       else single connection
           S->>C: Startup {..., agreedConf}
       end
       Note over C: start control listener thread<br/>(+ data listener thread if dual)

``agreedConf`` contains the common ``RemoteFieldLocation`` version, the number of connections, and
``enabledFeatures`` = client mask AND server mask. A message outside that mask is normally refused by the client
before it is sent (``checkEnabled`` throws a ``UserError``: "'X' is not permitted on FDB server host:port: the
feature is disabled in the client or server configuration"). The reply of the server, ``Unauthorised`` ("'X' is
disabled in the configuration of FDB server host:port"), is only a second line of defence. Only ``Wipe`` can be
disabled (``wipe: false``), server-wide, not per user or network.


Control versus data
-------------------

.. mermaid::

   flowchart TB
       subgraph Control channel
           direction TB
           A[Client sends request<br/>with new requestID]
           B[ClientConnection stores a promise<br/>under requestID]
           C[Server replies Received / Error / Unauthorised<br/>with same requestID]
           D[Control listener completes the promise]
           A --> B --> C --> D
       end
       subgraph Data channel
           direction TB
           E[Server worker streams Blob ... Complete<br/>or a single Error]
           F[Data listener dispatches by clientID<br/>to Client::handle]
           G[Client queue: iterator / DataHandle / Locations]
           E --> F --> G
       end

* **Control** requests are *blocking*: ``Client::controlWriteCheckResponse`` / ``controlWriteReadResponse`` send the
  message and wait on the promise. One blocking request at a time per ``Client`` (``blockingRequestMutex_``);
  different clients run concurrently.
* **Data** traffic is *asynchronous*: results are pushed by the server into per-request queues on the client.
  In single-connection mode the same flow runs over the control socket; the listener first looks for a promise
  with that ``requestID``, and otherwise forwards to the client.


Server threading
----------------

.. mermaid::

   flowchart LR
       subgraph ServerConnection
           H[control thread<br/>handle loop]
           D[data listener thread<br/>started on first Archive/Store/Read]
           A[archive worker<br/>archiveThreadLoop]
           R[read worker<br/>StoreHandler only]
           W[API workers<br/>one per list/axes/inspect/stats/wipe request]
       end
       H -- Archive / Store --> A
       D -- Blob / MultiBlob --> Q[(archiveQueue_)] --> A
       H -- Read --> RQ[(readLocationQueue_)] --> R
       H -- List / Axes / ... --> W

The control thread reads one message at a time and calls ``handleControl``; the handler returns a ``Handled``
value:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - ``Handled``
     - Server then...
   * - ``Replied``
     - does nothing, the handler already answered
   * - ``Yes``
     - sends ``Received``
   * - ``YesAddArchiveListener``
     - counts a data listener (starts the data thread in dual mode), sends ``Received``
   * - ``YesAddReadListener``
     - counts a data listener; ``Received`` was already sent by ``StoreHandler::read``
   * - ``No``
     - sends ``Error "Unable to handle message"``

* Every handler catches ``std::exception`` and ``...`` and turns them into an ``Error`` message for the request.
  The API and read workers do the same on their own threads, on a best-effort basis (``Connection::tryError()``
  never throws: if the connection is already down there is nobody left to tell).
* The archive worker and the data listener are shared by all the requests of the connection. If one of them fails,
  the whole connection is aborted rather than reporting a single request (see *Error and failure propagation*).
* ``Stop`` (per client) removes that client's state; ``Exit`` ends the connection gracefully.
* ``handle()`` never lets an exception escape. On every exit path it stops and joins all the workers
  (``stopWorkers()``) while the handler state they use is still alive; the derived destructors do the same.


Request flows
-------------

list / axes / inspect / stats / wipe (RemoteFDB)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. mermaid::

   sequenceDiagram
       participant A as RemoteFDB (API thread)
       participant L as ClientConnection listeners
       participant S as CatalogueHandler
       participant W as API worker

       A->>A: create message queue for requestID
       A->>S: List {request, depth} (control)
       S->>W: start worker thread
       S->>A: Received (control, completes promise)
       loop each element
           W->>L: Blob {ListElement} (data)
           L->>A: queue.emplace
       end
       W->>L: Complete (data)
       L->>A: queue.close -> iterator ends

If the worker fails, it sends ``Error`` on the data channel; ``RemoteFDB::handle`` interrupts the queue and the
iterator throws ``RemoteFDBException``.

archive
~~~~~~~

An archive touches both servers: the store keeps the bytes, the catalogue indexes the returned location.

.. mermaid::

   sequenceDiagram
       participant U as FDB::archive
       participant RS as RemoteStore
       participant SS as StoreHandler
       participant RC as RemoteCatalogue
       participant CS as CatalogueHandler

       U->>RS: archiveCb(key, data)
       opt first field of this flush cycle
           RS->>SS: Store (control)
           SS->>RS: Received
       end
       RS->>RS: remember callback for requestID (Locations)
       RS-->>SS: Blob {dbKey, idxKey, data} (data write queue)
       SS->>SS: archive worker: Store::archive
       SS->>RS: Store {FieldLocation} (control, same requestID)
       RS->>RC: callback: catalogue archive(location)
       opt first field
           RC->>CS: Archive (control)
           CS->>RC: Received
       end
       RC-->>CS: Blob {idxKey, datumKey, location} (data write queue)
       CS->>CS: archive worker: Catalogue::archive

Flush makes the above durable:

.. mermaid::

   sequenceDiagram
       participant RS as RemoteStore
       participant SS as StoreHandler
       participant RC as RemoteCatalogue
       participant CS as CatalogueHandler

       RS->>RS: wait until all locations came back (Locations::wait)
       RS->>SS: Flush {n} (control)
       SS->>SS: Store::flush
       SS->>RS: Received
       RC->>RC: flushDataWrites (barrier: all Blobs written first)
       RC->>CS: Flush {n} (control)
       CS->>CS: wait until n locations archived, Catalogue::flush
       CS->>RC: Received

Notes:

* Blobs are queued client-side (``dataWriteQueue_``, ``fdbDataWriteQueueLength``, default 320) and server-side
  (``fdbServerMaxQueueSize``, default 320), which gives back-pressure.
* The catalogue ``Flush`` must not overtake its ``Blob`` messages, hence the ``flushDataWrites()`` barrier (a single
  connection would otherwise deadlock the server).
* The server checks for a failure of its archive worker before flushing, and the catalogue flush stops waiting for
  locations if the worker failed or the connection was aborted. On the client, ``Locations::complete()`` throws
  if the archival failed, so ``flush()`` never proceeds as if all the locations had arrived.

read (retrieve)
~~~~~~~~~~~~~~~

.. mermaid::

   sequenceDiagram
       participant U as DataHandle::read
       participant RL as ReadLimiter
       participant RS as RemoteStore
       participant SS as StoreHandler
       participant RW as read worker

       U->>RS: dataHandle(location) -> new requestID + queue
       RS->>RL: add(request)
       Note over RL: sends when memoryUsed + size <= fdbReadLimit (1 GiB)
       RL->>SS: Read {location, remapKey} (control)
       SS->>RS: Received
       SS->>RW: readLocationQueue_
       loop chunks (about 4 MiB)
           RW->>RS: Blob (data)
           RS->>U: queue -> bytes
       end
       RW->>RS: Complete (data)
       RS->>RL: finishRequest (frees budget, may send next)

A ``FDBRemoteDataHandle`` must be read **in order**: there is no per-request back-pressure, so reading out of order
could deadlock.


Error and failure propagation
-----------------------------

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Situation
     - What the client sees
   * - Error in a blocking request (control)
     - the promise gets a ``RemoteFDBException``
   * - Feature disabled by configuration (e.g. wipe)
     - ``UserError`` "'X' is not permitted on FDB server host:port: the feature is disabled in the client or server
       configuration", thrown by the client before sending; the ``Unauthorised`` reply of the server ("'X' is
       disabled in the configuration of FDB server ...") reaches the client as a ``UserError`` "Unauthorised X
       request - ..."
   * - Error in a list/axes/... worker, or a read
     - the queue of the request is interrupted with ``RemoteFDBException``; the iterator or ``DataHandle::read``
       throws
   * - ``Error`` for a request that no client handler recognises, or with ``clientID == 0``
     - treated as a connection failure (see next row)
   * - Connection lost, or a listener hits an exception
     - ``handleConnectionError``: all pending promises fail, ``Client::closeConnection()`` interrupts queues and
       pending archives of every client, the connection is torn down
   * - Error in an archive blob (the archive worker dies on the server)
     - the failure and the IDs of the failing request are recorded and logged. An ``Error`` with the actual reason
       is sent for the failing request, on a best-effort basis (it is skipped if another thread is writing on the
       control socket), then the whole connection is aborted without waiting for a socket writer (the worker is
       shared, so no other pending request could ever get its location). ``RemoteStore`` fails the ``Locations`` of
       the failing request with the reason of the server, so ``flush()`` throws it; if the ``Error`` is skipped, or
       for the other pending requests, the client sees a lost connection (see the row above). A ``Flush`` still being
       handled by the server rethrows the recorded failure
   * - Data listener fails on the server (bad framing, transport error)
     - the stream is out of sync, so the connection is aborted as above, after a best-effort ``Error`` with
       ``clientID == 0`` carrying the reason (the client reports it as a connection failure)
   * - Failure in the server connection handler (e.g. protocol violation on the control connection)
     - the failure is logged and the connection is aborted, after the same best-effort ``Error`` with
       ``clientID == 0``

Invariants worth keeping:

* Never rethrow from a server or client worker thread (that terminates the process); report the error to the peer
  or to the owning queue instead, or, if the worker is shared (archive worker, data listener), abort the connection.
* A failed server connection is aborted (``Connection::abortConnection()``): both sockets are shut down, without
  an ``Exit`` message, which releases every thread blocked on them. ``Exit`` (``teardown()``) is only for a graceful
  end. All workers are stopped and joined in ``ServerConnection::handle()`` (and in the derived destructors) while
  the handler state they use is still alive.
* ``Locations::complete()`` checks the failure and the completion under one lock and throws on failure: a failed
  archival is never reported as complete. Connection loss is also remembered before a new archive is registered.
  Explicit flush failures remain errors; destructor flush failures are logged without throwing.
* Every error sent by the server must carry the ``clientID`` and ``requestID`` of the request it belongs to; an
  ``Error`` without them cannot be attributed and takes the whole connection down.


Configuration reference
-----------------------

Server (``type: catalogue | store``):

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Key
     - Meaning
   * - ``serverPort``
     - listening port (default 7654)
   * - ``serverThreaded``
     - thread instead of fork per connection
   * - ``supportedConnections``
     - ``[1]``, ``[2]`` or ``[1, 2]``
   * - ``preferSingleConnection``
     - tie-breaker when both modes are possible (client side)
   * - ``dataPortStart``, ``dataPortCount``
     - restrict the ephemeral data ports
   * - ``wipe``
     - set to ``false`` to disable wipe
   * - ``stores``, ``networks``
     - catalogue server: store endpoints, optionally per client network

Resources: ``fdbReadLimit`` / ``$FDB_READ_LIMIT``, ``fdbDataWriteQueueLength``, ``fdbServerMaxQueueSize``,
``fdbRetrieveQueueSize``, ``fdbMaxConnectRetries``, ``fdbConnectTimeout``.


Operational note
----------------

``fdb-server`` opens its listening socket with ``SO_REUSEPORT``. A leftover server on the same port is not rejected;
the kernel then spreads new connections between old and new servers, which shows up as random hangs or failures
(typically in tests). Kill stale ``fdb-server`` processes before debugging connection problems.

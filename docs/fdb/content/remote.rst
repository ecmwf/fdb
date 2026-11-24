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
     - reserved: never set nor checked (hashing every payload, up to the size of a field, would be expensive)

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
``enabledFeatures`` = client mask AND server mask. Only ``Wipe`` can be disabled: by the client or the server
configuration (``wipe: false``), or, on the server, for the clients of some networks only (``networks``, see the
configuration reference below; the server works out the network of the client from its address, and withdraws the
feature from the mask it offers to that client).

A message outside the agreed mask is refused by the client before it is sent: ``checkEnabled`` throws a
``UserError``, which says which side does not allow it, as the client remembers the mask it offered:

* "'X' is disabled in the client configuration (FDB server host:port)"
* "'X' is not permitted on FDB server host:port: the server does not allow it for this client"

The reply of the server, ``Unauthorised`` ("'X' is not permitted for this client on FDB server host:port"), is only a
second line of defence.


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
   * - Feature disabled by configuration (e.g. wipe, also per client network)
     - ``UserError`` thrown by the client before sending, saying whether the client ("'X' is disabled in the client
       configuration ...") or the server ("'X' is not permitted on FDB server ...: the server does not allow it for
       this client") does not allow it; the ``Unauthorised`` reply of the server ("'X' is not permitted for this
       client on FDB server ...") reaches the client as a ``UserError`` "Unauthorised X request - ..."
   * - Error in a list/axes/... worker, or a read
     - the queue of the request is interrupted with ``RemoteFDBException``; the iterator or ``DataHandle::read``
       throws
   * - ``Error`` for a request that no client handler recognises, or with ``clientID == 0``
     - treated as a connection failure (see next row)
   * - Connection lost, or a listener hits an exception
     - ``handleConnectionError``: all pending promises fail, ``Client::closeConnection(reason)`` interrupts queues
       and pending archives of every client, the connection is torn down. The reason (e.g. the text of an ``Error``
       with ``clientID == 0``) is passed on to all of them, so that a read or an archive waiting after its request
       was acknowledged still sees why the connection was lost ("Unexpected closure of store: <reason>"). In
       dual-connection mode the data listener first waits (at most 2 s) for the control listener to stop: a server
       that aborts a connection sends the reason on the control socket, and it must be processed before the generic
       failure, which would otherwise win
   * - Error in an archive blob (the archive worker dies on the server)
     - the failure and the IDs of the failing request are recorded and logged. The worker owns the reporting of
       the failure (``ServerConnection::failConnection()``): it sends an ``Error`` with the actual reason for the
       failing request, on a best-effort basis (it waits at most 200 ms for another writer of the control socket,
       and at most 200 ms for room in the send buffer of the peer), then aborts the whole connection (the worker is
       shared, so no other pending request could ever get its location), and only then interrupts the archive
       queue. The threads woken up by the interruption or by the abort (data listener, control listener on a single
       connection) see that the failure is owned and neither send their own error nor abort the sockets first.
       On the client, ``RemoteStore`` fails the ``Locations`` of the failing request with the reason of the server,
       and ``RemoteCatalogue`` retains it, so ``flush()`` throws it, also when it is called after the connection
       was lost; ``archive()`` fails fast with it. If the ``Error`` could not be sent, or for the other pending
       requests, the client sees a lost connection (see the row above). A ``Flush`` still being handled by the
       server rethrows the recorded failure
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
* A client is never called with ``ClientConnection::clientsMutex_`` (or its own request-map mutex) held: a listener
  may block in a client on the full queue of a slow reader, and ``closeConnection()`` must still be able to interrupt
  every queue. A client being called is pinned instead, so that ``remove()`` (i.e. its destruction) waits for the call
  to complete. A client being removed no longer receives messages, but is still told if the connection is lost; if
  one of its calls is in progress, its pending requests are cancelled first, as nobody will read them any more. A
  message for a cancelled (interrupted) request queue is discarded: it is not a failure of the connection, which keeps
  serving the other clients. Request queues are unregistered only after the last message has been queued.
* A failed archive cycle of ``RemoteCatalogue`` is not retried: its first failure (the archive error of the server,
  else the reason the connection was lost, else the failure of the flush) is recorded, and any later ``flush()`` or
  ``archive()`` rethrows it, before checking the counts.
* Input from the network is never checked with ``ASSERT``: framing, versions, sizes and MultiBlob frames are
  validated, and a violation raises ``RemoteProtocolException``, which fails the connection, not the process. Until the
  handshake has completed the server accepts payloads of at most 64 KiB (afterwards ``fdbServerMaxPayloadSize``,
  unlimited by default). ``Flush`` is a control message: on the data connection it is a protocol error.
* A blocking request is either answered, failed with the pending requests, or rejected: once the pending requests have
  been failed (connection lost, or ``Exit`` from the server), no new one is registered.
* ``ReadLimiter`` never throws on behalf of another request: releasing a read (also from the destructor of its
  ``DataHandle``) removes it whether it is queued or sent, and a read that cannot be sent releases its budget and fails
  its own handle (``RemoteStore::failRequest``). An abandoned handle interrupts its queue, so that the rest of its
  stream is discarded.
* The queues of the streamed requests of a client (reads, list, axes, ...) are kept in a ``RequestQueues``: a request
  is registered before it is sent, and unregistered if it cannot be sent, by its last message (``Complete`` or
  ``Error``), or when the connection is lost (``failAll``), so that no entry outlives its connection (request IDs
  restart on a new one). A consumer that abandons a request (a ``DataHandle`` or an iterator destroyed early)
  cancels it: the rest of its stream is discarded, and the listener of the connection is never blocked by it.
* A message for a client that has been removed is discarded, by the client connection and by the server (a client
  may go away, e.g. without flushing, while the server or the client is still sending for it).
* No socket I/O and no connection attempt with a registry mutex held (``ClientConnection::clientsMutex_``,
  ``ClientConnectionRouter::connectionMutex_``): a slow or unresponsive server must not block the other connections.
  Once its last client is gone a connection is retired (``usable()`` is false) and torn down: it is never handed out
  again. Connections are shared by endpoint *and* default endpoint, which is used to build the field locations.
* Failover between endpoints (e.g. the stores of an archiving ``RemoteStore``) tries all of them. If every one
  explicitly rejects the connection (``RemoteFDBRejected``: the server answered the startup message with an error),
  the first rejection is rethrown with its reason, and is not marked worth retrying (a protocol or configuration
  mismatch will not go away). If some could not be reached, did not answer in time (``RemoteFDBTimeout``, retryable)
  or failed otherwise, the ``ConnectionError`` stays retryable, and carries the reasons.
* A catalogue flush waits for its field locations only while some can still arrive: once the data connection has
  ended (or on a single connection, whose only reader is handling the flush) and the locations already queued have
  been archived, a count still short fails the flush instead of waiting forever. The archive worker publishes the
  failure of an element before counting it as processed, so a flush that sees no pending work reports that failure,
  not missing locations.
* Nothing waits for a peer forever during the handshake: the client reads the whole answer to its startup message
  (header, payload and end marker, at most 64 KiB) within ``fdbStartupTimeout`` (60 s), so a server that stops or
  slows down in the middle of it cannot extend the wait; the server waits at most ``fdbDataConnectionTimeout`` (60 s)
  for the data connection.
* A client is marked as removed by its ``Stop`` whether or not it has a state on the server yet: what is still queued
  for it (e.g. a first field not archived yet) is discarded, and never creates a store or a catalogue for it.
* On the server, the state of a client (store, catalogue, FDB) is looked up, created and removed under
  ``handlerMutex_`` only, and pinned (``shared_ptr``) by whoever uses it: a ``Stop`` removes it at any time, and it
  is released when its last user is done.
* The location of an archived field, received by the listener of the connection to the store, is only queued: a
  thread of the ``RemoteStore`` forwards it to the catalogue (``Locations``). The listener never waits for another
  connection (a slow catalogue would otherwise stall the store connection, and the destruction of its clients); a
  failure of the catalogue fails the archival (``flush()`` reports it), not the connection to the store.
* ``RemoteStore::get()`` caches one read store per endpoint: a store whose connection is lost is replaced by one with
  a new connection (the old one is kept, as other threads may still hold it, but the threads of its connection are
  joined as soon as they have stopped, see ``ClientConnection::reclaimThreads()``). The queue of the data writer is
  shared: a producer keeps a reference while it queues a request, without the lock, so the queue may be closed and
  released meanwhile; it then gets a connection error, never a released queue.
* An ``Exit`` from the server ends the connection on both channels: it is marked invalid, the blocking requests are
  failed and, in single-connection mode (no data listener), the streamed requests too. In dual-connection mode the
  server ends the data channel first: the data listener is given a bounded time (2 s) to read what was sent before;
  if the data channel has not ended by then, the streamed requests are failed and the connection is aborted.
* A failure on one channel ends the connection on both: after the (bounded) wait for the error of the server,
  ``handleConnectionError`` ends the connection (``Connection::endConnection()``), which wakes up the listener of the
  other channel, whose socket may still be healthy.
* Nothing on a failure path waits for another writer of a socket: that writer may be blocked on a full send buffer,
  the peer having stopped reading, and only aborting the connection releases it. The ``Exit`` (client) or ``Error``
  (server) sent before aborting is bounded (``tryWriteBounded``: at most 200 ms for the socket mutex, and 200 ms for
  room in the send buffer), and skipped if it cannot be sent. The graceful ``teardown()`` is only used when the
  connection ends normally.
* A connection that is over releases its sockets as soon as its last listener stops (``Connection::closeSockets()``),
  not when the object is destroyed: an object kept alive (e.g. by a retired read store) holds no file descriptor. A
  socket is closed under its mutex, so never under a write, and a later write fails instead of using a reused
  descriptor.
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
     - set to ``false`` to disable wipe (client or server)
   * - ``networks``
     - server: list of ``name`` and ``netmask``; the first network containing the address of a client is its
       network. A network may set ``wipe: true``: once any network sets ``wipe``, wipe is granted only to the
       clients of the networks setting it to ``true``, and denied to every other client, including those of no
       network
   * - ``stores``
     - catalogue server: store endpoints, by default and optionally per client network (keyed by network ``name``)

Resources: ``fdbReadLimit`` / ``$FDB_READ_LIMIT``, ``fdbDataWriteQueueLength``, ``fdbServerMaxQueueSize``,
``fdbRetrieveQueueSize``, ``fdbMaxConnectRetries``, ``fdbConnectTimeout``, ``fdbStartupTimeout`` /
``$FDB_STARTUP_TIMEOUT`` (client: answer to the startup message, 60 s), ``fdbDataConnectionTimeout`` /
``$FDB_DATA_CONNECTION_TIMEOUT`` (server: data connection of a client, 60 s), ``fdbServerMaxPayloadSize`` /
``$FDB_SERVER_MAX_PAYLOAD_SIZE`` (server: largest message after the handshake, unlimited by default).


Operational note
----------------

``fdb-server`` does not open its listening socket with ``SO_REUSEPORT`` (it did, which let a leftover server on the
same port silently share the new connections, showing up as random hangs or failures). A server whose port is still
in use now fails to start: kill the stale ``fdb-server`` process first.

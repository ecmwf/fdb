.. _FDB_Tracing:

Tracing
=======

FDB records the data-path and data-lifecycle operations as spans compatible with
`OpenTelemetry <https://opentelemetry.io>`__, and propagates the trace context to remote
FDB servers using the `W3C Trace Context <https://www.w3.org/TR/trace-context/>`__ format.
A request issued by an instrumented application can therefore be followed through the
client library, the catalogue server and the store server as a single trace.

FDB does not depend on the OpenTelemetry SDK. Spans follow the OpenTelemetry data model
(trace-id, span-id, parent, kind, attributes, status) and are handed to a pluggable exporter.

Tracing is enabled by default. Trace contexts are always generated and propagated. Spans are
written to the FDB log for the requests that fail, and for the requests whose caller passes a
sampled trace context: the spans with an error status to the error log (``eckit::Log::error()``),
the others to the info log (``eckit::Log::info()``). Successful requests without such a context
print nothing (see :ref:`FDB_Tracing_Failing_Requests`). With ``exporter: none``, nothing is recorded.

Traced operations
-----------------

========================  =====================================================================
Span                      Recorded for
========================  =====================================================================
``fdb.archive``           ``FDB::archive`` (all overloads), aggregated: see below
``fdb.archive_cycle``     archives made without a trace context, from the first archive to the flush
``fdb.flush``             ``FDB::flush``
``fdb.retrieve``          ``FDB::retrieve``, with ``fdb.inspect`` and ``fdb.read`` as children
``fdb.inspect``           ``FDB::inspect`` (ends when the returned iterator is exhausted)
``fdb.list``              ``FDB::list`` (ends when the returned iterator is exhausted)
``fdb.read``              ``FDB::read``
``fdb.wipe``              ``FDB::wipe``
``fdb.purge``             ``FDB::purge``
``fdb.server.*``          the corresponding work on a remote catalogue or store server (kind ``server``)
========================  =====================================================================

Archives are not traced one span per field. The archive calls of a flush cycle are aggregated into
one ``fdb.archive`` span per parent context, from the first archive call to the end of the last one.
It is ended by the flush, and carries the totals of its calls:

=====================  ==========================================================================
Attribute              Meaning
=====================  ==========================================================================
``fdb.calls``          number of archive calls
``fdb.fields``         number of fields archived
``fdb.bytes``          number of bytes archived
``fdb.busy_ns``        time spent in the archive calls, as opposed to between them
=====================  ==========================================================================

If an archive call fails, its span ends at once, with an error status and the key (or request)
that failed; the next call starts a new span. Likewise, a server records one
``fdb.server.archive`` span per client (i.e. per database) and parent context, ended by the
client's flush.

Each span is a child of, in order of precedence:

#. the trace context passed to the API call,
#. the calling thread's current context (``fdb5::tracing::ScopedContext``, ``fdb_trace_begin``),
#. otherwise a newly generated root (for archives: the current archive cycle).

Passing a trace context
-----------------------

C++
~~~

.. code-block:: cpp

   #include "fdb5/api/FDB.h"
   #include "fdb5/tracing/TraceContext.h"

   // e.g. injected from the caller's OpenTelemetry span with the W3C TraceContextPropagator
   auto trace = fdb5::tracing::TraceContext::fromTraceparent(traceparent, tracestate);

   fdb.archive(key, data, length, trace);
   fdb.flush(trace);

   // or, for every FDB call made by this thread within the scope
   fdb5::tracing::ScopedContext scope(trace);

C
~

.. code-block:: c

   fdb_trace_begin("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", NULL);
   fdb_archive(fdb, key, data, length);
   fdb_flush(fdb);
   fdb_trace_end();

Python
~~~~~~

The ``archive``, ``flush``, ``retrieve``, ``list``, ``inspect``, ``wipe`` and ``purge`` methods of
``pyfdb.FDB`` accept the keyword-only arguments ``traceparent`` and ``tracestate``. With
`OpenTelemetry for Python <https://opentelemetry.io/docs/languages/python/>`__:

.. code-block:: python

   from opentelemetry import trace
   from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

   with trace.get_tracer(__name__).start_as_current_span("my-archive"):
       carrier = {}
       TraceContextTextMapPropagator().inject(carrier)

       fdb.archive(data, traceparent=carrier.get("traceparent"), tracestate=carrier.get("tracestate"))
       fdb.flush(traceparent=carrier.get("traceparent"))

A malformed ``traceparent`` raises ``ValueError``. ``None`` or an empty string means no context.
The context is passed to the FDB when the call is made. For the methods that return a generator
(``list``, ``inspect``, ``wipe``, ``purge``), that happens when the generator is first iterated.

Command line tools
~~~~~~~~~~~~~~~~~~

All FDB tools accept ``--traceparent=<W3C traceparent>``. Otherwise they use the
``TRACEPARENT`` and ``TRACESTATE`` environment variables, as set by tools following the
OpenTelemetry environment-variable propagation convention. A malformed ``--traceparent`` is an
error; a malformed ``TRACEPARENT`` is ignored.

The tools write the spans they export to the standard error, so that they cannot corrupt their
results. With ``--porcelain`` or ``--json`` no span is exported at all.

Configuration
-------------

In an FDB client or ``fdb-server`` configuration:

.. code-block:: yaml

   tracing:
     enabled: true                     # false (or 'tracing: false') disables tracing in the process
     sampler: parentbased_always_off   # always_on | always_off | traceidratio | parentbased_*
     ratio: 0.01                       # for traceidratio samplers
     exporter: log                     # log (default) | none

All these settings are process-wide: the ``tracing`` block of the first configuration that
provides one is used (for an ``fdb-server``, its own configuration, which also applies to the
FDBs it opens). The configuration given to an FDB takes precedence over the one it expands to
(e.g. from ``FDB_HOME``); both are read before the FDB connects to any server. With tracing disabled, no span is recorded and no trace context is sent to the
FDB servers.

.. _FDB_Tracing_Failing_Requests:

Failing requests
~~~~~~~~~~~~~~~~

Failing requests are always traced. By default (``sampler: parentbased_always_off``) only the
failing requests are exported, plus those whose caller samples them. To also export a sample of the successful
requests, change the sampler:

.. code-block:: yaml

   tracing:
     sampler: traceidratio   # 1% of the successful requests
     ratio: 0.01

``sampler: parentbased_always_on`` exports every request.

Sampled spans are exported as soon as they end. The spans that are not sampled are recorded as well,
and held until their trace has no open span left in the process, that is until the FDB call
(or the archive cycle) is complete. If one of them ended with an error, the whole request is
exported, including the spans that ended before the failure. Otherwise the held spans are
discarded. At most 10000 spans are held per trace.

Notes:

* The ``parentbased_*`` samplers, including the default, follow the caller's sampled flag. A
  caller which samples all its traces (the OpenTelemetry SDK default) therefore has all its FDB
  spans exported. Use ``always_off`` or ``traceidratio`` to decide independently of the caller.
* The spans which are not sampled are still recorded, in memory, until their request is complete.
  Only ``exporter: none`` or ``enabled: false`` avoid this cost.
* The spans exported because of a failure keep ``trace_flags`` 0: they were not sampled.
* If a flush fails, the traces of the archive spans which it closes are exported with it, whichever trace they
  belong to. The archive spans keep their own status. So, if an archive fails (the backend may have
  failed to flush earlier data, e.g. when it evicts a database), are the traces of the archives not
  yet flushed.
* Each process decides for its own spans: a server exports its part of a request if it fails on
  the server, or if the request is sampled. A request that fails on the client only (e.g. a lost
  connection) is exported by the client, while the server exports its part only if it is sampled.
* The data handle returned by ``retrieve`` is read after the ``fdb.retrieve`` span has ended:
  a failure while reading it is not recorded.

The ``log`` exporter writes one JSON object per span and line, with OTLP field names
(``trace_id``, ``span_id``, ``parent_span_id``, ``start_time_unix_nano``, ``attributes``, ...),
to the error log for the spans with an error status, and to the info log for the others. The
logging infrastructure forwards them, e.g. to an OpenTelemetry Collector. Applications already
using the OpenTelemetry SDK can instead install their own ``fdb5::tracing::SpanExporter``.

Environment variables
~~~~~~~~~~~~~~~~~~~~~

=============================  ================================================================
Variable                       Meaning
=============================  ================================================================
``FDB_TRACING_ENABLED``        ``false`` disables tracing in the process, overriding configuration
``FDB_TRACING_SAMPLER``        sampler name (falls back to ``OTEL_TRACES_SAMPLER``)
``FDB_TRACING_SAMPLER_ARG``    sampler argument (falls back to ``OTEL_TRACES_SAMPLER_ARG``)
``FDB_TRACING_EXPORTER``       ``log`` (default) or ``none``
``OTEL_SERVICE_NAME``          ``service.name`` resource attribute of the exported spans
=============================  ================================================================

Remote protocol
---------------

Trace-context propagation is negotiated when a connection starts: client and server advertise
``"Tracing": [1]`` in the startup dictionary, and the agreed configuration carries the highest
common version. Only then does the client send ``Trace`` messages, which carry the binary trace
context (trace-id, span-id, flags, tracestate). A ``Trace`` message sets the context of the
messages that follow it on the same socket with the same client and request ids, until a message
with other ids, or another ``Trace`` message, is received. A ``Trace`` message without payload means
no context.

The client only sends a ``Trace`` message when the context differs from the one the server holds:
the successive blobs of an archive call, which share one, need a single message. The context is
sent whether it is sampled or not, so that the server records its part of a failing request.
A ``Trace`` message has its own payload size limit, 538 bytes, whatever the limit of the requests
of the server: a valid context never makes a request fail, and a larger one is a protocol error.

If either side predates this feature, or has tracing disabled in its process, the key is absent
and no ``Trace`` message is ever sent: the wire protocol is unchanged. A ``Trace`` message that the
server cannot use is logged and ignored; tracing never causes a request to fail.

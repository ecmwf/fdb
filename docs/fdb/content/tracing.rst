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

Tracing is enabled by default. Without an exporter configured, trace contexts are generated
and propagated but nothing is recorded.

Traced operations
-----------------

========================  =====================================================================
Span                      Recorded for
========================  =====================================================================
``fdb.archive``           ``FDB::archive`` (all overloads)
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

Configuration
-------------

In an FDB client or ``fdb-server`` configuration:

.. code-block:: yaml

   tracing:
     enabled: true                     # false (or 'tracing: false') disables tracing for this FDB/server
     sampler: parentbased_always_on    # always_on | always_off | traceidratio | parentbased_*
     ratio: 0.01                       # for traceidratio samplers
     exporter: log                     # none (default) | log
     path: /path/to/spans.jsonl        # log exporter target; default: the info log

``enabled`` applies to each FDB instance. ``sampler``, ``ratio``, ``exporter`` and ``path``
are process-wide: the first configuration that provides them is used.

The ``log`` exporter writes one JSON object per span and line, with OTLP field names
(``trace_id``, ``span_id``, ``parent_span_id``, ``start_time_unix_nano``, ``attributes``, ...),
suitable for ingestion by an OpenTelemetry Collector ``filelog`` receiver. Applications already
using the OpenTelemetry SDK can instead install their own ``fdb5::tracing::SpanExporter``.

Environment variables
~~~~~~~~~~~~~~~~~~~~~

=============================  ================================================================
Variable                       Meaning
=============================  ================================================================
``FDB_TRACING_ENABLED``        ``false`` disables tracing in the process, overriding configuration
``FDB_TRACING_SAMPLER``        sampler name (falls back to ``OTEL_TRACES_SAMPLER``)
``FDB_TRACING_SAMPLER_ARG``    sampler argument (falls back to ``OTEL_TRACES_SAMPLER_ARG``)
``FDB_TRACING_EXPORTER``       ``none`` or ``log``
``FDB_TRACING_LOG_PATH``       file the ``log`` exporter appends to
``OTEL_SERVICE_NAME``          ``service.name`` resource attribute of the exported spans
=============================  ================================================================

Remote protocol
---------------

Trace-context propagation is negotiated when a connection starts: client and server advertise
``"Tracing": [1]`` in the startup dictionary, and the agreed configuration carries the highest
common version. Only then does the client precede a request with a ``Trace`` message, which
carries the binary trace context (trace-id, span-id, flags, tracestate) for the following
message with the same client and request ids.

If either side predates this feature, or has tracing disabled, the key is absent and no
``Trace`` message is ever sent: the wire protocol is unchanged. A ``Trace`` message that the
server cannot use is logged and ignored; tracing never causes a request to fail.

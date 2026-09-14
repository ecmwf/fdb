try:
    import findlibs

    findlibs.load("fdb5")

    from ._pyfdb_bindings import (
        Config,
        ControlAction,
        ControlElement,
        ControlIdentifier,
        DataHandle,
        FDB,
        FDBToolRequest,
        IndexAxis,
        ListElement,
        PurgeElement,
        StatsElement,
        URI,
        WipeElement,
        WipeElementType,
        __fdb5_build_version__,
        init_bindings,
        version_info,
    )
except ImportError as exc:
    raise ImportError(
        "pyfdb's compiled pybind11 extension (_pyfdb_bindings) failed to import. "
        "This usually means it wasn't built, or was built for a "
        "different Python version/platform."
    ) from exc

# Initialize eckit::Main once on package import — the single place this happens.
init_bindings()

__all__ = [
    "Config",
    "ControlAction",
    "ControlElement",
    "ControlIdentifier",
    "DataHandle",
    "FDB",
    "FDBToolRequest",
    "IndexAxis",
    "ListElement",
    "PurgeElement",
    "StatsElement",
    "URI",
    "WipeElement",
    "WipeElementType",
    "__fdb5_build_version__",
    "init_bindings",
    "version_info",
]

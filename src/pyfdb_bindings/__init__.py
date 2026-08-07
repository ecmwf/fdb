try:
    from . import pyfdb_bindings as pyfdb_bindings
except ImportError as exc:
    raise ImportError(
        "pyfdb's compiled pybind11 extension (pyfdb_bindings) failed to import. "
        "This usually means it wasn't built, or was built for a "
        "different Python version/platform."
    ) from exc

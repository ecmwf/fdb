try:
    from ._pyfdb_bindings import *
except ImportError as exc:
    raise ImportError(
        "pyfdb's compiled pybind11 extension (_pyfdb_bindings) failed to import. "
        "This usually means it wasn't built, or was built for a "
        "different Python version/platform."
    ) from exc

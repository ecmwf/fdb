# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
import time
from functools import wraps


def profile(name):
    """Profile decorator with custom name.

    Args:
        name: Display name for the profiled function
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            wall_start = time.perf_counter_ns()
            cpu_start = time.process_time_ns()

            result = func(*args, **kwargs)

            wall_time = (time.perf_counter_ns() - wall_start) / 1e9
            cpu_time = (time.process_time_ns() - cpu_start) / 1e9
            cpu_percent = (cpu_time / wall_time * 100) if wall_time > 0 else 0

            print(f"Time for: {name}:")
            print(f"  Wall time: {wall_time:.3f}s")
            print(f"  CPU time:  {cpu_time:.3f}s ({cpu_percent:.1f}% CPU utilization)")

            return result

        return wrapper

    return decorator

# SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0


class InternalError(Exception):
    """Indicates an internal error.

    You will only see this exception if there is something broken inside pychunked_data_view.
    """

    pass


class MarsRequestFormattingError(RuntimeError):
    pass

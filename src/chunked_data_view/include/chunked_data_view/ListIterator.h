// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "eckit/io/DataHandle.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"

#include <memory>
#include <optional>
#include <tuple>
#include <utility>

namespace chunked_data_view {

/// Abstract iterator over FDB fields matching a MARS request.
///
/// Each call to next() yields the MARS key and a data handle for one matching field,
/// or std::nullopt when the sequence is exhausted.
class ListIteratorInterface {

public:

    virtual ~ListIteratorInterface() = default;

    /// Returns the next (key, data-handle) pair, or std::nullopt if there are no more fields.
    virtual std::optional<std::tuple<fdb5::Key, std::unique_ptr<eckit::DataHandle>>> next() = 0;
};


/// Wraps a real fdb5::ListIterator to satisfy the ListIteratorInterface contract.
class ListIteratorWrapperImpl : public ListIteratorInterface {

    fdb5::ListIterator listIterator_;

public:

    explicit ListIteratorWrapperImpl(fdb5::ListIterator listIterator) : listIterator_(std::move(listIterator)) {};
    std::optional<std::tuple<fdb5::Key, std::unique_ptr<eckit::DataHandle>>> next() override;
};

/// Wraps @p listIterator in a ListIteratorInterface-compatible heap object.
std::unique_ptr<ListIteratorInterface> makeListIterator(fdb5::ListIterator listIterator);
};  // namespace chunked_data_view

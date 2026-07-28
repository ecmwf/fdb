/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
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

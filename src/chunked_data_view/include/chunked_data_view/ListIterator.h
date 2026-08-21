// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "eckit/io/DataHandle.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"

#include <memory>
#include <optional>

namespace chunked_data_view {

/// A field entry returned by a list/inspect call: the MARS key identifying the field
/// and a pointer to its storage location (from which a DataHandle can be opened).
struct ListElement {
    fdb5::Key key;
    std::shared_ptr<const fdb5::FieldLocation> location;

    /// Opens a new DataHandle for this field.
    std::unique_ptr<eckit::DataHandle> dataHandle() const {
        return std::unique_ptr<eckit::DataHandle>(location->dataHandle());
    }
};

/// Abstract iterator over FDB fields matching a MARS request.
///
/// Each call to next() yields a ListElement for one matching field,
/// or std::nullopt when the sequence is exhausted.
class ListIteratorInterface {

public:

    virtual ~ListIteratorInterface() = default;

    /// Returns the next field entry, or std::nullopt if there are no more fields.
    virtual std::optional<ListElement> next() = 0;
};


/// Wraps a real fdb5::ListIterator to satisfy the ListIteratorInterface contract.
class ListIteratorWrapperImpl : public ListIteratorInterface {

    fdb5::ListIterator listIterator_;

public:

    explicit ListIteratorWrapperImpl(fdb5::ListIterator listIterator) : listIterator_(std::move(listIterator)) {};
    std::optional<ListElement> next() override;
};

/// Wraps @p listIterator in a ListIteratorInterface-compatible heap object.
std::unique_ptr<ListIteratorInterface> makeListIterator(fdb5::ListIterator listIterator);
};  // namespace chunked_data_view

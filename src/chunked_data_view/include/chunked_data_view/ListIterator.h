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

class ListIteratorInterface {

public:

    virtual ~ListIteratorInterface() = default;
    virtual std::optional<std::tuple<fdb5::Key, std::unique_ptr<eckit::DataHandle>>> next() = 0;
};


class ListIteratorWrapperImpl : public ListIteratorInterface {

    fdb5::ListIterator listIterator_;

public:

    explicit ListIteratorWrapperImpl(fdb5::ListIterator listIterator) : listIterator_(std::move(listIterator)) {};
    std::optional<std::tuple<fdb5::Key, std::unique_ptr<eckit::DataHandle>>> next() override;
};

std::unique_ptr<ListIteratorInterface> makeListIterator(fdb5::ListIterator listIterator);
};  // namespace chunked_data_view

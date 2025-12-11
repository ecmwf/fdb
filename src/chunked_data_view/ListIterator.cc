/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "chunked_data_view/ListIterator.h"

#include "eckit/io/DataHandle.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/Key.h"

#include <memory>
#include <optional>
#include <tuple>
#include <utility>

namespace chunked_data_view {

std::optional<std::tuple<fdb5::Key, std::unique_ptr<eckit::DataHandle>>> ListIteratorWrapperImpl::next() {
    fdb5::ListElement elem;

    auto has_next = listIterator_.next(elem);

    if (has_next) {
        return std::make_tuple(elem.combinedKey(), std::unique_ptr<eckit::DataHandle>(elem.location().dataHandle()));
    }

    // std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // has_next = listIterator_.next(elem);
    //
    // if (has_next) {
    //     return std::make_tuple(elem.combinedKey(), std::unique_ptr<eckit::DataHandle>(elem.location().dataHandle()));
    // }

    return std::nullopt;
};

std::unique_ptr<ListIteratorInterface> makeListIterator(fdb5::ListIterator listIterator) {
    return std::make_unique<ListIteratorWrapperImpl>(std::move(listIterator));
}
}  // namespace chunked_data_view

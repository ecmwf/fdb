// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
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

    return std::nullopt;
};

std::unique_ptr<ListIteratorInterface> makeListIterator(fdb5::ListIterator listIterator) {
    return std::make_unique<ListIteratorWrapperImpl>(std::move(listIterator));
}
}  // namespace chunked_data_view

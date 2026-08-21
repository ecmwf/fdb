// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "chunked_data_view/ListIterator.h"

#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"

#include <optional>

namespace chunked_data_view {

std::optional<ListElement> ListIteratorWrapperImpl::next() {
    fdb5::ListElement elem;

    if (listIterator_.next(elem)) {
        return ListElement{elem.combinedKey(), elem.sharedLocation()};
    }

    return std::nullopt;
};

std::unique_ptr<ListIteratorInterface> makeListIterator(fdb5::ListIterator listIterator) {
    return std::make_unique<ListIteratorWrapperImpl>(std::move(listIterator));
}
}  // namespace chunked_data_view

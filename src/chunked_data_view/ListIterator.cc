
#include "chunked_data_view/ListIterator.h"

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

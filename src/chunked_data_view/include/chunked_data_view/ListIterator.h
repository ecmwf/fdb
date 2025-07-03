#pragma once 

#include <optional>
#include <tuple>
#include "eckit/io/DataHandle.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"

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

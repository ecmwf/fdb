#pragma once
#include <memory>
#include <string>
#include "datautil/common/scope.h"
#include "datautil/common/data_map.h"


namespace fdb5 { class FDB; }

namespace compare {

class IComparator {
public:
    virtual ~IComparator() = default;
    virtual Result compare(const Options& opts) = 0;
};

// Known types: "mars", "grib"
class ComparatorFactory {
public:
    static std::unique_ptr<IComparator> create(const std::string& type,
                                               common::DataIndex& ref,
                                               common::DataIndex& test);
};

} // namespace compare
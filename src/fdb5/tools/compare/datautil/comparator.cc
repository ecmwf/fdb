#include "comparator.h"
#include "datautil/mars/compare.h"
// #include "datautil/grib/compare.h"


namespace compare {

std::unique_ptr<IComparator> ComparatorFactory::create(const std::string& type,
                                                       common::DataIndex& ref,
                                                       common::DataIndex& test) {
    if (type == "mars")  return mars::makeMarsComparator(ref, test);
    // if (type == "grib")  return makeGribComparator(ref, test);
    
    return nullptr;
}

} // namespace compare

#pragma once
#include "datautil/comparator.h"
#include "datautil/common/data_map.h"
#include "datautil/common/scope.h"

using namespace compare;
using namespace compare::common;
namespace compare::mars {
    class MarsComparator final : public IComparator {
    public:
        MarsComparator(const DataIndex& ref, const DataIndex& test) : ref_(ref), test_(test) {}
        Result compare(const Options& opts) override;
        
    private:
        const DataIndex& ref_;
        const DataIndex& test_;
    };

    std::unique_ptr<IComparator> makeMarsComparator(const DataIndex& ref,
                                                const DataIndex& test);
} // namespace compare::mars

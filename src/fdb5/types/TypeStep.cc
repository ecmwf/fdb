/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/container/DenseSet.h"
#include "eckit/utils/Translator.h"

#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/StepRange.h"
#include "metkit/mars/StepRangeNormalise.h"

#include <string>
#include "fdb5/database/Catalogue.h"
#include "fdb5/types/TypeStep.h"
#include "fdb5/types/TypesFactory.h"

using metkit::mars::StepRange;
using metkit::mars::StepRangeNormalise;


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeStep::TypeStep(const std::string& name, const std::string& type) : Type(name, type) {}

TypeStep::~TypeStep() {}


std::string TypeStep::toKey(const std::string& value) const {
    return StepRange(value);
}

bool TypeStep::match(const std::string&, const std::string& value1, const std::string& value2) const {
    if (value1 == value2) {
        return true;
    }

    std::string z1 = "0-" + value1;
    if (z1 == value2) {
        return true;
    }

    std::string z2 = "0-" + value2;
    if (z2 == value1) {
        return true;
    }

    return false;
}

void TypeStep::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                         eckit::StringList& values, const Notifier&, const CatalogueReader* cat) const {

    // Get the steps / step ranges from the request

    std::vector<std::string> steps;
    request.getValues(keyword, steps, true);

    std::vector<StepRange> ranges;
    std::copy(steps.begin(), steps.end(), std::back_inserter(ranges));

    // If this is before knowing the Catalogue, we are constrained on what we can do.

    if (cat) {

        // Get the axis
        auto ax = cat->axis("step");
        if (ax) {
            std::vector<StepRange> axis;
            for (auto step : ax->get()) {
                if (!step.empty()) {
                    axis.push_back(StepRange(step));
                }
            }
            std::sort(axis.begin(), axis.end());

            // Match the step range to the axis
            StepRangeNormalise::normalise(ranges, axis);
        }
    }

    // Convert the ranges back into strings for the FDB

    eckit::Translator<StepRange, std::string> t;

    values.reserve(ranges.size());
    std::transform(ranges.begin(), ranges.end(), std::back_inserter(values), [&](const StepRange& r) { return t(r); });
}

void TypeStep::print(std::ostream& out) const {
    out << "TypeStep[name=" << name_ << "]";
}

static TypeBuilder<TypeStep> type("Step");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

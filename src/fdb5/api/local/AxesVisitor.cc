/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/local/AxesVisitor.h"

#include "fdb5/database/Catalogue.h"

namespace fdb5 {
namespace api {
namespace local {

//----------------------------------------------------------------------------------------------------------------------

AxesVisitor::AxesVisitor(eckit::Queue<AxesElement>& queue,
                         const metkit::mars::MarsRequest& request,
                         const Config& config,
                         int level) :
        QueryVisitor<AxesElement>(queue, request),
        schema_(config.schema()),
        level_(level) {}

#if 0

// TODO: Here we can do nice tricks to make things go muuuuuuuuuuuuuch faster...
//       See improvements to the EntryVisitMechanism... & the schema

bool AxesVisitor::preVisitDatabase(const eckit::URI& uri) {

    // If level == 1, avoid constructing the Catalogue/Store objects, so just interrogate the URIs
    if (level_ == 1 && uri.scheme() == "toc") {
        // TODO: This is hacky, only works with the toc backend...
        if (schema_.matchFirstLevel(uri.path().baseName(), dbKey_)) {
            axes_.wipe();
            axes_.insert(dbKey_);
            axes_.sort();
            queue_.emplace(AxesElement{std::move(dbKey_), std::move(axes_)});
        }
        return false;
    }

    return true;
}
#endif

bool AxesVisitor::visitDatabase(const Catalogue& catalogue, const Store& store) {
    dbKey_ = catalogue.key();
    axes_.wipe();
    axes_.insert(dbKey_);
    axes_.sort();
    return (level_ > 1);
}

bool AxesVisitor::visitIndex(const Index& index) {
    if (index.partialMatch(request_)) {
        IndexAxis tmpAxis;
        tmpAxis.insert(index.key());
        tmpAxis.sort();
        axes_.merge(tmpAxis);   // avoid sorts on the (growing) main Axes object

        if (level_ > 2) {
            axes_.merge(index.axes());
        }
    }
    return false;
}

void AxesVisitor::catalogueComplete(const fdb5::Catalogue& catalogue) {
    queue_.emplace(AxesElement{std::move(dbKey_), std::move(axes_)});
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5

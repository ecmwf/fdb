/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Emanuele Danovaro
/// @author Simon Smart
/// @date   August 2022

#pragma once

#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/database/MoveVisitor.h"
#include "fdb5/toc/TocCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TocMoveVisitor : public MoveVisitor {

public:

    TocMoveVisitor(const TocCatalogue& catalogue, const Store& store, const metkit::mars::MarsRequest& request,
                   const eckit::URI& dest, eckit::Queue<MoveElement>& queue);
    ~TocMoveVisitor() override;

private:  // methods

    bool visitDatabase(const Catalogue& catalogue) override;

    void move();

private:  // members

    // What are the parameters of the move operation
    const TocCatalogue& catalogue_;
    const Store& store_;
    eckit::Queue<MoveElement>& queue_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

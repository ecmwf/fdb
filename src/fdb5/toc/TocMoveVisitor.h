/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   August 2019

#ifndef fdb5_TocMoveVisitor_H
#define fdb5_TocMoveVisitor_H


#include "fdb5/database/MoveVisitor.h"
#include "fdb5/toc/TocCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TocMoveVisitor : public MoveVisitor {

public:

    TocMoveVisitor(const TocCatalogue& catalogue,
                   const Store& store,
                   const metkit::mars::MarsRequest& request,
                   const eckit::URI& dest);
    ~TocMoveVisitor() override;

private: // methods

    bool visitDatabase(const Catalogue& catalogue, const Store& store) override;

    void move();

private: // members

    // What are the parameters of the move operation
    const TocCatalogue& catalogue_;
    const Store& store_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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

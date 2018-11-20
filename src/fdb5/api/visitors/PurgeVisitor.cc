
/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/visitors/PurgeVisitor.h"

namespace fdb5 {
namespace api {
namespace visitor {

//----------------------------------------------------------------------------------------------------------------------


PurgeVisitor::PurgeVisitor(eckit::Queue<PurgeElement> &queue, bool doit) :
    QueryVisitor(queue),
    doit_(doit) {}

void PurgeVisitor::visitDatabase(const DB& db) {
    NOTIMP;
}

void PurgeVisitor::visitIndex(const Index& index) {
    NOTIMP;
}

void PurgeVisitor::databaseComplete(const DB& db) {
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace visitor
} // namespace api
} // namespace fdb5

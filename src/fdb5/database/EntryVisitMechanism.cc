/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/EntryVisitMechanism.h"

#include "eckit/io/AutoCloser.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Manager.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Schema.h"

using namespace eckit;


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBVisitException : public eckit::Exception {
public:
    using Exception::Exception;
};

//----------------------------------------------------------------------------------------------------------------------

EntryVisitor::EntryVisitor() : currentDatabase_(0), currentIndex_(0) {}

EntryVisitor::~EntryVisitor() {}

bool EntryVisitor::visitDatabase(const DB& db) {
    currentDatabase_ = &db;
    return true;
}

void EntryVisitor::databaseComplete(const DB& db) {
    if (currentDatabase_ != 0) {
        ASSERT(currentDatabase_ == &db);
    }
    currentDatabase_ = 0;
    currentIndex_ = 0;
}

bool EntryVisitor::visitIndex(const Index& index) {
    currentIndex_ = &index;
    return true;
}

void EntryVisitor::visitDatum(const Field &field, const std::string& keyFingerprint) {
    ASSERT(currentDatabase_);
    ASSERT(currentIndex_);

    Key key(keyFingerprint, currentDatabase_->schema().ruleFor(currentDatabase_->key(), currentIndex_->key()));
    visitDatum(field, key);
}

//----------------------------------------------------------------------------------------------------------------------

EntryVisitMechanism::EntryVisitMechanism(const Config& config) :
    dbConfig_(config),
    fail_(true) {}

void EntryVisitMechanism::visit(const FDBToolRequest& request, EntryVisitor& visitor) {

    if (visitor.visitEntries() && !visitor.visitIndexes()) {
        throw FDBVisitException("Cannot visit entries without visiting indexes", Here());
    }

    // A request against all is the same as using an empty key in visitableLocations.

    ASSERT(request.all() == request.request().empty());

    // TODO: Put minimim keys check into FDBToolRequest.

    Log::debug<LibFdb5>() << "REQUEST ====> " << request.request() << std::endl;

    try {

        std::vector<PathName> paths(Manager(dbConfig_).visitableLocations(request.request(), request.all()));

        // n.b. it is not an error if nothing is found (especially in a sub-fdb).

        // And do the visitation

        for (PathName path : paths) {

            if (path.exists()) {
                if (!path.isDir()) path = path.dirName();
                path = path.realName();

                Log::debug<LibFdb5>() << "FDB processing path " << path << std::endl;

                std::unique_ptr<DB> db(DBFactory::buildReader(path));
                ASSERT(db->open());
                eckit::AutoCloser<DB> closer(*db);

                db->visitEntries(visitor, false);
            }
        }

    } catch (eckit::UserError&) {
        throw;
    } catch (eckit::Exception& e) {
        Log::warning() << e.what() << std::endl;
        if (fail_) throw;
    }





}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

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

#include "fdb5/api/FDBToolRequest.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Manager.h"
#include "fdb5/LibFdb.h"
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

void EntryVisitor::visitDatabase(const DB& db) {
    currentDatabase_ = &db;
}

void EntryVisitor::visitIndex(const Index& index) {
    currentIndex_ = &index;
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
    fail_(true) {
}

void EntryVisitMechanism::visit(const FDBToolRequest& request, EntryVisitor& visitor) {

    // A request against all is the same as using an empty key in visitableLocations.

    ASSERT(request.all() == request.key().empty());

    // TODO: Put minimim keys check into FDBToolRequest.

    Log::debug<LibFdb>() << "KEY ====> " << request.key() << std::endl;

    try {

        std::vector<PathName> paths(Manager(dbConfig_).visitableLocations(request.key()));

        if (paths.size() == 0) {
            std::stringstream ss;
            ss << "No FDB matches for all";
            Log::warning() << ss.str() << std::endl;
            if (fail_) throw FDBVisitException(ss.str(), Here());
        }

        // And do the visitation

        for (PathName path : paths) {

            if (path.exists()) {
                if (!path.isDir()) path = path.dirName();
                path = path.realName();

                Log::debug<LibFdb>() << "FDB processing path " << path << std::endl;

                std::unique_ptr<DB> db(DBFactory::buildReader(path));
                ASSERT(db->open());

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

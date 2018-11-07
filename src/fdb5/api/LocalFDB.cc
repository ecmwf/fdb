/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"
#include "eckit/container/Queue.h"

#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/LocalFDB.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Retriever.h"
#include "fdb5/LibFdb.h"

#include "marslib/MarsTask.h"


namespace fdb5 {

static FDBBuilder<LocalFDB> localFdbBuilder("local");

//----------------------------------------------------------------------------------------------------------------------

namespace {

class ListVisitor : public EntryVisitor {

public:

    ListVisitor(eckit::Queue<ListElement>& queue) :
        queue_(queue) {}

    void visitDatum(const Field& field, const Key& key) override {
        ASSERT(currentDatabase_);
        ASSERT(currentIndex_);

        queue_.emplace(ListElement({currentDatabase_->key(), currentIndex_->key(), key},
                                      field.sharedLocation()));
    }

private:
    eckit::Queue<ListElement>& queue_;
};


class DumpVisitor : public EntryVisitor {

private:

    DumpVisitor(bool simple_);

    void visitDatabase(const DB& db) override {
        db.dump(bufferedOstream, simple_);
    }

    void visitIndex(const Index& index) override { NOTIMP; /* intentionally  */ }
    void visitDatum(const Field& field, const Key& key) override { NOTIMP; /* intentionally */ }
};

}

//----------------------------------------------------------------------------------------------------------------------

void LocalFDB::archive(const Key& key, const void* data, size_t length) {

    if (!archiver_) {
        eckit::Log::debug<LibFdb>() << *this << ": Constructing new archiver" << std::endl;
        archiver_.reset(new Archiver(config_));
    }

    archiver_->archive(key, data, length);
}


eckit::DataHandle *LocalFDB::retrieve(const MarsRequest &request) {

    if (!retriever_) {
        eckit::Log::debug<LibFdb>() << *this << ": Constructing new retriever" << std::endl;
        retriever_.reset(new Retriever(config_));
    }

    MarsRequest e("environ");
    MarsTask task(request, e);

    return retriever_->retrieve(task);
}


ListIterator LocalFDB::list(const FDBToolRequest& request) {

    // Create a general mechanism to call an EntryVisitor on a local object
    // Call the entry visitor
    // See FDBInspect...

    auto async_worker = [this, request] (eckit::Queue<ListElement>& queue) {
        EntryVisitMechanism mechanism(config_);
        ListVisitor visitor(queue);
        mechanism.visit(request, visitor);
    };

    return ListIterator(new ListAsyncIterator(async_worker));
}

std::string LocalFDB::id() const {
    return config_.expandPath("~fdb");
}


void LocalFDB::flush() {
    if (archiver_) {
        archiver_->flush();
    }
}


void LocalFDB::print(std::ostream &s) const {
    s << "LocalFDB(home=" << config_.expandPath("~fdb") << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

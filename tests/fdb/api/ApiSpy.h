/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @date   Mar 2018

#ifndef fdb_testing_ApiSpy_H
#define fdb_testing_ApiSpy_H

#include <tuple>
#include <vector>

#include "eckit/message/Message.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"

#include "metkit/mars/MarsRequest.h"

namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

class ApiSpy : public fdb5::FDBBase {

private:  // types

    struct Counts {
        Counts() :
            archive(0),
            inspect(0),
            list(0),
            axes(0),
            dump(0),
            status(0),
            wipe(0),
            purge(0),
            stats(0),
            flush(0),
            control(0),
            move(0) {}
        size_t archive;
        size_t inspect;
        size_t list;
        size_t axes;
        size_t dump;
        size_t status;
        size_t wipe;
        size_t purge;
        size_t stats;
        size_t flush;
        size_t control;
        size_t move;
    };

    using Archives  = std::vector<std::tuple<fdb5::Key, const void*, size_t>>;
    using Retrieves = std::vector<metkit::mars::MarsRequest>;

    class FakeDataHandle : public eckit::DataHandle {
        void print(std::ostream& s) const override { s << "FakeDH"; }
        eckit::Length openForRead() override { return 999; }
        void openForWrite(const eckit::Length&) override { NOTIMP; }
        void openForAppend(const eckit::Length&) override { NOTIMP; }
        eckit::Length estimate() override { return 999; }
        long read(void*, long) override { NOTIMP; }
        long write(const void*, long) override { NOTIMP; }
        void close() override { NOTIMP; }
    };

public:  // methods

    ApiSpy(const fdb5::Config& config, const std::string& name) : FDBBase(config, name) {
        knownSpies().push_back(this);
    }
    ~ApiSpy() override { knownSpies().erase(std::find(knownSpies().begin(), knownSpies().end(), this)); }

    void archive(const fdb5::Key& key, const void* data, size_t length) override {
        counts_.archive += 1;
        archives_.push_back(std::make_tuple(key, data, length));
    }

    fdb5::ListIterator inspect(const metkit::mars::MarsRequest& request) override {
        counts_.inspect += 1;
        retrieves_.push_back(request);
        return fdb5::ListIterator(0);
    }

    fdb5::ListIterator list(const fdb5::FDBToolRequest& /* request */, const int level) override {
        counts_.list += 1;
        ASSERT(level == 3);
        return fdb5::ListIterator(0);
    }

    fdb5::AxesIterator axesIterator(const fdb5::FDBToolRequest& request, int level = 3) override {
        counts_.axes += 1;
        return fdb5::AxesIterator(0);
    }

    fdb5::DumpIterator dump(const fdb5::FDBToolRequest& request, bool simple) override {
        counts_.dump += 1;
        return fdb5::DumpIterator(0);
    }

    fdb5::StatusIterator status(const fdb5::FDBToolRequest& request) override {
        counts_.status += 1;
        return fdb5::StatusIterator(0);
    }

    fdb5::WipeStateIterator wipe(const fdb5::FDBToolRequest& request, bool doit, bool verbose,
                                 bool unsafeWipeAll) override {
        counts_.wipe += 1;
        return fdb5::WipeStateIterator(0);
    }

    fdb5::PurgeIterator purge(const fdb5::FDBToolRequest& request, bool doit, bool verbose) override {
        counts_.purge += 1;
        return fdb5::PurgeIterator(0);
    }

    fdb5::StatsIterator stats(const fdb5::FDBToolRequest& request) override {
        counts_.stats += 1;
        return fdb5::StatsIterator(0);
    }

    fdb5::MoveIterator move(const fdb5::FDBToolRequest& request, const eckit::URI& dest) override {
        counts_.move += 1;
        return fdb5::MoveIterator(0);
    }

    fdb5::StatusIterator control(const fdb5::FDBToolRequest& request, fdb5::ControlAction action,
                                 fdb5::ControlIdentifiers identifiers) override {
        counts_.control += 1;
        return fdb5::StatusIterator(0);
    }

    void flush() override { counts_.flush += 1; }

    // For diagnostics

    const Counts& counts() const { return counts_; }

    const Archives& archives() const { return archives_; }
    const Retrieves& retrieves() const { return retrieves_; }

    static std::vector<ApiSpy*>& knownSpies() {
        static std::vector<ApiSpy*> s;
        return s;
    }

private:  // methods

    void print(std::ostream& s) const override { s << "ApiSpy()"; }

private:  // members

    Counts counts_;

    Archives archives_;
    Retrieves retrieves_;
};


static fdb5::FDBBuilder<ApiSpy> selectFdbBuilder("spy");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

#endif

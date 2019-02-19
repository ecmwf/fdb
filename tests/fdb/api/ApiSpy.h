/// @date   Mar 2018

#ifndef fdb_testing_ApiSpy_H
#define fdb_testing_ApiSpy_H

#include <vector>
#include <tuple>

#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/FDB.h"

#include "marslib/MarsRequest.h"

namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

class ApiSpy : public fdb5::FDBBase {

private: // types

    struct Counts {
        Counts() : archive(0), retrieve(0), list(0), dump(0), where(0), wipe(0), purge(0), stats(0), flush(0) {}
        size_t archive;
        size_t retrieve;
        size_t list;
        size_t dump;
        size_t where;
        size_t wipe;
        size_t purge;
        size_t stats;
        size_t flush;
    };

    using Archives = std::vector<std::tuple<fdb5::Key, const void*, size_t>>;
    using Retrieves = std::vector<MarsRequest>;

    class FakeDataHandle : public eckit::DataHandle {
        void print(std::ostream& s) const override { s << "FakeDH"; }
        eckit::Length openForRead() override { return 999; }
        void openForWrite(const eckit::Length&) override { NOTIMP; }
        void openForAppend(const eckit::Length&)  override { NOTIMP; }
        eckit::Length estimate() override { return 999; }
        long read(void*,long) override { NOTIMP; }
        long write(const void*,long) override { NOTIMP; }
        void close() override { NOTIMP; }
    };

public: // methods

    ApiSpy(const fdb5::Config& config, const std::string& name) : FDBBase(config, name) {
        knownSpies().push_back(this);
    }
    virtual ~ApiSpy() {
        knownSpies().erase(std::find(knownSpies().begin(), knownSpies().end(), this));
    }

    virtual void archive(const fdb5::Key& key, const void* data, size_t length) {
        counts_.archive += 1;
        archives_.push_back(std::make_tuple(key, data, length));
    }

    virtual eckit::DataHandle* retrieve(const MarsRequest& request) {
        counts_.retrieve += 1;
        retrieves_.push_back(request);
        return new FakeDataHandle;
    }

    virtual fdb5::ListIterator list(const fdb5::FDBToolRequest& request) override {
        counts_.list += 1;
        return fdb5::ListIterator(0);
    }

    virtual fdb5::DumpIterator dump(const fdb5::FDBToolRequest& request, bool simple) override {
        counts_.dump += 1;
        return fdb5::DumpIterator(0);
    }

    virtual fdb5::WhereIterator where(const fdb5::FDBToolRequest& request) override {
        counts_.where += 1;
        return fdb5::WhereIterator(0);
    }

    virtual fdb5::WipeIterator wipe(const fdb5::FDBToolRequest& request, bool doit, bool verbose) override {
        counts_.wipe += 1;
        return fdb5::WipeIterator(0);
    }

    virtual fdb5::PurgeIterator purge(const fdb5::FDBToolRequest& request, bool doit, bool verbose) override {
        counts_.purge += 1;
        return fdb5::PurgeIterator(0);
    }

    virtual fdb5::StatsIterator stats(const fdb5::FDBToolRequest& request) override {
        counts_.stats += 1;
        return fdb5::StatsIterator(0);
    }

    virtual void flush() {
        counts_.flush += 1;
    }

    // For diagnostics

    const Counts& counts() const { return counts_; }

    const Archives& archives() const { return archives_; }
    const Retrieves& retrieves() const { return retrieves_; }

    static std::vector<ApiSpy*>& knownSpies() {
        static std::vector<ApiSpy*> s;
        return s;
    }

private: // methods

    virtual void print(std::ostream& s) const { s << "ApiSpy()"; }

private: // members

    Counts counts_;

    Archives archives_;
    Retrieves retrieves_;
};


static fdb5::FDBBuilder<ApiSpy> selectFdbBuilder("spy");

//----------------------------------------------------------------------------------------------------------------------

} // namespace test
} // namespace fdb

#endif

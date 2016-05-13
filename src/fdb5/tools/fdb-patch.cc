/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "grib_api.h"

#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/log/BigNum.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Timer.h"
#include "eckit/log/Plural.h"
#include "eckit/thread/ThreadPool.h"

#include "eckit/config/Resource.h"

#include "fdb5/config/UMask.h"
#include "fdb5/database/Index.h"
#include "fdb5/grib/GribArchiver.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBInspect.h"


//----------------------------------------------------------------------------------------------------------------------

class PatchVisitor : public fdb5::EntryVisitor {
  public:
    PatchVisitor(fdb5::HandleGatherer &gatherer, size_t &count, eckit::Length &total) :
        gatherer_(gatherer),
        count_(count),
        total_(total) {
    }

  private:
    virtual void visit(const fdb5::Index &index,
                       const std::string &indexFingerprint,
                       const std::string &fieldFingerprint,
                       const fdb5::Field &field);

    fdb5::HandleGatherer &gatherer_;
    size_t &count_;
    eckit::Length &total_;
    std::set<std::string> active_;

};

void PatchVisitor::visit(const fdb5::Index &index,
                         const std::string &indexFingerprint,
                         const std::string &fieldFingerprint,
                         const fdb5::Field &field) {

    std::string unique = indexFingerprint + "+" + fieldFingerprint;

    if (active_.find(unique) == active_.end()) {
        active_.insert(unique);

        gatherer_.add(field.dataHandle());
        count_++;
        total_ += field.length();
    }
}

//----------------------------------------------------------------------------------------------------------------------

class PatchArchiver : public fdb5::GribArchiver {
    const fdb5::Key &key_;
    virtual void patch(grib_handle *);
  public:
    PatchArchiver(const fdb5::Key &key): key_(key) {

    }
};

void PatchArchiver::patch(grib_handle *h) {
    for (fdb5::Key::const_iterator j = key_.begin(); j != key_.end(); ++j) {
        size_t len = j->second.size();
        ASSERT(grib_set_string(h, j->first.c_str(), j->second.c_str(), &len) == 0);
    }
}

//----------------------------------------------------------------------------------------------------------------------

class ArchiveThread : public eckit::ThreadPoolTask {
    eckit::ScopedPtr<eckit::DataHandle> handle_;
    PatchArchiver archiver_;

    virtual void execute() {
        archiver_.archive(*handle_);
    }

  public:
    ArchiveThread(eckit::DataHandle *handle, const fdb5::Key &key): handle_(handle), archiver_(key) {}
};

//----------------------------------------------------------------------------------------------------------------------

class FDBPatch : public fdb5::FDBInspect {

  public: // methods

    FDBPatch(int argc, char **argv) :
        fdb5::FDBInspect(argc, argv),
        count_(0),
        total_(0) {
        options_.push_back(new eckit::option::SimpleOption<std::string>("expver", "Set the expver"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("class", "Set the class"));
        options_.push_back(new eckit::option::SimpleOption<size_t>("threads", "Number of threads (default 1)"));
    }

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void process(const eckit::PathName &path, const eckit::option::CmdArgs &args);
    virtual void init(const eckit::option::CmdArgs &args);
    virtual void execute(const eckit::option::CmdArgs &args);

    virtual int minimumPositionalArguments() const {
        return 1;
    }

    fdb5::Key key_;
    size_t count_;
    eckit::Length total_;
    eckit::ScopedPtr<eckit::ThreadPool> pool_;
};

void FDBPatch::usage(const std::string &tool) const {
    fdb5::FDBInspect::usage(tool);
}

void FDBPatch::execute(const eckit::option::CmdArgs &args) {
    eckit::Timer timer(args.tool());
    fdb5::UMask umask(fdb5::UMask::defaultUMask());
    fdb5::FDBInspect::execute(args);

    if (pool_) {
        pool_->waitForThreads();
    }

    eckit::Log::info() << eckit::Plural(count_, "field")
                       << " ("
                       << eckit::Bytes(total_)
                       << ") copied to "
                       << key_
                       << std::endl;

    eckit::Log::info() << "Rates: "
                       << eckit::Bytes(total_, timer)
                       << ", "
                       << count_  / timer.elapsed()
                       << " fields/s"
                       << std::endl;
}

void FDBPatch::init(const eckit::option::CmdArgs &args) {

    std::string s;

    if (args.get("expver", s)) {
        std::ostringstream oss;
        oss << std::setfill('0') << std::setw(4) << s;
        key_.set("expver", oss.str());
    }

    if (args.get("class", s)) {
        key_.set("class", s);
    }

    if (key_.empty()) {
        eckit::Log::info() << "Please provide either --expver or --class" << std::endl;
        exit(1);
    }

    size_t threads = 1;
    args.get("threads", threads);

    if (threads > 1) {
        static size_t threadStackSize = eckit::Resource<size_t>("$THREAD_STACK_SIZE", 20 * 1024 * 1024);

        pool_.reset(new eckit::ThreadPool(args.tool(), threads, threadStackSize));
    }
}

void FDBPatch::process(const eckit::PathName &path, const eckit::option::CmdArgs &args) {

    eckit::Log::info() << "Listing " << path << std::endl;

    fdb5::TocHandler handler(path);
    fdb5::Key key = handler.databaseKey();
    eckit::Log::info() << "Database key " << key << std::endl;

    PatchArchiver archiver(key_);

    std::vector<fdb5::Index *> indexes = handler.loadIndexes();


    for (std::vector<fdb5::Index *>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {
        fdb5::HandleGatherer gatherer(true);
        PatchVisitor visitor(gatherer, count_, total_);
        (*i)->entries(visitor);

        if (pool_) {
            pool_->push(new ArchiveThread(gatherer.dataHandle(), key_));
        } else {
            eckit::ScopedPtr<eckit::DataHandle> handle(gatherer.dataHandle());
            archiver.archive(*handle);
        }
    }

    handler.freeIndexes(indexes);
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBPatch app(argc, argv);
    return app.start();
}


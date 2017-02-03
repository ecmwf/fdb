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

#include "eckit/config/Resource.h"

#include "fdb5/config/UMask.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/grib/GribArchiver.h"
#include "fdb5/io/HandleGatherer.h"

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
                       const fdb5::Field &field,
                       const std::string &indexFingerprint,
                       const std::string &fieldFingerprint);

    fdb5::HandleGatherer &gatherer_;
    size_t &count_;
    eckit::Length &total_;
    std::set<std::string> active_;

};

void PatchVisitor::visit(const fdb5::Index &index,
                         const fdb5::Field &field,
                         const std::string &indexFingerprint,
                         const std::string &fieldFingerprint) {

    std::string unique = indexFingerprint + "+" + fieldFingerprint;

    if (active_.find(unique) == active_.end()) {
        active_.insert(unique);

        gatherer_.add(field.dataHandle());
        count_++;
        total_ += field.location().length();
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

class FDBPatch : public fdb5::FDBInspect {

  public: // methods

    FDBPatch(int argc, char **argv) :
        fdb5::FDBInspect(argc, argv),
        count_(0),
        total_(0) {
        options_.push_back(new eckit::option::SimpleOption<std::string>("expver", "Set the expver"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("class", "Set the class"));
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
};

void FDBPatch::usage(const std::string &tool) const {
    fdb5::FDBInspect::usage(tool);
}

void FDBPatch::execute(const eckit::option::CmdArgs &args) {
    eckit::Timer timer(args.tool());
    fdb5::UMask umask(fdb5::UMask::defaultUMask());
    fdb5::FDBInspect::execute(args);

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
}

void FDBPatch::process(const eckit::PathName &path, const eckit::option::CmdArgs &args) {

    eckit::Log::info() << "Listing " << path << std::endl;

    eckit::ScopedPtr<fdb5::DB> db(fdb5::DBFactory::buildReader(path));

    eckit::Log::info() << "Database key " << db->key() << std::endl;

    PatchArchiver archiver(key_);

    const std::vector<fdb5::Index> indexes = db->indexes();

    size_t count = count_;
    eckit::Length total = total_;

    fdb5::HandleGatherer gatherer(false);

    for (std::vector<fdb5::Index>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {

        PatchVisitor visitor(gatherer, count_, total_);

        eckit::Log::info() << "Scanning" << *i << std::endl;

        i->entries(visitor);

        eckit::ScopedPtr<eckit::DataHandle> handle(gatherer.dataHandle());

        eckit::Log::info() << eckit::Plural(count_ - count, "field")
                           << " ("
                           << eckit::Bytes(total_ - total)
                           << ") to copy to "
                           << key_
                           << " from "
                           << *i
                           << std::endl;


        archiver.archive(*handle);
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBPatch app(argc, argv);
    return app.start();
}


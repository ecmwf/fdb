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
#include "fdb5/config/UMask.h"
#include "fdb5/database/Index.h"
#include "fdb5/grib/GribArchiver.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBInspect.h"


//----------------------------------------------------------------------------------------------------------------------

class PatchVisitor : public fdb5::EntryVisitor {
  public:
    PatchVisitor(fdb5::HandleGatherer &gatherer) :
        gatherer_(gatherer) {
    }

  private:
    virtual void visit(const fdb5::Index &index,
                       const std::string &indexFingerprint,
                       const std::string &fieldFingerprint,
                       const fdb5::Field &field);

    fdb5::HandleGatherer &gatherer_;
};

void PatchVisitor::visit(const fdb5::Index &index,
                         const std::string &indexFingerprint,
                         const std::string &fieldFingerprint,
                         const fdb5::Field &field) {
    gatherer_.add(field.dataHandle());
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

    FDBPatch(int argc, char **argv) : fdb5::FDBInspect(argc, argv) {
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

};

void FDBPatch::usage(const std::string &tool) const {
    fdb5::FDBInspect::usage(tool);
}

void FDBPatch::execute(const eckit::option::CmdArgs &args) {
    fdb5::UMask umask(fdb5::UMask::defaultUMask());
    fdb5::FDBInspect::execute(args);
}

void FDBPatch::init(const eckit::option::CmdArgs &args) {

    bool ok = false;
    std::string s;

    if (args.get("expver", s)) {
        std::ostringstream oss;
        oss << std::setfill('0') << std::setw(4) << s;
        key_.set("expver", oss.str());
        ok = true;
    }

     if (args.get("class", s)) {
        key_.set("class", s);
        ok = true;
    }

    if(!ok) {
        eckit::Log::info() << "Please provide either --expver or --class" << std::endl;
        exit(1);
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
        PatchVisitor visitor(gatherer);
        (*i)->entries(visitor);

        eckit::ScopedPtr<eckit::DataHandle> dh(gatherer.dataHandle());
        archiver.archive(*dh);
    }

    handler.freeIndexes(indexes);
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBPatch app(argc, argv);
    return app.start();
}


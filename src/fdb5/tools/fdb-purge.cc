/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/toc/PurgeVisitor.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBInspect.h"

//----------------------------------------------------------------------------------------------------------------------

/// Purges duplicate entries from the database and removes associated data (if owned, not adopted)

class FDBPurge : public fdb5::FDBInspect {

  public: // methods

    FDBPurge(int argc, char **argv) :
        fdb5::FDBInspect(argc, argv),
        doit_(false) {

        options_.push_back(new eckit::option::SimpleOption<bool>("doit", "Delete the files (data and indexes)"));

    }

  private: // methods

    virtual void process(const eckit::PathName &, const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual void init(const eckit::option::CmdArgs &args);
    virtual void finish(const eckit::option::CmdArgs &args);

    bool doit_;

};

void FDBPurge::usage(const std::string &tool) const {

    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " [--doit] [path1|request1] [path2|request2] ..."
                       << std::endl;
    FDBInspect::usage(tool);
}

void FDBPurge::init(const eckit::option::CmdArgs &args) {
    args.get("doit", doit_);
}

void FDBPurge::process(const eckit::PathName &path, const eckit::option::CmdArgs &args) {

    eckit::Log::info() << "Scanning " << path << std::endl;

    eckit::ScopedPtr<fdb5::DB> db(fdb5::DBFactory::buildReader(path));
    ASSERT(db->open());

    fdb5::TocDB* tocdb = dynamic_cast<fdb5::TocDB*>(db.get());
    if(!tocdb) {
        std::ostringstream oss;
        oss << "Database in " << path
            << ", expected type " << fdb5::TocDB::dbTypeName()
            << " but got type " << db->dbType();
        throw eckit::BadParameter(oss.str(), Here());
    }

    fdb5::PurgeVisitor visitor(*tocdb);

    db->visitEntries(visitor);

    visitor.report(eckit::Log::info());

    if (doit_) {
        visitor.purge(eckit::Log::info());
    }
}


void FDBPurge::finish(const eckit::option::CmdArgs &args) {



    if (!doit_) {
        eckit::Log::info() << std::endl
                           << "Rerun command with --doit flag to delete unused files"
                           << std::endl
                           << std::endl;
    }

}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBPurge app(argc, argv);
    return app.start();
}

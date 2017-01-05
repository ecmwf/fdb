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

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBInspect.h"


//----------------------------------------------------------------------------------------------------------------------

class ListVisitor : public fdb5::EntryVisitor {
  public:
    ListVisitor(const fdb5::Key &dbKey,
                const fdb5::Schema &schema,
               bool location) :
        dbKey_(dbKey),
        schema_(schema),
        location_(location) {
    }

  private:
    virtual void visit(const fdb5::Index& index,
                       const fdb5::Field& field,
                       const std::string& indexFingerprint,
                       const std::string& fieldFingerprint);

    const fdb5::Key &dbKey_;
    const fdb5::Schema &schema_;
    bool location_;
};

void ListVisitor::visit(const fdb5::Index& index,
                        const fdb5::Field& field,
                        const std::string&,
                        const std::string& fieldFingerprint) {

    fdb5::Key key(fieldFingerprint, schema_.ruleFor(dbKey_, index.key()));

    std::cout << dbKey_ << index.key() << key;

    if (location_) {
        std::cout << " " << field.location();
    }

    std::cout << std::endl;

}

//----------------------------------------------------------------------------------------------------------------------

class FDBList : public fdb5::FDBInspect {

  public: // methods

    FDBList(int argc, char **argv) :
        fdb5::FDBInspect(argc, argv),
        location_(false) {
        options_.push_back(new eckit::option::SimpleOption<bool>("location", "Also print the location of each field"));
    }

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void process(const eckit::PathName &path, const eckit::option::CmdArgs &args);
    // virtual int minimumPositionalArguments() const { return 1; }
    virtual void init(const eckit::option::CmdArgs &args);

    bool location_;

};

void FDBList::usage(const std::string &tool) const {
    fdb5::FDBInspect::usage(tool);
}

void FDBList::init(const eckit::option::CmdArgs &args) {
    args.get("location", location_);
}

void FDBList::process(const eckit::PathName &path, const eckit::option::CmdArgs &args) {

    eckit::Log::info() << "Listing " << path << std::endl;

    eckit::ScopedPtr<fdb5::DB> db(fdb5::DBFactory::build_read(path));
    ASSERT(db->open());

    ListVisitor visitor(db->key(), db->schema(), location_);

    db->visitEntries(visitor);
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBList app(argc, argv);
    return app.start();
}


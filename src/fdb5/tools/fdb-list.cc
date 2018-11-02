/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/config/Resource.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBInspect.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBToolRequest.h"

using namespace eckit;

/*
 * This is a test case
 *
 * TODO: Generalise to more cases
 *
 */

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

/*class ListVisitor : public fdb5::EntryVisitor {
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

}*/

//----------------------------------------------------------------------------------------------------------------------

class FDBList : public fdb5::FDBTool {

  public: // methods

    FDBList(int argc, char **argv) :
        fdb5::FDBTool(argc, argv),
        location_(false),
        all_(false) {

        options_.push_back(new eckit::option::SimpleOption<bool>("location", "Also print the location of each field"));
        options_.push_back(new option::SimpleOption<bool>("all", "Visit all FDB databases"));
    }

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void execute(const eckit::option::CmdArgs& args);
    // virtual int minimumPositionalArguments() const { return 1; }
    virtual void init(const eckit::option::CmdArgs &args);

    bool location_;
    bool all_;
    std::vector<std::string> requests_;

    std::vector<std::string> minimumKeys_;
};

void FDBList::usage(const std::string &tool) const {
    fdb5::FDBTool::usage(tool);
}

void FDBList::init(const option::CmdArgs& args) {
    args.get("location", location_);
    args.get("all", all_);

    if (all_ && args.count()) {
        usage(args.tool());
        exit(1);
    }

    // TODO: ignore-errors

    for (size_t i = 0; i < args.count(); ++i) {
        requests_.emplace_back(args(i));
    }
}

void FDBList::execute(const option::CmdArgs& args) {

    FDB fdb;

    if (all_) {
        ASSERT(requests_.empty());
        requests_.push_back("");
    }

    for (const std::string& request : requests_) {

        FDBToolRequest tool_request(request, all_);
        auto listObject = fdb.list(tool_request);

        FDBListElement elem;
        while (listObject.next(elem)) {
            elem.print(Log::info(), location_);
            Log::info() << std::endl;
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBList app(argc, argv);
    return app.start();
}


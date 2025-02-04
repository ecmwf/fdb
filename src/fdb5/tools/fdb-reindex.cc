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
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/tools/FDBVisitTool.h"


using namespace eckit;
using namespace eckit::option;

// Reindex the contents of one FDB into another, by listing the contents of the first and rewriting the indexes in the second
namespace fdb5::tools {

class FDBReindex: public FDBVisitTool {
public:  // methods
    FDBReindex(int argc, char** argv): FDBVisitTool(argc, argv, "class,expver") {
        options_.push_back(new SimpleOption<bool>("full", "Include all entries (including masked duplicates)"));
        options_.push_back(new SimpleOption<bool>("porcelain","Streamlined and stable output. Useful as input for other tools or scripts."));
        options_.push_back(new eckit::option::SimpleOption<std::string>("source_config", "Required: FDB configuration filename. This FDB will be listed"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("sink_config", "Required: FDB configuration filename. Indexes will be written to this FDB."));
    }

protected:
    bool needsConfig_{false}; // because we need two!

private:
    void execute(const CmdArgs& args) override;
    void init(const CmdArgs& args) override;

    bool full_ {false};
    bool porcelain_ {false};

    std::string source_config_;
    std::string sink_config_;
};

//----------------------------------------------------------------------------------------------------------------------

void FDBReindex::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    full_      = args.getBool("full", full_);
    porcelain_ = args.getBool("porcelain", porcelain_);
    source_config_ = args.getString("source_config", "");
    sink_config_ = args.getString("sink_config", "");

    // not optional
    if (source_config_.empty() || sink_config_.empty()) {
        throw UserError("Both source_config and sink_config must be specified");
    }
}

void FDBReindex::execute(const CmdArgs& args) {

    FDB source(Config::make(source_config_));
    FDB sink(Config::make(sink_config_));

    for (const FDBToolRequest& request : requests()) {

        if (LibFdb5::instance().debug()) {
            LOG_DEBUG_LIB(LibFdb5) << "Listing for request" << std::endl;
            request.print(Log::debug<LibFdb5>());
            LOG_DEBUG_LIB(LibFdb5) << std::endl;
        }

        // If --full is supplied, then include all entries including duplicates.
        auto it = source.list(request, !full_);
        ListElement elem;
        while (it.next(elem)) {
            LOG_DEBUG_LIB(LibFdb5) << "Reindexing ListElement: " << elem << std::endl;
            const FieldLocation& location = elem.location();
            const Key& key = elem.combinedKey();
            sink.reindex(key, location);
        }

        sink.flush();
    }
}
//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char **argv) {
    fdb5::tools::FDBReindex app(argc, argv);
    return app.start();
}


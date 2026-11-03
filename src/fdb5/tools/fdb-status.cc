// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit::option;
using namespace eckit;


namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBStatus : public FDBVisitTool {
public:  // methods

    FDBStatus(int argc, char** argv) : FDBVisitTool(argc, argv, "class,expver") {}

private:  // methods

    void execute(const CmdArgs& args) override;
};


void FDBStatus::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    for (const FDBToolRequest& request : requests("read")) {

        auto statusIterator = fdb.status(request);

        size_t count = 0;
        StatusElement elem;
        while (statusIterator.next(elem)) {

            Log::info() << "Database: " << elem.key << std::endl
                        << "  location: " << elem.location.asString() << std::endl;

            if (!elem.controlIdentifiers.enabled(ControlIdentifier::Retrieve)) {
                Log::info() << "  retrieve: LOCKED" << std::endl;
            }
            if (!elem.controlIdentifiers.enabled(ControlIdentifier::Archive)) {
                Log::info() << "  archive: LOCKED" << std::endl;
            }
            if (!elem.controlIdentifiers.enabled(ControlIdentifier::List)) {
                Log::info() << "  list: LOCKED" << std::endl;
            }
            if (!elem.controlIdentifiers.enabled(ControlIdentifier::Wipe)) {
                Log::info() << "  wipe: LOCKED" << std::endl;
            }
            if (!elem.controlIdentifiers.enabled(ControlIdentifier::UniqueRoot)) {
                Log::info() << "  multi-root: PERMITTED" << std::endl;
            }

            count++;
        }

        if (count == 0 && failOnNoData()) {
            std::ostringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools


int main(int argc, char** argv) {
    fdb5::tools::FDBStatus app(argc, argv);
    return app.start();
}

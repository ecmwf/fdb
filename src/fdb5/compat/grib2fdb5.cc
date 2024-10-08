/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <memory>

#include "eckit/io/DataHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/log/Log.h"

#include "fdb5/message/MessageArchiver.h"
#include "fdb5/tools/FDBTool.h"

using namespace eckit;

class Grib2Fdb5 : public fdb5::FDBTool {
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int minimumPositionalArguments() const {
        return 1;
    }

  public:
    Grib2Fdb5(int argc, char **argv): fdb5::FDBTool(argc, argv) {}
};

void Grib2Fdb5::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " [-c class] [-e expver] [-T type] [-s stream] [-f file]" << std::endl;
    fdb5::FDBTool::usage(tool);
}

void Grib2Fdb5::execute(const eckit::option::CmdArgs &args) {

    fdb5::Key check;

    std::vector<eckit::PathName> paths;

    size_t i = 0;
    while (i + 1 < args.count()) {
        std::string k = args(i);

        if (k == "-f") {
            paths.push_back(args(i + 1));

            i += 2;
            continue;
        }

        if (k == "-d") {
            Log::info() << "Option -d ignored: " << args(i + 1) << std::endl;
            i += 2;
            continue;
        }

        if (k == "-1") {
            Log::info() << "Option -1 ignored" <<  std::endl;
            i++;
            continue;
        }

        if (k == "-e") {
            check.push("expver", args(i + 1));
            i += 2;
            continue;
        }

        if (k == "-c") {
            check.push("class", args(i + 1));
            i += 2;
            continue;
        }

        if (k == "-s") {
            check.push("stream", args(i + 1));
            i += 2;
            continue;
        }

        if (k == "-T") {
            check.push("type", args(i + 1));
            i += 2;
            continue;
        }

        usage(args.tool());
        exit(1);
    }

    // Do the archiving with the relevant checks

    fdb5::MessageArchiver archiver(check);

    for (const auto& path : paths) {
        Log::info() << "Processing " << path << std::endl;
        Log::info() << "Key " << check << std::endl;
        std::unique_ptr<eckit::DataHandle> dh ( path.fileHandle() );
        archiver.archive( *dh );
    }

}


int main(int argc, char **argv) {
    Grib2Fdb5 app(argc, argv);
    return app.start();
}

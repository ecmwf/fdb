/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"
#include "eckit/log/Progress.h"
#include "eckit/log/Seconds.h"
#include "eckit/log/Timer.h"
#include "eckit/message/Message.h"
#include "eckit/message/Reader.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/api/FDB.h"
#include "fdb5/database/Key.h"
#include "fdb5/message/MessageDecoder.h"
#include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/tools/FDBTool.h"

//----------------------------------------------------------------------------------------------------------------------
namespace fdb5::tools {

class FDBAdopt : public fdb5::FDBTool {

    virtual void execute(const eckit::option::CmdArgs& args);
    virtual void usage(const std::string& tool) const;
    virtual int minimumPositionalArguments() const { return 1; }

public:

    FDBAdopt(int argc, char** argv) : fdb5::FDBTool(argc, argv) {}
};

void FDBAdopt::usage(const std::string& tool) const {
    eckit::Log::info() << std::endl << "Usage: " << tool << " gribfile1 [gribfile2] ..." << std::endl;
    fdb5::FDBTool::usage(tool);
}

void FDBAdopt::execute(const eckit::option::CmdArgs& args) {

    FDB fdb(config(args));
    fdb5::MessageDecoder decoder;

    for (size_t i = 0; i < args.count(); i++) {
        eckit::PathName path(args(i));

        if (!path.exists()) {
            throw eckit::UserError("File does not exist: " + args(i), Here());
        }
        if (path.isDir()) {
            throw eckit::UserError("Expected a file but got a directory: " + args(i), Here());
        }
        // Index GRIB file
        eckit::Log::info() << "Processing file " << path << std::endl;

        eckit::message::Reader reader(path);
        eckit::message::Message msg;

        size_t count = 0;
        eckit::Length total_size = 0;
        eckit::Length totalFileSize = path.size();
        eckit::Timer timer;
        timer.start();

        eckit::Progress progress("Scanning", 0, totalFileSize);

        eckit::PathName full(path.realName());
        eckit::URI uri("file", full);

        while ((msg = reader.next())) {
            fdb5::Key key;
            decoder.messageToKey(msg, key);

            eckit::Length length = msg.length();
            eckit::Offset offset = reader.position() - length;

            fdb.reindex(key, fdb5::TocFieldLocation{uri, offset, length, fdb5::Key()});
            total_size += length;
            progress(total_size);
            count++;
        }

        eckit::Log::info() << "FDB indexed " << eckit::Plural(count, "field") << ","
                           << " size " << eckit::Bytes(total_size) << ","
                           << " in " << eckit::Seconds(timer.elapsed()) << " (" << eckit::Bytes(total_size, timer)
                           << ")" << std::endl;
    }
}

}  // namespace fdb5::tools

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    fdb5::tools::FDBAdopt app(argc, argv);
    return app.start();
}

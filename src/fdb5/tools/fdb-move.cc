/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <dirent.h>
#include <sys/file.h>

#include "eckit/io/FileHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/thread/ThreadPool.h"

#include "fdb5/tools/FDBVisitTool.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/LibFdb5.h"

using namespace eckit::option;
using namespace eckit;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBMove : public FDBVisitTool {
public: // methods

    FDBMove(int argc, char **argv);
    ~FDBMove() override;

private: // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs &args) override;

private: // members

    eckit::PathName destination_;
};

// class for writing a chunk of the user buffer - used to perform multiple simultaneous writes
class FileCopy : public eckit::ThreadPoolTask {
    eckit::PathName src_;
    eckit::PathName dest_;

    void execute() {
        eckit::FileHandle src(src_);
        eckit::FileHandle dest(dest_);
        src.copyTo(dest);
    }

public:
    FileCopy(const eckit::PathName& srcPath, const eckit::PathName& destPath, const std::string& fileName):
        src_(srcPath / fileName), dest_(destPath / fileName) {}
};

FDBMove::FDBMove(int argc, char **argv) :
    FDBVisitTool(argc, argv, "class,expver,stream,date,time"),
    destination_("") {

    options_.push_back(new SimpleOption<std::string>("dest", "Destination root"));
}

FDBMove::~FDBMove() {}


void FDBMove::init(const CmdArgs& args) {
    FDBVisitTool::init(args);

    std::string dest = args.getString("dest");

    if (dest.empty()) {
        std::stringstream ss;
        ss << "No destination root specified.";
        throw UserError(ss.str(), Here());
    } else {
        destination_ = eckit::PathName(dest);
        if (!destination_.exists()) {
            std::stringstream ss;
            ss << "Destination root does not exist";
            throw UserError(ss.str(), Here());
        }
    }
}


void FDBMove::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    for (const FDBToolRequest& request : requests("read")) {

        auto statusIterator = fdb.control(request, ControlAction::Disable, ControlIdentifier::Archive | ControlIdentifier::Wipe | ControlIdentifier::UniqueRoot);

        size_t count = 0;
        ControlElement elem;
        while (statusIterator.next(elem)) {
            eckit::PathName db = elem.location.path().baseName(true);

            eckit::PathName dest_db = destination_ / db;
            if(dest_db.exists()) {
                std::stringstream ss;
                ss << "Destination folder " << dest_db << " already exist";
                throw UserError(ss.str(), Here());
            }
            dest_db.mkdir();

            Log::info() << "Database: " << elem.key << std::endl
                        << "  location: " << elem.location.asString() << std::endl
                        << "  new location: " << dest_db << std::endl;


            ASSERT(elem.controlIdentifiers.has(ControlIdentifier::Archive));
            ASSERT(elem.controlIdentifiers.has(ControlIdentifier::Wipe));
            ASSERT(elem.controlIdentifiers.has(ControlIdentifier::UniqueRoot));

            eckit::ThreadPool pool(elem.location.asString(), 4);

            DIR* dirp = ::opendir(elem.location.asString().c_str());
            struct dirent* dp;
            while ((dp = readdir(dirp)) != NULL) {
                if (strstr( dp->d_name, ".index")) {

                    eckit::PathName src_ = elem.location.path() / dp->d_name;
                    int fd = ::open(src_.asString().c_str(), O_RDWR);
                    if(::flock(fd, LOCK_EX)) {
                        std::stringstream ss;
                        ss << "Index file " << dp->d_name << " is locked";
                        throw UserError(ss.str(), Here());
                    }

                    eckit::FileHandle src(src_);
                    eckit::FileHandle dest(dest_db / dp->d_name);
                    src.copyTo(dest);
                }
            }
            closedir(dirp);

            dirp = ::opendir(elem.location.asString().c_str());
            while ((dp = readdir(dirp)) != NULL) {
                if (strstr( dp->d_name, ".data") ||
                    strstr( dp->d_name, "toc.") ||
                    strstr( dp->d_name, "schema")) {

                    pool.push(new FileCopy(elem.location.path(), dest_db, dp->d_name));
                }
            }
            closedir(dirp);

            pool.wait();
            pool.push(new FileCopy(elem.location.path(), dest_db, "toc"));
            pool.wait();

            count++;
        }

        if (count == 0 && fail()) {
            std::stringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }

        
    }
}
//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBMove app(argc, argv);
    return app.start();
}

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/io/s3/S3BucketName.h"
#include "eckit/io/s3/S3Name.h"
#include "eckit/io/s3/S3ObjectName.h"
#include "eckit/io/s3/S3Session.h"
#include "eckit/log/Log.h"
#include "eckit/net/Endpoint.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBTool.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>

namespace fdb5 ::tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBS3 : public FDBTool {

public:  // methods
    FDBS3(int argc, char** argv) : FDBTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<std::string>("endpoint", "S3 remote endpoint"));

        options_.push_back(new eckit::option::SimpleOption<bool>("lookup", "Lookup item"));

        options_.push_back(new eckit::option::SimpleOption<bool>("create", "Create item if it doesn't exist"));
        options_.push_back(new eckit::option::SimpleOption<std::uint64_t>("size", "Size in bytes (needed by create)"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("perm", "Permissions (needed by create)"));

        options_.push_back(new eckit::option::SimpleOption<bool>("delete", "Delete item if it exists"));
    }

private:  // methods
    void usage(const std::string& tool) const override;

    void init(const eckit::option::CmdArgs& args) override;

    void execute(const eckit::option::CmdArgs& args) override;

private:  // members
    std::string endpoint_ {"127.0.0.1:9000"};
    std::string path_;

    bool lookup_ {false};
    bool create_ {false};
    bool delete_ {false};
};

//----------------------------------------------------------------------------------------------------------------------

void FDBS3::usage(const std::string& tool) const {

    eckit::Log::info() << "\nUsage: " << tool << " [options] [path] ..." << "\n\n\nExamples:\n=========\n\n"
                       << tool << " --endpoint=127.0.0.1:9000 --lookup <path>\n\n"
                       << tool << " --endpoint=127.0.0.1:9000 --create <bucket path>\n\n"
                       << tool << " --endpoint=127.0.0.1:9000 --delete <path>\n\n";

    FDBTool::usage(tool);  // show the rest of the usage
}

//----------------------------------------------------------------------------------------------------------------------

void FDBS3::init(const eckit::option::CmdArgs& args) {
    FDBTool::init(args);

    if (args.count() != 1) {
        eckit::Log::info() << "!!! missing arguement [path] (see usage below) !!!\n";
        usage(args.tool());
        exit(1);
    }

    endpoint_ = args.getString("endpoint", endpoint_);

    lookup_ = args.getBool("lookup", lookup_);
    create_ = args.getBool("create", create_);
    delete_ = args.getBool("delete", delete_);

    path_ = args(0);

    auto cfg = config(args).getSubConfiguration("s3");
    eckit::S3Session::instance().loadClients(cfg.getString("configuration"));
    eckit::S3Session::instance().loadCredentials(cfg.getString("credentials"));
}

void FDBS3::execute(const eckit::option::CmdArgs& args) {

    auto name = eckit::S3Name::make(endpoint_, path_);

    if (auto* bucket = dynamic_cast<eckit::S3BucketName*>(name.get())) {

        if (lookup_) {
            try {
                const auto exists = bucket->exists();
                eckit::Log::info() << bucket->path() << (exists ? " is found!" : " is not found!") << std::endl;
            } catch (const eckit::Exception& e) {
                eckit::Log::info() << "Failed to lookup: " << bucket->path() << std::endl;
            }
        } else if (delete_) {
            try {
                bucket->destroy();
                eckit::Log::info() << "Deleted " << bucket->path() << std::endl;
            } catch (const eckit::Exception& e) {
                eckit::Log::info() << "Failed to delete: " << bucket->path() << std::endl;
            }
        } else if (create_) {
            try {
                bucket->create();
                eckit::Log::info() << "Created " << bucket->path() << std::endl;
            } catch (const eckit::Exception& e) {
                eckit::Log::info() << "Failed to create: " << bucket->path() << std::endl;
            }
        }

        return;
    }

    if (auto* object = dynamic_cast<eckit::S3ObjectName*>(name.get())) {

        if (lookup_) {
            try {
                const auto exists = object->exists();
                eckit::Log::info() << object->path() << (exists ? " is found!" : " is not found!") << std::endl;
            } catch (const eckit::Exception& e) {
                eckit::Log::info() << "Failed to lookup: " << object->path() << std::endl;
            }
        } else if (delete_) {
            try {
                object->remove();
                eckit::Log::info() << "Deleted " << object->path() << std::endl;
            } catch (const eckit::Exception& e) {
                eckit::Log::info() << "Failed to delete: " << object->path() << std::endl;
            }
        } else if (create_) {
            eckit::Log::info() << "The option \"--create\" requires a bucket path.\n";
            usage(args.tool());
            exit(1);
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBS3 app(argc, argv);
    return app.start();
}

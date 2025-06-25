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

#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <string>

#include "eckit/exception/Exceptions.h"
#include "eckit/io/fam/FamPath.h"
#include "eckit/io/fam/FamProperty.h"
#include "eckit/io/fam/FamRegion.h"
#include "eckit/io/fam/FamRegionName.h"
#include "eckit/log/Log.h"
#include "eckit/net/Endpoint.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBTool.h"

namespace fdb5 ::tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBFam : public FDBTool {

public:  // methods

    FDBFam(int argc, char** argv) : FDBTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<std::string>("endpoint", "FAM remote endpoint"));

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

    std::string endpoint_{"127.0.0.1:8080"};
    eckit::FamPath path_;

    bool lookup_{false};
    bool create_{false};
    bool delete_{false};

    eckit::FamProperty item_;

    bool isRegion_{true};
};

//----------------------------------------------------------------------------------------------------------------------

void FDBFam::usage(const std::string& tool) const {

    eckit::Log::info() << "\nUsage: " << tool << " [options] [path] ..." << "\n\n\nExamples:\n=========\n\n"
                       << tool << " --endpoint=127.0.0.1:8080 --lookup <my_path>\n\n"
                       << tool << " --endpoint=127.0.0.1:8080 --create --size=1024 --perm=0640 <my_path>\n\n"
                       << tool << " --endpoint=127.0.0.1:8080 --delete <my_path>\n\n";

    FDBTool::usage(tool);  // show the rest of the usage
}

//----------------------------------------------------------------------------------------------------------------------

void FDBFam::init(const eckit::option::CmdArgs& args) {
    FDBTool::init(args);

    LOG_DEBUG_LIB(LibFdb5) << "FDBFam::init" << std::endl;

    if (args.count() != 1) {
        eckit::Log::info() << "!!! missing arguement [path] (see usage below) !!!\n";
        usage(args.tool());
        exit(1);
    }

    endpoint_ = args.getString("endpoint", endpoint_);
    lookup_   = args.getBool("lookup", lookup_);
    create_   = args.getBool("create", create_);
    delete_   = args.getBool("delete", delete_);

    if (create_) {
        try {
            item_ = eckit::FamProperty{args.getUnsigned("size"), args.getString("perm", "0640")};
        }
        catch (const eckit::Exception& e) {
            eckit::Log::info() << "!!! missing option [size] (see usage below) !!!\n";
            usage(args.tool());
            exit(1);
        }
    }

    path_     = eckit::FamPath{args(0)};
    isRegion_ = path_.objectName.empty();

    LOG_DEBUG_LIB(LibFdb5) << "Item " << item_ << std::endl;
}

void FDBFam::execute(const eckit::option::CmdArgs& args) {

    if (isRegion_) {

        auto regionName = eckit::FamRegionName(endpoint_, path_);

        LOG_DEBUG_LIB(LibFdb5) << "Region [" << regionName << "] ..." << std::endl;

        if (lookup_) {
            try {
                auto region = regionName.lookup();
                eckit::Log::info() << region << std::endl;
            }
            catch (const eckit::Exception& e) {
                eckit::Log::info() << "Failed to lookup: " << regionName << std::endl;
            }
        }
        else if (delete_) {
            try {
                const auto region = regionName.lookup();
                region.destroy();
                eckit::Log::info() << "Deleted " << region << std::endl;
            }
            catch (const eckit::Exception& e) {
                eckit::Log::info() << "Failed to delete: " << regionName << std::endl;
            }
        }
        else if (create_) {
            try {
                const auto region = regionName.create(item_.size, item_.perm);
                eckit::Log::info() << "Created " << region << std::endl;
            }
            catch (const eckit::Exception& e) {
                eckit::Log::info() << "Failed to create: " << regionName << std::endl;
            }
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBFam app(argc, argv);
    return app.start();
}

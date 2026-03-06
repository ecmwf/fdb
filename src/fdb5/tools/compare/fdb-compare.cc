/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   fdb-compare.cc
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   August 2025

#include "fdb5/tools/compare/common/Types.h"
#include "fdb5/tools/compare/grib/Compare.h"
#include "fdb5/tools/compare/grib/Utils.h"
#include "fdb5/tools/compare/mars/Compare.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/runtime/Tool.h"


#include <cstring>
#include <ctime>
#include <iostream>
#include <limits>
#include <vector>

using namespace eckit;
using namespace eckit::option;
using namespace compare;

namespace fdb5::tools::compare {

class FDBCompare : public eckit::Tool {
public:  // methods

    FDBCompare(int argc, char** argv);

    virtual void run() override;

    void execute(const CmdArgs& args);
    void init(const CmdArgs& args);
    void usage(const std::string& tool) const;

private:

    std::vector<eckit::option::Option*> options_ = {};

    Options opts_;

    std::optional<std::string> config1_;
    std::optional<std::string> config2_;
    std::optional<std::string> req1String_;
    std::optional<std::string> req2String_;
    bool singleFDB_ = false;
};

//---------------------------------------------------------------------------------------------------------------------

static FDBCompare* instance_ = nullptr;

static void usage(const std::string& tool) {
    ASSERT(instance_);
    instance_->usage(tool);
}

void FDBCompare::run() {
    eckit::option::CmdArgs args(&fdb5::tools::compare::usage, options_, 0, 0);

    init(args);
    execute(args);
}

FDBCompare::FDBCompare(int argc, char** argv) : eckit::Tool(argc, argv, "FDB_HOME") {
    ASSERT(instance_ == nullptr);
    instance_ = this;

    options_.push_back(
        new SimpleOption<std::string>("config1",
                                      "Path to a FDB config. If not passed, FDB5 specific "
                                      "environment variables like FDB5_CONFIG_FILE will be evaluated. If ``--config1`` "
                                      "is not provided, ``--request1`` must be provided."));
    options_.push_back(
        new SimpleOption<std::string>("config2",
                                      "Optional: Path to a second different FDB config. Default: Same as --config1 (if "
                                      "passed), otherwise FDB5 specific environment variables are evaluated."));
    options_.push_back(
        new SimpleOption<std::string>("request1",
                                      "Optional: Specialized mars keys to request data from FDB1 or both FDBs"
                                      "e.g. ``--request1=\"class=rd,expver=1234\"``."
                                      "Allows comparing different MARS subtrees if ``--request2`` is specified with"
                                      "different arguments."));

    options_.push_back(new SimpleOption<std::string>(
        "request2",
        "Optional: Specialized mars keys to request different data from the second FDB,"
        "e.g. ``--request2=\"class=od,expver=abcd\".``"
        "Allows comparing different MARS subtrees. Only valid if ``--request1` has ben specified."));

    options_.push_back(new SimpleOption<std::string>(
        "scope",
        "[mars (default)|header-only|all] The FDBs can be compared in different scopes, 1) [mars] Mars Metadata "
        "only (default), 2) [header-only] includes Mars Key comparison and the comparison of the data headers "
        "(e.g. grib headers) 3) [all] includes Mars key and data header comparison but also the data sections up "
        "to a defined floating point tolerance."));

    options_.push_back(new SimpleOption<std::string>(
        "grib-comparison-type",
        " [hash-keys|grib-keys(default)|bit-identical] Comparing two Grib messages can be done via either "
        "(grib-comparison-type=hash-keys) in a bit-identical way with hash-keys for each sections, via grib keys "
        "in the message (grib-comparison-type=grib-keys (default)) or by directly comparing bit-identical memory "
        "segments (grib-comparison-type=bit-identical)."));

    options_.push_back(
        new SimpleOption<double>("fp-tolerance", "Floating point tolerance for comparison (default=10*EPSILON)."));

    options_.push_back(
        new SimpleOption<std::string>("mars-keys-ignore",
                                      "Format: \"Key1=Value1,Key2=Value2,...KeyN=ValueN\" All Messages that "
                                      "contain any of the defined key value pairs will be omitted."));

    options_.push_back(
        new SimpleOption<std::string>("grib-keys-select",
                                      "Format: \"Key1,Key2,Key3...KeyN\" Only the specified grib keys will be "
                                      "compared (Only effective with grib-comparison-type=grib-keys)."));

    options_.push_back(
        new SimpleOption<std::string>("grib-keys-ignore",
                                      "Format: \"Key1,Key2,Key3...KeyN\" The specified key words will be ignored (only "
                                      "effective with grib-comparison-type=grib-keys)."));


    options_.push_back(new SimpleOption<bool>(
        "verbose", "Print additional output, including a progress bar for grib message comparisons."));
}


void FDBCompare::usage(const std::string& tool) const {
    Log::info() << "FDB Comparison" << std::endl << std::endl;

    Log::info()
        << "Examples:" << std::endl
        << "=========" << std::endl
        << "Compares two entire FDBs for equality. Using the `--scope=all` options, not only the headers but also the "
           "data is compared:"
        << std::endl
        << tool << " --config1=<path/to/config1.yaml> --config2=<path/to/config2.yaml> --scope=all " << std::endl
        << std::endl

        << "Compares a specific part (class=rd,expver=1234) of two different FDBs for equality:" << std::endl
        << tool
        << " --config1=<path/to/config1.yaml> --config2=<path/to/config2.yaml> --request1=\"class=rd,expver=1234\" "
        << std::endl
        << std::endl


        << "Compares different data versions within one FDB using ``--request1`` and ``--request2``. In that case the "
           "only usable ``--grib-comparison-type`` is `grib-keys` (the default option):"
        << std::endl
        << tool
        << " --config1=<path/to/config1.yaml> --request1=\"class=od,expver=abcd\" --request2=\"class=rd,expver=1234\"  "
        << std::endl
        << std::endl

        << "Compares different data versions within the production FDB if executed on ATOS (the FDB5 config will be "
           "inferred from the environment):"
        << std::endl
        << tool << " --request1=\"class=od,expver=abcd\" --request2=\"class=rd,expver=1234\" " << std::endl
        << std::endl;
}


//---------------------------------------------------------------------------------------------------------------------


void FDBCompare::init(const CmdArgs& args) {
    // No positional args are allowed for the tool
    if (args.count() != 0) {
        usage(args.tool());
        exit(1);
    }

    if (args.has("config1")) {
        config1_ = args.getString("config1");
    }

    if (args.has("config2")) {
        config2_ = args.getString("config2");
    }
    else {
        config2_ = config1_;
    }

    opts_.scope = parseScope(args.getString("scope", "mars"));
    if (opts_.scope == Scope::HeaderOnly) {
        grib::appendDataRelevantKeys(opts_.gribKeysIgnore);
    }

    opts_.method = parseMethod(args.getString("grib-comparison-type", "grib-keys"));

    if (args.has("fp-tolerance")) {
        opts_.tolerance = args.getDouble("fp-tolerance");
    }
    else {
        opts_.tolerance = std::numeric_limits<double>::epsilon() * 10.0;
    }
    if (opts_.tolerance && opts_.tolerance < std::numeric_limits<double>::epsilon()) {
        throw UserError("The tolerance should be at least machine epsilon to compare floating point values.", Here());
    }

    std::string tmp = args.getString("mars-keys-ignore", "");
    if (!tmp.empty()) {
        opts_.ignoreMarsKeys = fdb5::Key::parse(tmp);
    }

    if (args.has("request1")) {
        req1String_ = args.getString("request1");
    }
    else if (args.has("request2")) {
        throw UserError("Option --request2 passed without specifing --request1. Either none, both or only request1.",
                        Here());
    }

    if (args.has("request2")) {
        req2String_ = args.getString("request2");
    }
    else {
        req2String_ = req1String_;
    }

    if (!config1_ && !req1String_) {
        throw UserError(
            "Either --config1 or --request1 (or both) need to be passed as minimum required arguments to prevent "
            "inadvertently expensive comparisons of large FDBs with themselves.",
            Here());
    }


    tmp = args.getString("grib-keys-select", "");
    if (!tmp.empty()) {
        if (opts_.method != Method::KeyByKey) {
            eckit::Log::info() << "The specified eccode select keys will have no effect because grib-comparison-type = "
                               << opts_.method << " and would need to be grib-keys" << std::endl;
        }
        else {
            parseKeySet(opts_.gribKeysSelect, tmp);
        }
    }

    tmp = args.getString("grib-keys-ignore", "");
    if (!tmp.empty()) {
        if (opts_.method != Method::KeyByKey) {
            eckit::Log::info() << "The specified eccode ignore keys will have no effect because grib-comparison-type = "
                               << opts_.method << " and would need to be grib-keys" << std::endl;
        }
        else {
            parseKeySet(opts_.gribKeysIgnore, tmp);
        }
    }


    opts_.verbose = args.getBool("verbose", false);
}

//---------------------------------------------------------------------------------------------------------------------

namespace {
FDB makeFDB(const PathName& config) {
    PathName configPath(config);
    if (!configPath.exists()) {
        std::ostringstream ss;
        ss << "Path " << config << " does not exist";
        throw UserError(ss.str(), Here());
    }
    if (configPath.isDir()) {
        std::ostringstream ss;
        ss << "Path " << config << " is a directory. Expecting a file";
        throw UserError(ss.str(), Here());
    }

    return FDB(Config::make(config));
}

FDB makeDefaultFDB() {
    return LibFdb5::instance().defaultConfig();
}

}  // namespace

void FDBCompare::execute(const CmdArgs& args) {
    FDB fdb1 = config1_ ? makeFDB(*config1_) : makeDefaultFDB();
    FDB fdb2 = config2_ ? makeFDB(*config2_) : makeDefaultFDB();

    if (req1String_) {
        opts_.request1 = fdb5::Key::parse(*req1String_);
    }
    if (req2String_) {
        opts_.request2 = fdb5::Key::parse(*req2String_);
    }

    const auto pickReq = [&](const std::optional<std::string>& s) -> fdb5::FDBToolRequest {
        if (s) {
            auto reqs = FDBToolRequest::requestsFromString(*s);
            if (reqs.empty()) {
                throw eckit::UserError("No valid request parsed from: " + *s, Here());
            }
            if (reqs.size() > 1) {
                throw eckit::UserError("More than one request parsed from: " + *s, Here());
            }
            return reqs[0];
        }
        // Create request listing all keys (--all option and no minimum keys)
        return FDBToolRequest(metkit::mars::MarsRequest{}, true, {});
    };

    DataIndex idx1 = assembleCompareMap(fdb1, pickReq(req1String_), opts_.ignoreMarsKeys);
    DataIndex idx2 = assembleCompareMap(fdb2, pickReq(req2String_), opts_.ignoreMarsKeys);


    if (opts_.request1 && opts_.request2) {
        opts_.marsReqDiff = requestDiff(*opts_.request1, *opts_.request2);
    }
    using ::compare::operator<<;
    eckit::Log::info() << opts_.marsReqDiff << std::endl;
    for (auto const& [key, _] : opts_.marsReqDiff) {
        for (const auto& mkey : grib::translateFromMARS(key)) {
            opts_.gribKeysIgnore.insert(mkey);
        }
    }

    auto handleRes = [](const Result& res) {
        // Print results
        eckit::Log::info() << "****************** SUMMARY ********************" << std::endl;
        eckit::Log::info() << res << std::endl;

        if (!res.match) {
            throw eckit::Exception("FDBs differ from each other", Here());
        }
    };

    // First compare the FDB key (MARS)
    auto res = mars::compareMarsKeys(idx1, idx2, opts_);
    if (!res.match) {
        eckit::Log::error() << "[MARS KEYS COMPARE] MISMATCH\n";
        handleRes(res);
    }

    // Second compare the message (GRIB)
    if (opts_.scope != Scope::Mars) {
        eckit::Log::info() << "[GRIB COMPARISON LOG]" << "Compare Grib messages" << std::endl;


        auto gribRes = grib::compareGrib(idx1, idx2, opts_);
        res.update(gribRes);
    }

    handleRes(res);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools::compare

int main(int argc, char** argv) {
    fdb5::tools::compare::FDBCompare app(argc, argv);
    return app.start();
}

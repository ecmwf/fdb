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

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"


#include "eckit/filesystem/PathName.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"


#include <cstring>
#include <ctime>
#include <iostream>
#include <limits>
#include <vector>

using namespace eckit;
using namespace eckit::option;
using namespace compare;

namespace fdb5::tools {


class FDBCompare : public FDBVisitTool {
public:  // methods

    FDBCompare(int argc, char** argv) : FDBVisitTool(argc, argv, "class,expver") {
        options_.push_back(new SimpleOption<std::string>("test-config", "Path to a FDB config."));
        options_.push_back(new SimpleOption<std::string>("reference-config", "Path to a second FDB config."));
        options_.push_back(new SimpleOption<std::string>(
            "reference-request",
            "Specialized mars keys to request data from the reference FDB, e.g. "
            "--reference-request=\"class=od,expver=abcd\". Allows comparing different MARS subtrees."));

        options_.push_back(new SimpleOption<std::string>(
            "test-request",
            "Specialized mars keys to request data from the test FDB, e.g. --test-request=\"class=rd,expver=1234\". "
            "Allows comparing different MARS subtrees."));

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
            "in the "
            "reference FDB (grib-comparison-type=grib-keys (default))  or by directly comparing bit-identical memory "
            "segments (grib-comparison-type=bit-identical)."));

        options_.push_back(new SimpleOption<double>(
            "fp-tolerance", "Floating point tolerance for comparison default=machine tolerance epsilon."));

        options_.push_back(
            new SimpleOption<std::string>("mars-keys-ignore",
                                          "Format: \"Key1=Value1,Key2=Value2,...KeyN=ValueN\" All Messages that "
                                          "contain any of the defined key value pairs will be omitted."));

        options_.push_back(
            new SimpleOption<std::string>("grib-keys-select",
                                          "Format: \"Key1,Key2,Key3...KeyN\" Only the specified grib keys will be "
                                          "compared (Only effective with grib-comparison-type=grib-keys)."));

        options_.push_back(new SimpleOption<std::string>(
            "grib-keys-ignore",
            "Format: \"Key1,Key2,Key3...KeyN\" The specified key words will be ignored (only "
            "effective with grib-comparison-type=grib-keys)."));


        options_.push_back(new SimpleOption<bool>(
            "verbose", "Print additional output, including a progress bar for grib message comparisons. "));
    }

private:  // methods

    virtual void execute(const CmdArgs& args) override;
    virtual void init(const CmdArgs& args) override;
    virtual void usage(const std::string& tool) const override;

    Options opts_;

    std::string testConfig_;
    std::string referenceConfig_;
    std::optional<std::string> refReqString_;
    std::optional<std::string> testReqString_;
    bool singleFDB_ = false;
};

//---------------------------------------------------------------------------------------------------------------------


void FDBCompare::init(const CmdArgs& args) {
    FDBVisitTool::init(args);

    testConfig_      = args.getString("test-config", "");
    referenceConfig_ = args.getString("reference-config", "");
    if (testConfig_.empty() && referenceConfig_.empty()) {
        singleFDB_ = true;
    }

    opts_.scope = parseScope(args.getString("scope", "mars"));
    if (opts_.scope == Scope::HeaderOnly) {
        grib::appendDataRelevantKeys(opts_.gribKeysIgnore);
    }

    opts_.method = parseMethod(args.getString("grib-comparison-type", "grib-keys"));

    if (args.has("fp-tolerance")) {
        opts_.tolerance = args.getDouble("fp-tolerance");
    }
    if (opts_.tolerance && opts_.tolerance < std::numeric_limits<double>::epsilon()) {
        throw UserError("The tolerance should be at least machine epsilon to compare floating point values.", Here());
    }

    std::string tmp = args.getString("mars-keys-ignore", "");
    if (!tmp.empty()) {
        opts_.ignoreMarsKeys = fdb5::Key::parse(tmp);
    }

    if (args.has("reference-request")) {
        refReqString_ = args.getString("reference-request");
    }

    if (args.has("test-request")) {
        testReqString_ = args.getString("test-request");
    }

    if ((testReqString_ && !refReqString_) || (!testReqString_ && refReqString_)) {
        throw UserError("Options --reference-request and --test-request must either both be set or both be omitted.",
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
    PathName configPathref(config);
    if (!configPathref.exists()) {
        std::ostringstream ss;
        ss << "Path " << config << " does not exist";
        throw UserError(ss.str(), Here());
    }
    if (configPathref.isDir()) {
        std::ostringstream ss;
        ss << "Path " << config << " is a directory. Expecting a file";
        throw UserError(ss.str(), Here());
    }

    return FDB(Config::make(config));
}

struct CompFDB {
    FDB ref;
    FDB test;
};

}  // namespace

void FDBCompare::execute(const CmdArgs& args) {
    CompFDB fdb = ([&]() -> CompFDB {
        if (!singleFDB_) {
            return {makeFDB(referenceConfig_), makeFDB(testConfig_)};
        }
        else {
            return {FDB(config(args)), FDB(config(args))};
        }
    })();

    if (refReqString_) {
        opts_.referenceRequest = fdb5::Key::parse(*refReqString_);
    }
    if (testReqString_) {
        opts_.testRequest = fdb5::Key::parse(*testReqString_);
    }

    const auto pickReq = [&](const std::optional<std::string>& s) -> fdb5::FDBToolRequest {
        if (s) {
            return FDBToolRequest::requestsFromString(*s)[0];
        }
        return requests()[0];
    };

    DataIndex refIdx  = assembleCompareMap(fdb.ref, pickReq(refReqString_), opts_.ignoreMarsKeys);
    DataIndex testIdx = assembleCompareMap(fdb.test, pickReq(testReqString_), opts_.ignoreMarsKeys);


    if (opts_.referenceRequest && opts_.testRequest) {
        opts_.marsReqDiff = requestDiff(*opts_.referenceRequest, *opts_.testRequest);
    }
    using compare::operator<<;
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
    auto res = mars::compareMarsKeys(refIdx, testIdx, opts_);
    if (!res.match) {
        eckit::Log::error() << "[MARS KEYS COMPARE] MISMATCH\n";
        handleRes(res);
    }

    // Second compare the message (GRIB)
    if (opts_.scope != Scope::Mars) {
        eckit::Log::info() << "[GRIB COMPARISON LOG]" << "Compare Grib messages" << std::endl;


        auto gribRes = grib::compareGrib(refIdx, testIdx, opts_);
        res.update(gribRes);
    }

    handleRes(res);
}

//---------------------------------------------------------------------------------------------------------------------

void fdb5::tools::FDBCompare::usage(const std::string& tool) const {

    // derived classes should provide this type of usage information ...

    Log::info() << "FDB Comparison" << std::endl << std::endl;

    Log::info() << "Examples:" << std::endl
                << "=========" << std::endl
                << std::endl
                << tool
                << " --reference-config=<path/to/reference/config.yaml> --test-config=<path/to/test/config.yaml "
                   "--minimum-keys=\"\" --all"
                << std::endl
                << std::endl;

    FDBVisitTool::usage(tool);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBCompare app(argc, argv);
    return app.start();
}

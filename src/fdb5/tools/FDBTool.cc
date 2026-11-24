// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <cstdlib>

#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBTool.h"
#include "fdb5/tracing/TraceContext.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static FDBTool* instance_ = nullptr;

FDBTool::FDBTool(int argc, char** argv) : eckit::Tool(argc, argv, "FDB_HOME") {
    ASSERT(instance_ == nullptr);
    instance_ = this;
}

static void usage(const std::string& tool) {
    ASSERT(instance_);
    instance_->usage(tool);
}

void FDBTool::run() {
    if (needsConfig_) {
        options_.push_back(new eckit::option::SimpleOption<std::string>("config", "FDB configuration filename"));
    }

    options_.push_back(new eckit::option::SimpleOption<std::string>(
        "traceparent",
        "W3C trace context (OpenTelemetry) under which FDB calls are recorded. Default: $TRACEPARENT/$TRACESTATE"));

    eckit::option::CmdArgs args(&fdb5::usage, options_, numberOfPositionalArguments(), minimumPositionalArguments());

    tracing::ScopedContext traceScope(traceContext(args));

    init(args);
    execute(args);
    finish(args);
}

tracing::TraceContext FDBTool::traceContext(const eckit::option::CmdArgs& args) {
    const char* tracestate = ::getenv("TRACESTATE");

    std::string traceparent = args.getString("traceparent", "");
    if (!traceparent.empty()) {
        // explicitly requested: a malformed value is an error
        return tracing::TraceContext::fromTraceparent(traceparent, tracestate ? tracestate : "");
    }

    // OpenTelemetry environment carrier: as for any propagated context, a malformed value is ignored
    if (const char* env = ::getenv("TRACEPARENT"); env && *env) {
        tracing::TraceContext ctx = tracing::TraceContext::parse(env, tracestate ? tracestate : "");
        if (!ctx.valid()) {
            eckit::Log::warning() << "Ignoring invalid TRACEPARENT '" << env << "'" << std::endl;
        }
        return ctx;
    }
    return {};
}

Config FDBTool::config(const eckit::option::CmdArgs& args, const eckit::Configuration& appConfig) {

    if (args.has("config")) {
        std::string config = args.getString("config", "");
        if (config.empty()) {
            throw eckit::UserError("Missing config file name", Here());
        }
        eckit::PathName configPath(config);
        if (!configPath.exists()) {
            std::ostringstream ss;
            ss << "Path " << config << " does not exist";
            throw eckit::UserError(ss.str(), Here());
        }
        if (configPath.isDir()) {
            std::ostringstream ss;
            ss << "Path " << config << " is a directory. Expecting a file";
            throw eckit::UserError(ss.str(), Here());
        }
        return Config::make(configPath, appConfig);
    }

    return LibFdb5::instance().defaultConfig(appConfig);
}

std::vector<Key> FDBTool::parse(const std::string& request, const fdb5::Config& config) {
    auto dbrequests = FDBToolRequest::requestsFromString(request, {}, false, "read");
    ASSERT(dbrequests.size() == 1);

    const auto& dbrequest = dbrequests.front();
    ASSERT(!dbrequest.all());

    return config.schema().expandDatabase(dbrequest.request());
}

void FDBTool::usage(const std::string&) const {}

void FDBTool::init(const eckit::option::CmdArgs&) {}

void FDBTool::finish(const eckit::option::CmdArgs&) {}

//----------------------------------------------------------------------------------------------------------------------

FDBToolException::FDBToolException(const std::string& w) : Exception(w) {}

FDBToolException::FDBToolException(const std::string& w, const eckit::CodeLocation& l) : Exception(w, l) {}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

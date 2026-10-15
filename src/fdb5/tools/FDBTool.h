// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   FDBTool.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include <vector>

#include "eckit/config/Configuration.h"
#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/runtime/Tool.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

namespace eckit {
namespace option {
class Option;
class CmdArgs;
}  // namespace option
}  // namespace eckit

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Base class for FDB tools
/// Must derive to instanciate

class FDBTool : public eckit::Tool {

protected:  // methods

    FDBTool(int argc, char** argv);
    ~FDBTool() override {}

    void run() override;

public:  // methods

    static Config config(const eckit::option::CmdArgs& args,
                         const eckit::Configuration& userConfig = eckit::LocalConfiguration());

    virtual void usage(const std::string& tool) const;

protected:  // members

    std::vector<eckit::option::Option*> options_ = {};
    /// Set this to false in tool subclass if your tool does not require access to 'config.yaml'
    bool needsConfig_{true};

protected:  // methods

    virtual void init(const eckit::option::CmdArgs& args);
    virtual void finish(const eckit::option::CmdArgs& args);
    std::vector<Key> parse(const std::string& request, const Config& config);

private:  // methods

    virtual void execute(const eckit::option::CmdArgs& args) = 0;

    virtual int numberOfPositionalArguments() const { return -1; }
    virtual int minimumPositionalArguments() const { return -1; }
};

//----------------------------------------------------------------------------------------------------------------------


class FDBToolException : public eckit::Exception {
public:

    FDBToolException(const std::string&);
    FDBToolException(const std::string&, const eckit::CodeLocation&);
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


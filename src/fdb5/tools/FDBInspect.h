// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   FDBInspect.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Mar 2016

#pragma once

#include "fdb5/tools/FDBTool.h"

#include "eckit/exception/Exceptions.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBInspect : public FDBTool {

protected:  // methods

    FDBInspect(int argc, char** argv, std::string minimunKeys = std::string());

    virtual void usage(const std::string& tool) const;

    virtual void init(const eckit::option::CmdArgs& args);

    virtual void execute(const eckit::option::CmdArgs& args);

    bool fail() const;

private:  // methods

    virtual void process(const eckit::PathName&, const eckit::option::CmdArgs& args) = 0;

protected:  // members

    // minimum set of keys needed to execute a query
    // these are used for safety to restrict the search space for the visitors
    std::vector<std::string> minimumKeys_;

    // When processing elements, do we fail on error, or report warning and continue?
    bool fail_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

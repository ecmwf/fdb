// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Simon Smart
/// @date   November 2018

#pragma once

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBTool.h"

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBVisitTool : public FDBTool {

protected:  // methods

    FDBVisitTool(int argc, char** argv, std::string minimunKeys = std::string());
    ~FDBVisitTool() override;

    void usage(const std::string& tool) const override;

    void init(const eckit::option::CmdArgs& args) override;

    void run() override;

    bool fail() const;
    bool failOnNoData() const;

    std::vector<FDBToolRequest> requests(const std::string& verb = "retrieve") const;

private:  // members

    // minimum set of keys needed to execute a query
    // these are used for safety to restrict the search space for the visitors
    std::vector<std::string> minimumKeys_;

    std::vector<std::string> requests_;

    // Fail on errors?
    bool fail_;
    bool ignoreNoData_;

    bool all_;

    // Don't apply contextual exapansion on mars requests.
    bool raw_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

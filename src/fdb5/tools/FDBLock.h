// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Simon Smart
/// @date   July 2019

#pragma once

#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit::option;
using namespace eckit;


namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBLock : public FDBVisitTool {
public:  // methods

    FDBLock(int argc, char** argv, bool unlock = false);
    ~FDBLock() override;

private:  // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs& args) override;

private:  // members

    bool unlock_;

    bool list_;
    bool retrieve_;
    bool archive_;
    bool wipe_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools


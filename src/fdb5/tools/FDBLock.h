/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   July 2019

#ifndef fdb5_fdb_lock_H
#define fdb5_fdb_lock_H

#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit::option;
using namespace eckit;


namespace fdb5 {
namespace tools {

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

}  // namespace tools
}  // namespace fdb5

#endif

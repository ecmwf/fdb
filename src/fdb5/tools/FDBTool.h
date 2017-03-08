/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   FDBTool.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_FDBTool_H
#define fdb5_FDBTool_H

#include "eckit/runtime/Tool.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/database/DB.h"
#include "eckit/option/SimpleOption.h"

namespace eckit {
    namespace option {
    class Option;
    class CmdArgs;
    }
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBTool : public eckit::Tool {

protected: // methods

    FDBTool(int argc, char **argv);

public:
    virtual void usage(const std::string &tool) const = 0;

protected: // members

    std::vector<eckit::option::Option *> options_;

private: // methods

    virtual void init(const eckit::option::CmdArgs& args);
    virtual void execute(const eckit::option::CmdArgs& args) = 0;
    virtual void finish(const eckit::option::CmdArgs& args);

    virtual int numberOfPositionalArguments() const { return -1; }
    virtual int minimumPositionalArguments() const { return -1; }

    virtual void run();

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

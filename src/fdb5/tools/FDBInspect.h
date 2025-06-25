/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   FDBInspect.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Mar 2016

#ifndef fdb5_FDBInspect_H
#define fdb5_FDBInspect_H

#include "eckit/exception/Exceptions.h"

#include "fdb5/tools/FDBTool.h"

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

#endif

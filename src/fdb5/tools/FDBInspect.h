/*
 * (C) Copyright 1996-2017 ECMWF.
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

#include "fdb5/tools/FDBTool.h"

#include "eckit/exception/Exceptions.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBInspect : public FDBTool {

protected: // methods

    FDBInspect(int argc, char **argv, std::string defaultMinimunKeySet = std::string());

    virtual void usage(const std::string &tool) const = 0;

    virtual void init(const eckit::option::CmdArgs& args);

    virtual void execute(const eckit::option::CmdArgs& args);

    bool fail() const;

private: // methods

    virtual void process(const eckit::PathName&, const eckit::option::CmdArgs& args) = 0;

private: // members

    std::vector<std::string> minimumKeySet_;

    bool fail_;
};

//----------------------------------------------------------------------------------------------------------------------


class InspectFailException : public eckit::Exception {
public:
    InspectFailException(const std::string&);
    InspectFailException(const std::string&, const eckit::CodeLocation&);
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

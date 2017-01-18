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
/// @date   Mar 2016

#ifndef fdb5_FDBInspect_H
#define fdb5_FDBInspect_H

#include "fdb5/tools/FDBTool.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBInspect : public FDBTool {

protected: // methods

    FDBInspect(int argc, char **argv, const std::vector<std::string>& minimumKeySet = std::vector<std::string>());

    virtual void usage(const std::string &tool) const = 0;

    virtual void execute(const eckit::option::CmdArgs& args);

private: // methods

    virtual void process(const eckit::PathName&, const eckit::option::CmdArgs& args) = 0;

     std::vector<std::string> minimumKeySet_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

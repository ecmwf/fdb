/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   FDBAccess.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_FDBAccess_H
#define fdb5_FDBAccess_H

#include "fdb5/tools/FDBTool.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBAccess : public FDBTool {

protected: // methods

    FDBAccess(int argc, char **argv);


public:
    virtual void usage(const std::string &tool);

protected: // members


private: // methods


};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

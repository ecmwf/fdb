/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   ParamHandler.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_ParamHandler_H
#define fdb5_ParamHandler_H

#include "fdb5/KeywordHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class ParamHandler : public KeywordHandler {

public: // methods

    ParamHandler(const std::string& name);

    virtual ~ParamHandler();

    virtual void getValues(const MarsRequest& request,
                           const std::string& keyword,
                           eckit::StringList& values) const;

    virtual Op* makeOp(const MarsTask& task, Op& parent) const;

private: // methods

    virtual void print( std::ostream& out ) const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

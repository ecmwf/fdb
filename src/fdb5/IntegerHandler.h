/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   IntegerHandler.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_IntegerHandler_H
#define fdb5_IntegerHandler_H

#include "fdb5/KeywordHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class IntegerHandler : public KeywordHandler {

public: // methods

    IntegerHandler(const std::string& name);

    virtual ~IntegerHandler();

    virtual void getValues(const MarsRequest& request,
                           const std::string& keyword,
                           eckit::StringList& values,
                           const MarsTask& task,
                           const DB* db) const;

private: // methods

    virtual void print( std::ostream& out ) const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   DoubleHandler.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_DoubleHandler_H
#define fdb5_DoubleHandler_H

#include "fdb5/KeywordHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DoubleHandler : public KeywordHandler {

public: // methods

    DoubleHandler(const std::string& name);

    virtual ~DoubleHandler();

    virtual void toKey(std::ostream& out,
                       const std::string& keyword,
                       const std::string& value) const ;

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

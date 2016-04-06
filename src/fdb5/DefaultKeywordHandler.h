/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   DefaultKeywordHandler.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_DefaultKeywordHandler_H
#define fdb5_DefaultKeywordHandler_H

#include "fdb5/KeywordHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DefaultKeywordHandler : public KeywordHandler {

public: // methods

    DefaultKeywordHandler();
    
    virtual ~DefaultKeywordHandler();

    virtual Op* makeOp(const MarsTask& task, Op& parent) const;

public: // class methods

    static const char* name();

private: // methods

    virtual void print( std::ostream& out ) const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

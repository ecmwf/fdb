/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   KeywordType.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_KeywordType_H
#define fdb5_KeywordType_H

#include <string>

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"

namespace fdb5 {

class KeywordHandler;

//----------------------------------------------------------------------------------------------------------------------

/// A self-registering factory for producing KeywordType instances

class KeywordType {

    std::string name_;

    virtual KeywordHandler* make(const std::string& keyword) const = 0 ;

protected:

    KeywordType(const std::string&);
    virtual ~KeywordType();

public:

    static void list(std::ostream &);
    static KeywordHandler* build(const std::string& name, const std::string& keyword);

};

/// Templated specialisation of the self-registering factory,
/// that does the self-registration, and the construction of each object.

template< class T>
class KeywordHandlerBuilder : public KeywordType {

    virtual KeywordHandler* make(const std::string& keyword) const {
        return new T(keyword);
    }

public:
    KeywordHandlerBuilder(const std::string& name) : KeywordType(name) {}
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

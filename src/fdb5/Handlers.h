/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Handlers.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_Handlers_H
#define fdb5_Handlers_H

#include <string>
#include <map>

#include "eckit/memory/NonCopyable.h"

namespace fdb5 {

class KeywordHandler;

//----------------------------------------------------------------------------------------------------------------------

class Handlers : private eckit::NonCopyable {

public: // methods

    Handlers();

    ~Handlers();

    const KeywordHandler& lookupHandler(const std::string& keyword) const;

    void addType(const std::string&, const std::string&);


private: // members

    typedef std::map<std::string, KeywordHandler*> HandlerMap;

    mutable HandlerMap handlers_;

    std::map<std::string, std::string> types_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

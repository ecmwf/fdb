/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"

#include "fdb5/Schema.h"
#include "fdb5/KeywordType.h"
#include "fdb5/KeywordHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Schema::Schema()
{
}

Schema::~Schema()
{
    for(HandlerMap::iterator i = handlers_.begin(); i != handlers_.end(); ++i) {
        delete (*i).second;
    }
}

const KeywordHandler& Schema::lookupHandler(const std::string& keyword) const
{
    std::map<std::string, KeywordHandler*>::const_iterator j = handlers_.find(keyword);

    if (j != handlers_.end()) {
        return *(*j).second;
    }
    else {
        std::string type = Resource<std::string>(keyword+"Type","Default");
        KeywordHandler* newKH = KeywordType::build(type, keyword);
        handlers_[keyword] = newKH;
        return *newKH;
    }
}

std::ostream& operator<<(std::ostream& s, const Schema& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"

#include "fdb5/KeyCollector.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

KeyCollector::~KeyCollector()
{
}

void KeyCollector::enter(const std::string& , const std::string& )
{
}

void KeyCollector::leave()
{
}

void KeyCollector::enter(std::vector<Key>& keys)
{

}

void KeyCollector::leave(std::vector<Key>& keys)
{

}

void KeyCollector::values(const MarsRequest& request, const std::string& keyword, StringList& values)
{
    NOTIMP;
}

std::ostream& operator<<(std::ostream& s, const KeyCollector& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

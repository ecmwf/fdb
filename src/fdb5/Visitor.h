/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Visitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_Visitor_H
#define fdb5_Visitor_H

#include <iosfwd>
#include <vector>

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"

class MarsRequest;

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class Visitor : public eckit::NonCopyable {
public: // methods

    virtual ~Visitor();

    virtual void selectDatabase(const Key& key) = 0;
    virtual void selectIndex(const Key& key) = 0;
    virtual void selectDatum(const Key& key) = 0;


    virtual void values(const MarsRequest& request,
                        const std::string& keyword,
                        eckit::StringList& values) = 0;

    virtual void enter(const std::string& keyword, const std::string& value);
    virtual void leave();

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

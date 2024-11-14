/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Matcher.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_Matcher_H
#define fdb5_Matcher_H

#include <iosfwd>
#include <vector>

#include "eckit/memory/NonCopyable.h"

class MarsTask;
namespace metkit {
namespace mars {
    class MarsRequest;
}
}

namespace fdb5 {

class BaseKey;
class Key;
class TypesRegistry;

//----------------------------------------------------------------------------------------------------------------------

class Matcher : public eckit::NonCopyable {

public: // methods

    Matcher();

    virtual ~Matcher();

    virtual bool optional() const;

    virtual const std::string &value(const Key& , const std::string &keyword) const;
    virtual const std::vector<std::string>& values(const metkit::mars::MarsRequest& rq, const std::string& keyword) const;
    virtual const std::string &defaultValue() const;

    virtual bool match(const std::string &keyword, const Key& key) const = 0;
    virtual void fill(BaseKey& key, const std::string &keyword, const std::string& value) const;


    virtual void dump(std::ostream &s, const std::string &keyword, const TypesRegistry &registry) const = 0;

    friend std::ostream &operator<<(std::ostream &s, const Matcher &x);

private: // methods

    virtual void print( std::ostream &out ) const = 0;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

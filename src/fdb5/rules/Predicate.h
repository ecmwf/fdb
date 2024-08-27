/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Predicate.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_Predicate_H
#define fdb5_Predicate_H

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "eckit/memory/NonCopyable.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Key;
class Matcher;
class TypesRegistry;

//----------------------------------------------------------------------------------------------------------------------

class Predicate : public eckit::NonCopyable {

public: // methods

    Predicate(const std::string &keyword, Matcher *matcher);

    ~Predicate();

    bool match(const Key &key) const;
    bool match(const std::string& value) const;

    void dump( std::ostream &s, const TypesRegistry &registry ) const;
    void fill(Key &key, const std::string& value) const;

    const std::string &value(const Key &key) const;
    const std::vector<std::string>& values(const metkit::mars::MarsRequest& rq) const;
    const std::string &defaultValue() const;

    bool optional() const;

    const std::string& keyword() const;

private: // methods

    friend std::ostream &operator<<(std::ostream &s, const Predicate &x);

    void print( std::ostream &out ) const;

private: // members

    std::unique_ptr<Matcher> matcher_;

    std::string keyword_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

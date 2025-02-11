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

#include "eckit/serialisation/Streamable.h"

#include "fdb5/rules/Matcher.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Key;
// class Matcher;
class TypesRegistry;

//----------------------------------------------------------------------------------------------------------------------

class Predicate : public eckit::Streamable {

public:  // methods

    Predicate(std::string keyword, Matcher* matcher);

    explicit Predicate(eckit::Stream& stream);


    /// @note this calls find() on key; prefer match(value)
    bool match(const Key& key) const;

    bool match(const std::string& value) const;

    void dump(std::ostream& out, const TypesRegistry& registry) const;

    void fill(Key& key, const std::string& value) const;

    const std::string& value(const Key& key) const;

    const std::vector<std::string>& values(const metkit::mars::MarsRequest& rq) const;

    const std::string& defaultValue() const;

    bool optional() const;

    const std::string& keyword() const { return keyword_; }

    // streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:  // methods

    void encode(eckit::Stream& out) const override;

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& out, const Predicate& predicate);

private:  // members

    std::string keyword_;

    std::unique_ptr<Matcher> matcher_;

    // streamable

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<Predicate> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

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

#include "eckit/serialisation/Streamable.h"

class MarsTask;

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Key;
class TypesRegistry;

//----------------------------------------------------------------------------------------------------------------------

class Matcher : public eckit::Streamable {

public:  // methods

    Matcher() = default;

    Matcher(eckit::Stream& stream);

    virtual bool optional() const { return false; }

    virtual const std::string& value(const Key&, const std::string& keyword) const;
    virtual const std::vector<std::string>& values(const metkit::mars::MarsRequest& rq,
                                                   const std::string& keyword) const;
    virtual const std::string& defaultValue() const;

    virtual bool match(const std::string& value) const                   = 0;
    virtual bool match(const std::string& keyword, const Key& key) const = 0;
    virtual void fill(Key& key, const std::string& keyword, const std::string& value) const;

    virtual void dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const = 0;

    friend std::ostream& operator<<(std::ostream& s, const Matcher& x);

    // streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:  // methods

    void encode(eckit::Stream& out) const override;

    virtual void print(std::ostream& out) const = 0;

private:  // members

    // streamable

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<Matcher> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

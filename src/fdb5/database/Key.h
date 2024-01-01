/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Key.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_Key_H
#define fdb5_Key_H

#include <map>
#include <string>
#include <vector>
#include <set>

#include "eckit/types/Types.h"

namespace eckit {
    class JSON;
    template<class T> class DenseSet;
}

namespace metkit {
namespace mars {
    class MarsRequest;
}
}

namespace fdb5 {

class TypesRegistry;
class Rule;

//----------------------------------------------------------------------------------------------------------------------

class Key {

public: // methods

    Key();

    explicit Key(eckit::Stream &);
    explicit Key(const std::string &request);
    explicit Key(const std::string &keys, const Rule* rule);

    explicit Key(const eckit::StringDict &keys);

    std::set<std::string> keys() const;

    void set(const std::string &k, const std::string &v);
    void unset(const std::string &k);

    void push(const std::string &k, const std::string &v);
    void pop(const std::string &k);

    const std::string& get( const std::string &k ) const;

    void clear();

    bool match(const Key& other) const;
    bool match(const metkit::mars::MarsRequest& request) const;

    bool match(const Key& other, const eckit::StringList& ignore) const;

    bool match(const std::string& key, const std::set<std::string>& values) const;
    bool match(const std::string& key, const eckit::DenseSet<std::string>& values) const;

    /// test that, if keys are present in the supplied request, they match the
    /// keys present in the key. Essentially implements a reject-filter
    bool partialMatch(const metkit::mars::MarsRequest& request) const;

    bool operator< (const Key &other) const {
        return keys_ < other.keys_;
    }

    bool operator!= (const Key &other) const {
        return keys_ != other.keys_;
    }

    bool operator== (const Key &other) const {
        return keys_ == other.keys_;
    }

    friend std::ostream& operator<<(std::ostream &s, const Key &x) {
        x.print(s);
        return s;
    }

    friend eckit::Stream& operator<<(eckit::Stream &s, const Key &x) {
        x.encode(s);
        return s;
    }

    friend eckit::Stream& operator>>(eckit::Stream& s, Key& x) {
        x = Key(s);
        return s;
    }

    friend eckit::JSON& operator<<(eckit::JSON& s, const Key& k) {
        k.json(s);
        return s;
    }

    void rule(const Rule *rule);
    const Rule *rule() const;
    const TypesRegistry& registry() const;

    std::string valuesToString() const;

    const eckit::StringList& names() const;

    std::string value(const std::string& keyword) const;
    std::string canonicalValue(const std::string& keyword) const;

    typedef eckit::StringDict::const_iterator const_iterator;
    typedef eckit::StringDict::const_reverse_iterator const_reverse_iterator;

    const_iterator begin() const { return keys_.begin(); }
    const_iterator end() const { return keys_.end(); }

    const_reverse_iterator rbegin() const { return keys_.rbegin(); }
    const_reverse_iterator rend() const { return keys_.rend(); }

    const_iterator find(const std::string& s) const { return keys_.find(s); }

    size_t size() const { return keys_.size(); }

    bool empty() const { return keys_.empty(); }

    /// @throws When "other" doesn't contain all the keys of "this"
    void validateKeysOf(const Key& other, bool checkAlsoValues = false) const;

    const eckit::StringDict& keyDict() const;

    metkit::mars::MarsRequest request(std::string verb = "retrieve") const;

    operator std::string() const;

    operator eckit::StringDict() const;

private: // members

    //TODO add unit test for each type
    std::string canonicalise(const std::string& keyword, const std::string& value) const;

    void print( std::ostream &out ) const;
    void json(eckit::JSON& s) const;
    void decode(eckit::Stream& s);
    void encode(eckit::Stream &s) const;

    std::string toString() const;

    eckit::StringDict keys_;
    eckit::StringList names_;

    const Rule *rule_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

namespace std {
    template <>
    struct hash<fdb5::Key> {
        size_t operator() (const fdb5::Key& key) const {
            return std::hash<std::string>()(key.valuesToString());
        }
    };
}

#endif

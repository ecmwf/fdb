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
#include <memory>

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

class BaseKey {

public: // methods

    // explicit BaseKey(const std::shared_ptr<TypesRegistry> reg = nullptr, bool canonical = false);
    // explicit BaseKey(eckit::Stream &, const std::shared_ptr<TypesRegistry> reg = nullptr);
    // explicit BaseKey(const std::string &keys, const Rule* rule);
    // explicit BaseKey(const eckit::StringDict &keys, const std::shared_ptr<TypesRegistry> reg=nullptr);
    // BaseKey(std::initializer_list<std::pair<const std::string, std::string>>, const std::shared_ptr<TypesRegistry> reg=nullptr);

    BaseKey() : keys_(), names_() {}
    BaseKey(const BaseKey &key) : keys_(key.keys_), names_(key.names_) {}

    explicit BaseKey(const eckit::StringDict &keys) : keys_(keys) {
        for (const auto& k : keys) {
            names_.emplace_back(k.first);
        }
    }
    BaseKey(std::initializer_list<std::pair<const std::string, std::string>> l) : keys_(l) {
        for (const auto& k : l) {
            names_.emplace_back(k.first);
        }
    }

    virtual ~BaseKey();

    std::set<std::string> keys() const;

    void set(const std::string &k, const std::string &v);
    void unset(const std::string &k);

    void push(const std::string &k, const std::string &v);
    void pop(const std::string &k);

    const std::string& get( const std::string &k ) const;

    void clear();

    // std::vector<eckit::URI> TocEngine::databases(const Key& key,
    bool match(const BaseKey& other) const;
    bool match(const metkit::mars::MarsRequest& request) const;

    // bool match(const BaseKey& other, const eckit::StringList& ignore) const;

    // bool match(const std::string& key, const std::set<std::string>& values) const;
    bool match(const std::string& key, const eckit::DenseSet<std::string>& values) const;

    /// test that, if keys are present in the supplied request, they match the
    /// keys present in the key. Essentially implements a reject-filter
    bool partialMatch(const metkit::mars::MarsRequest& request) const;

    bool operator< (const BaseKey& other) const {
        return keys_ < other.keys_;
    }

    bool operator!= (const BaseKey& other) const {
        return keys_ != other.keys_;
    }

    bool operator== (const BaseKey& other) const {
        return keys_ == other.keys_;
    }

    friend std::ostream& operator<<(std::ostream &s, const BaseKey& x) {
        x.print(s);
        return s;
    }

    friend eckit::Stream& operator<<(eckit::Stream &s, const BaseKey& x) {
        x.encode(s);
        return s;
    }

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

    const eckit::StringDict& keyDict() const;

    metkit::mars::MarsRequest request(std::string verb = "retrieve") const;

    operator std::string() const;

    operator eckit::StringDict() const;

protected: // members

    //TODO add unit test for each type
    virtual std::string canonicalise(const std::string& keyword, const std::string& value) const = 0;
    virtual std::string type(const std::string& keyword) const = 0;

    void print( std::ostream &out ) const;
    void decode(eckit::Stream& s);
    void encode(eckit::Stream &s) const;

    std::string toString() const;

    eckit::StringDict keys_;
    eckit::StringList names_;
};


//----------------------------------------------------------------------------------------------------------------------

class Key : public BaseKey {

public: // methods

    explicit Key();
    explicit Key(eckit::Stream &);
    // explicit Key(const std::string &keys);
    explicit Key(const eckit::StringDict &keys);
    Key(std::initializer_list<std::pair<const std::string, std::string>>);

    static Key parseString(const std::string& s);

    friend eckit::Stream& operator>>(eckit::Stream& s, Key& x) {
        x = Key(s);
        return s;
    }

private: // members

    //TODO add unit test for each type
    std::string canonicalise(const std::string& keyword, const std::string& value) const override;
    std::string type(const std::string& keyword) const override;

};

//----------------------------------------------------------------------------------------------------------------------

class TypedKey : public BaseKey {

public: // methods

    // explicit TypedKey(const Key& key);
    explicit TypedKey(const Key& key, const std::shared_ptr<TypesRegistry> reg);
    explicit TypedKey(const std::shared_ptr<TypesRegistry> reg);
    explicit TypedKey(eckit::Stream &, const std::shared_ptr<TypesRegistry> reg);
    explicit TypedKey(const std::string &keys, const Rule* rule);
    explicit TypedKey(const eckit::StringDict &keys, const std::shared_ptr<TypesRegistry> reg);
    TypedKey(std::initializer_list<std::pair<const std::string, std::string>>, const std::shared_ptr<TypesRegistry> reg);

    /// @todo - this functionality should not be supported any more.
    static TypedKey parseString(const std::string&, const std::shared_ptr<TypesRegistry> reg);

    Key canonical() const;

    /// @throws When "other" doesn't contain all the keys of "this"
    void validateKeys(const BaseKey& other, bool checkAlsoValues = false) const;

    friend eckit::Stream& operator>>(eckit::Stream& s, TypedKey& x) {
        x = TypedKey(s, nullptr);
        return s;
    }

    // Registry is needed before we can stringise/canonicalise.
    void registry(const std::shared_ptr<TypesRegistry> reg);
    [[ nodiscard ]]
    const TypesRegistry& registry() const;
    const void* reg() const;

private: // members

    //TODO add unit test for each type
    std::string canonicalise(const std::string& keyword, const std::string& value) const override;
    std::string type(const std::string& keyword) const override;

    std::shared_ptr<TypesRegistry> registry_;

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

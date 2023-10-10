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

#pragma once

#include "fdb5/database/Key.h"
#include "eckit/types/Types.h"


namespace fdb5 {

class TypesRegistry;
class Rule;

//----------------------------------------------------------------------------------------------------------------------

class InspectionKey : public Key {

public: // methods

    InspectionKey();

    explicit InspectionKey(const Key& other);
    // explicit InspectionKey(eckit::Stream &);
    explicit InspectionKey(const std::string &request);
    explicit InspectionKey(const std::string &keys, const Rule* rule);

    explicit InspectionKey(const eckit::StringDict &keys);
    
    ~InspectionKey() override {}

    Key canonical() const;

    void rule(const Rule *rule);
//    const Rule *rule() const;
    const TypesRegistry& registry() const;

    // friend eckit::Stream& operator>>(eckit::Stream& s, InspectionKey& x) {
    //     x = InspectionKey(s);
    //     return s;
    // }
    // friend eckit::Stream& operator<<(eckit::Stream &s, const InspectionKey &x) {
    //     x.encode(s);
    //     return s;
    // }

    friend std::ostream& operator<<(std::ostream &s, const InspectionKey& x) {
        x.print(s);
        return s;
    }

private: // methods

    //TODO add unit test for each type
    std::string canonicalise(const std::string& keyword, const std::string& value) const override;

    operator eckit::StringDict() const override;

//    void validateKeysOf(const Key& other, bool checkAlsoValues = false) const;

    // void print( std::ostream &out ) const;
    // void decode(eckit::Stream& s);
    // void encode(eckit::Stream &s) const;

    // std::string toString() const;

private: // members

    const Rule *rule_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

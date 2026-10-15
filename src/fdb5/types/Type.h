// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   Type.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include <string>

#include "eckit/types/Types.h"

namespace metkit {
namespace mars {
class MarsRequest;
}
}  // namespace metkit

namespace fdb5 {

class CatalogueReader;

//----------------------------------------------------------------------------------------------------------------------

class Type {

public:  // methods

    Type(const std::string& name, const std::string& type, const std::string& alias = "");

    Type(const Type&) = delete;
    Type& operator=(const Type&) = delete;
    Type(Type&&) = delete;
    Type& operator=(Type&&) = delete;

    virtual ~Type() = default;

    const std::string& alias() const;

    virtual std::string tidy(const std::string& value) const;

    virtual std::string toKey(const std::string& value) const;

    virtual void getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                           eckit::StringList& values, const CatalogueReader* cat) const;

    virtual bool match(const std::string& keyword, const std::string& value1, const std::string& value2) const;

    friend std::ostream& operator<<(std::ostream& s, const Type& x);

public:  // class methods

    static const Type& lookup(const std::string& keyword);

    const std::string& type() const;

private:  // methods

    virtual void print(std::ostream& out) const = 0;

protected:  // members

    std::string name_;
    std::string type_;
    std::string alias_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


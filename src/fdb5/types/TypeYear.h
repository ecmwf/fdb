/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TypeYear.h
/// @author Emanuele Danovaro
/// @date   January 2025

#ifndef fdb5_TypeYear_H
#define fdb5_TypeYear_H

#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TypeYear : public Type {

public:  // methods

    TypeYear(const std::string& name, const std::string& type, const std::string& alias = "year");

    ~TypeYear() override;

    std::string toKey(const std::string& value) const override;

    virtual void getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                           eckit::StringList& values, const Notifier& wind, const CatalogueReader* cat) const override;

private:  // methods

    void print(std::ostream& out) const override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

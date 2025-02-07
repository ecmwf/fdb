/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TypeAbbreviation.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_TypeAbbreviation_H
#define fdb5_TypeAbbreviation_H

#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TypeAbbreviation : public Type {

public:  // methods

    TypeAbbreviation(const std::string& name, const std::string& type);

    ~TypeAbbreviation() override;

    std::string toKey(const std::string& value) const override;

    virtual void getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                           eckit::StringList& values, const Notifier& wind, const CatalogueReader* cat) const override;

private:  // methods

    void print(std::ostream& out) const override;
    size_t count_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

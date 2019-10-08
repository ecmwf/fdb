/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Nov 2016

#ifndef fdb5_LibFdb5_H
#define fdb5_LibFdb5_H

#include "eckit/system/Library.h"

#include "fdb5/database/DB.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

class Config;

//----------------------------------------------------------------------------------------------------------------------

class LibFdb5 : public eckit::system::Library {
public:

    LibFdb5();

    static LibFdb5& instance();

    Config defaultConfig();

protected:

    const void* addr() const;

    virtual std::string version() const;

    virtual std::string gitsha1(unsigned int count) const;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

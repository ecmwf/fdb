/*
 * (C) Copyright 1996-2013 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#ifndef fdb5_Error_H
#define fdb5_Error_H

#include "eckit/log/CodeLocation.h"
#include "eckit/exception/Exceptions.h"

namespace fdb5 {

class Error: public eckit::Exception {
public:
    Error(const eckit::CodeLocation& loc, const std::string& s);
};

} // namespace fdb5

#endif

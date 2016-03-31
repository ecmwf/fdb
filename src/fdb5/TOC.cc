/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"

#include "fdb5/TOC.h"

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

TOC::TOC()
{
}

TOC::~TOC()
{
}

std::vector<std::string> TOC::paramsList() const
{
    /// @TODO get the schema
    NOTIMP;
}

eckit::DataHandle*TOC::retrieve(const FdbTask& task, const marskit::MarsRequest& field) const
{
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TOC.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_TOC_H
#define fdb_TOC_H

#include <vector>
#include <string>

namespace eckit { class DataHandle; }
namespace marskit { class MarsRequest; }

namespace fdb {

class FdbTask;

//----------------------------------------------------------------------------------------------------------------------

class TOC {

public: // methods

	TOC();

    /// Destructor
    
    ~TOC();

    std::vector<std::string> paramsList() const;

    eckit::DataHandle* retrieve(const FdbTask& task, const marskit::MarsRequest& field) const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif

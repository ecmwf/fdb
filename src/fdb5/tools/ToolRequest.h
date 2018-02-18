/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   ToolRequest.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_ToolRequest_H
#define fdb5_ToolRequest_H

#include <string>
#include <vector>

#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class ToolRequest {
public: // methods

    ToolRequest(const std::string& r, const std::vector<std::string>& minimumKeySet = std::vector<std::string>());

    const Key& key() const;

private: // methods

    Key key_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @date Jun 2012

#ifndef fdb5_RequestParser_h
#define fdb5_RequestParser_h

#include "eckit/parser/StreamParser.h"
#include "eckit/types/Types.h"

#include "marslib/MarsRequest.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class RequestParser : public eckit::StreamParser {

public: // methods

    RequestParser(std::istream &in);

    MarsRequest parse(bool lower = true);

private: // methods

    std::string parseIdent(bool);


};

//----------------------------------------------------------------------------------------------------------------------

} // namespace eckit

#endif

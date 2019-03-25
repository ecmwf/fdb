/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   LegacyArchiver.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_LegacyArchiver_H
#define fdb5_LegacyArchiver_H

#include "eckit/types/Types.h"
#include "eckit/io/DataBlob.h"

#include "fdb5/database/Archiver.h"
#include "fdb5/database/Key.h"
#include "fdb5/legacy/LegacyTranslator.h"

namespace eckit {
class DataHandle;
}

namespace fdb5 {
namespace legacy {

//----------------------------------------------------------------------------------------------------------------------

class LegacyArchiver :  public Archiver {

public: // methods

    LegacyArchiver(const eckit::Configuration& config=eckit::LocalConfiguration());

    void archive(const eckit::DataBlobPtr blob);

    void legacy(const std::string &keyword, const std::string &value);

private: // members

    LegacyTranslator translator_;

    Key legacy_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5

#endif

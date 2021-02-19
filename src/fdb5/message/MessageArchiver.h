/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   MessageArchiver.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_MessageArchiver_H
#define fdb5_MessageArchiver_H

#include <iosfwd>

#include "eckit/io/Length.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/Archiver.h"
#include "fdb5/config/Config.h"
#include "fdb5/message/MessageDecoder.h"
#include "fdb5/database/Key.h"
#include "fdb5/api/FDB.h"

namespace eckit {
class DataHandle;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MessageArchiver :
    public MessageDecoder {

public: // methods

    MessageArchiver(const fdb5::Key& key = Key(),
                 bool completeTransfers = false,
                 bool verbose = false,
                 const Config& config=Config());

    void filters(const std::string& include, const std::string& exclude);

    eckit::Length archive(eckit::DataHandle &source);

    void flush();

private: // protected

    eckit::Channel& logVerbose() const;

    bool filterOut(const Key& k) const;

private: // members

    FDB fdb_;

    fdb5::Key key_;

    std::vector<metkit::mars::MarsRequest> include_;
    std::vector<metkit::mars::MarsRequest> exclude_;

    bool completeTransfers_;

    bool verbose_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

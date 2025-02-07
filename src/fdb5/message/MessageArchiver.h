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

#pragma once

#include <iosfwd>

#include "eckit/io/Length.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Key.h"
#include "fdb5/message/MessageDecoder.h"

namespace eckit {
class DataHandle;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MessageArchiver : public MessageDecoder {

public:  // methods

    MessageArchiver(const fdb5::Key& key = Key(), bool completeTransfers = false, bool verbose = false,
                    const Config& config = Config().expandConfig());

    void filters(const std::string& include, const std::string& exclude);
    void modifiers(const std::string& modify);

    eckit::Length archive(eckit::DataHandle& source);

    void flush();

private:  // protected

    bool filterOut(const Key& k) const;

    eckit::message::Message transform(eckit::message::Message&);

private:  // members

    FDB fdb_;

    fdb5::Key key_;

    std::vector<metkit::mars::MarsRequest> include_;
    std::vector<metkit::mars::MarsRequest> exclude_;

    eckit::StringDict modifiers_;

    bool completeTransfers_;

    bool verbose_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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

    void setFilters(const std::string& include, const std::string& exclude);
    void setModifiers(const fdb5::Key& modify);

    eckit::Length archive(eckit::DataHandle& source);

    void flush();

private:  // protected

    bool filterOut(const Key& k) const;

    void transform(eckit::message::Message&);

private:  // members

    FDB fdb_;

    fdb5::Key key_;

    std::vector<metkit::mars::MarsRequest> include_;
    std::vector<metkit::mars::MarsRequest> exclude_;

    eckit::OrderedStringDict modifiers_;

    bool completeTransfers_;

    bool verbose_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

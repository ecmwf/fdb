/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   GribArchiver.h
/// @date   December 2020

#pragma once

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/message/MessageArchiver.h"

namespace eckit {
class DataHandle;
namespace message {
class Message;
}
}  // namespace eckit

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// TODO: remove as soon as mars-server has been updated to use MessageArchiver

class GribArchiver : public MessageArchiver {

public: // methods

    GribArchiver(const fdb5::Key& key = Key(),
                 bool completeTransfers = false,
                 bool verbose = false,
                 const Config& config) : MessageArchiver(key, completeTransfers, verbose, config) {}
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/database/Indexer.h"

#include <pthread.h>
#include <unistd.h>

#include "eckit/runtime/Main.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Indexer::Indexer() : pid_(::getpid()), thread_(::pthread_self()) {
    hostname_ = eckit::Main::hostname();
}

void Indexer::print(std::ostream& out) const {
    out << "(host=" << hostname_ << ",pid=" << pid_ << ",thread=" << thread_ << ")";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

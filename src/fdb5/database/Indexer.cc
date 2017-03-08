/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Indexer.h"

#include <pthread.h>
#include <unistd.h>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Indexer::Indexer() :
    pid_(::getpid()),
    thread_(::pthread_self())
{
    SYSCALL( ::gethostname( hostname_.data(), hostname_.static_size() ) );
}

void Indexer::print(std::ostream& out) const
{
    out << "(host=" << hostname_
        << ",pid=" << pid_
        << ",thread=" << thread_
        << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

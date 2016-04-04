/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/config/Resource.h"

#include "fdb5/TocActions.h"

#include "fdb5/TocDBReader.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDBReader::TocDBReader(const Key& key) : TocDB(key)
{
}

TocDBReader::~TocDBReader()
{
}

eckit::DataHandle* TocDBReader::retrieve(const MarsTask& task, const Key& key) const
{
    NOTIMP;
}

void TocDBReader::print(std::ostream &out) const
{
    out << "TocDBReader()";
}

DBBuilder<TocDBReader> TocDBReader_builder("toc.reader");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

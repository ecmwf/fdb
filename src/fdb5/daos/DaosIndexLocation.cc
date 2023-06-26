/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <typeinfo>

#include "fdb5/daos/DaosIndexLocation.h"

// using namespace eckit;

namespace fdb5 {

// ::eckit::ClassSpec TocIndexLocation::classSpec_ = {&IndexLocation::classSpec(), "TocIndexLocation",};
// ::eckit::Reanimator<TocIndexLocation> TocIndexLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

DaosIndexLocation::DaosIndexLocation(const fdb5::DaosKeyValueName& name, off_t offset) :
    name_(name),
    offset_(offset) {}

// TocIndexLocation::TocIndexLocation(Stream& s) {
//     s >> path_;
//     s >> offset_;
// }

// off_t TocIndexLocation::offset() const {
//     return offset_;
// }

// /*PathName TocIndexLocation::path() const
// {
//     return path_;
// }*/

// eckit::URI DaosIndexLocation::uri() const {
//     return name.URI();
// }

// IndexLocation* TocIndexLocation::clone() const {
//     return new TocIndexLocation(path_, offset_);
// }

// void TocIndexLocation::encode(Stream& s) const {
//     s << path_;
//     s << offset_;
// }

void DaosIndexLocation::print(std::ostream &out) const {

    out << "(" << name_.URI().asString() << ":" << offset_ << ")";

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

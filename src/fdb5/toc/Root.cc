/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/Root.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Root::Root(const std::string &re, const std::string &path, const std::string& handler, bool active, bool visit):
    re_(re),
    path_(path),
    handler_(handler),
    active_(active),
    visit_(visit) {

}

bool Root::match(const std::string& s) const {
    return re_.match(s);
}

const eckit::PathName &Root::path() const {
    return path_;
}

bool Root::active() const {
    return active_;
}

bool Root::visit() const {
    return visit_;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

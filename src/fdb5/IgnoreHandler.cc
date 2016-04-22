/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/utils/Translator.h"

#include "fdb5/KeywordType.h"
#include "fdb5/IgnoreHandler.h"
#include "fdb5/DB.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IgnoreHandler::IgnoreHandler(const std::string &name, const std::string& type) :
    KeywordHandler(name, type) {
}

IgnoreHandler::~IgnoreHandler() {
}


void IgnoreHandler::toKey(std::ostream& out,
                       const std::string& keyword,
                       const std::string& value) const {
}

void IgnoreHandler::getValues(const MarsRequest &request,
                            const std::string &keyword,
                            eckit::StringList &values,
                            const MarsTask &task,
                            const DB *db) const {
}

void IgnoreHandler::print(std::ostream &out) const {
    out << "IgnoreHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<IgnoreHandler> handler("Ignore");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/tools/FDBLock.h"

#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit::option;
using namespace eckit;


namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

FDBLock::FDBLock(int argc, char** argv, bool unlock) :
    FDBVisitTool(argc, argv, "class,expver,stream,date,time"),
    unlock_(unlock),
    list_(false),
    retrieve_(false),
    archive_(false),
    wipe_(false) {

    std::string prefix = std::string(unlock ? "Unl" : "L") + "ock matching databases for ";

    options_.push_back(new SimpleOption<bool>("list", prefix + "listing"));
    options_.push_back(new SimpleOption<bool>("retrieve", prefix + "retrieval"));
    options_.push_back(new SimpleOption<bool>("archive", prefix + "archival"));
    options_.push_back(new SimpleOption<bool>("wipe", prefix + "wipe"));
}


FDBLock::~FDBLock() {}


void FDBLock::init(const CmdArgs& args) {
    FDBVisitTool::init(args);

    args.get("list", list_);
    args.get("retrieve", retrieve_);
    args.get("archive", archive_);
    args.get("wipe", wipe_);

    if (!(list_ || retrieve_ || archive_ || wipe_)) {
        std::stringstream ss;
        ss << "No identifier specified to (un)lock.";
        throw UserError(ss.str(), Here());
    }
}


void FDBLock::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    ControlAction action = unlock_ ? ControlAction::Enable : ControlAction::Disable;

    ControlIdentifiers identifiers;
    if (list_) {
        identifiers |= ControlIdentifier::List;
    }
    if (retrieve_) {
        identifiers |= ControlIdentifier::Retrieve;
    }
    if (archive_) {
        identifiers |= ControlIdentifier::Archive;
    }
    if (wipe_) {
        identifiers |= ControlIdentifier::Wipe;
    }

    for (const FDBToolRequest& request : requests("read")) {

        auto statusIterator = fdb.control(request, action, identifiers);

        size_t count = 0;
        StatusElement elem;
        while (statusIterator.next(elem)) {
            Log::info() << "Database: " << elem.key << std::endl
                        << "  location: " << elem.location.asString() << std::endl;

            if (!elem.controlIdentifiers.enabled(ControlIdentifier::Retrieve)) {
                Log::info() << "  retrieve: LOCKED" << std::endl;
            }
            if (!elem.controlIdentifiers.enabled(ControlIdentifier::Archive)) {
                Log::info() << "  archive: LOCKED" << std::endl;
            }
            if (!elem.controlIdentifiers.enabled(ControlIdentifier::List)) {
                Log::info() << "  list: LOCKED" << std::endl;
            }
            if (!elem.controlIdentifiers.enabled(ControlIdentifier::Wipe)) {
                Log::info() << "  wipe: LOCKED" << std::endl;
            }
            if (!elem.controlIdentifiers.enabled(ControlIdentifier::UniqueRoot)) {
                Log::info() << "  multi-root: PERMITTED" << std::endl;
            }

            count++;
        }

        if (count == 0 && fail()) {
            std::stringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace tools
}  // namespace fdb5

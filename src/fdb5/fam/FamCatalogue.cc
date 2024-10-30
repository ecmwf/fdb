/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/fam/FamCatalogue.h"

#include "eckit/filesystem/URI.h"
// #include "eckit/log/Timer.h"
#include "fdb5/LibFdb5.h"
// #include "fdb5/fam/FamMoveVisitor.h"
// #include "fdb5/fam/FamPurgeVisitor.h"
// #include "fdb5/fam/FamStats.h"
// #include "fdb5/fam/FamWipeVisitor.h"
// #include "fdb5/fam/RootManager.h"
#include "fdb5/rules/Rule.h"

// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FamCatalogue::FamCatalogue(const eckit::FamRegionName& region, const ControlIdentifiers& identifiers, const Config& config):
    FamCommon(region),
    Catalogue(Key(), identifiers, config),
    version_ {FamRecordVersion(config)},
    table_ {root_.object("table")} { }

FamCatalogue::FamCatalogue(const eckit::URI& uri, const Config& config):
    FamCatalogue(uri, ControlIdentifier {}, config) { }

// FamCatalogue::FamCatalogue(const Key& key, const fdb5::Config& config):
//     FamCommon(config, key), Catalogue(key, ControlIdentifier {}, config) { }

// FamCatalogue::FamCatalogue(const eckit::PathName&    directory,
//                            const ControlIdentifiers& controlIdentifiers,
//                            const fdb5::Config&       config):
//     Catalogue(Key(), controlIdentifiers, config), FamHandler(directory, config) {
//     // Read the real DB key into the DB base object
//     dbKey_ = databaseKey();
// }

//----------------------------------------------------------------------------------------------------------------------

void FamCatalogue::remove(const eckit::FamObjectName& name, std::ostream& logAlways, std::ostream& logVerbose, bool doit) {
    logVerbose << "remove: ";
    logAlways << name.uri() << '\n';
    // if (doit) { name.lookup().deallocate(); }
    NOTIMP;
}

auto FamCatalogue::uri() const -> eckit::URI {
    return FamCommon::uri();
}

auto FamCatalogue::exists() const -> bool {
    return FamCommon::exists();
}

void FamCatalogue::loadSchema() {
    NOTIMP;
    // const auto schema = root_.object("schema").lookup().buffer();
    // std::stringstream stream;
    // stream << schema.view();
    // schema_.load(stream, true);
}

auto FamCatalogue::schema() const -> const Schema& {
    return schema_;
}

auto FamCatalogue::indexes(const bool /* sorted */) const -> std::vector<Index> {
    std::vector<Index> result;

    /// @todo here would be the eckit::FamMap as KV store on FAM, which is not implemented yet
    NOTIMP;

    return result;
}

void FamCatalogue::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {
    NOTIMP;
    // const bool walkSubToc = conf.getBool(DUMP_PARAM_WALK, false);
    // FamHandler::dump(out, simple, walkSubToc);
}

void FamCatalogue::maskIndexEntry(const Index& index) const {
    NOTIMP;
    // FamHandler(root_, config_).writeClearRecord(index);
}

auto FamCatalogue::metadataPaths() const -> std::vector<eckit::PathName> {
    NOTIMP;
}

StatsReportVisitor* FamCatalogue::statsReportVisitor() const {
    NOTIMP;
}

PurgeVisitor* FamCatalogue::purgeVisitor(const Store& /* store */) const {
    NOTIMP;
}

WipeVisitor* FamCatalogue::wipeVisitor(const Store&                     store,
                                       const metkit::mars::MarsRequest& request,
                                       std::ostream&                    out,
                                       bool                             doit,
                                       bool                             porcelain,
                                       bool                             unsafeWipeAll) const {
    NOTIMP;
}

MoveVisitor* FamCatalogue::moveVisitor(const Store&                     store,
                                       const metkit::mars::MarsRequest& request,
                                       const eckit::URI&                dest,
                                       eckit::Queue<MoveElement>&       queue) const {
    NOTIMP;
}

void FamCatalogue::allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& /* metadata */,
                             std::set<eckit::URI>& /* data */) const {
    NOTIMP;
}

void FamCatalogue::control(const ControlAction& /* action */, const ControlIdentifiers& /* identifiers */) const {
    NOTIMP;
    // FamHandler::control(action, identifiers);
}

bool FamCatalogue::enabled(const ControlIdentifier& /* identifier */) const {
    NOTIMP;
    // return Catalogue::enabled(identifier) && FamHandler::enabled(identifier);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

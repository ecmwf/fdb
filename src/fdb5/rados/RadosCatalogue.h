/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date Jun 2024

#pragma once

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/MoveVisitor.h"
#include "fdb5/database/PurgeVisitor.h"
#include "fdb5/database/StatsReportVisitor.h"
#include "fdb5/rados/RadosCommon.h"
#include "fdb5/rules/Schema.h"

#include "eckit/config/Configuration.h"
#include "eckit/container/Queue.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Offset.h"

#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace fdb5 {

class Rule;
class RuleDatabase;
class CatalogueWipeState;

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on Rados

class RadosCatalogue : public CatalogueImpl, public RadosCommon {

public:  // methods

    RadosCatalogue(const Key& key, const fdb5::Config& config);
    RadosCatalogue(const eckit::URI& uri, const ControlIdentifiers& controlIdentifiers, const fdb5::Config& config);

    static const char* catalogueTypeName() { return "rados"; }

    eckit::URI uri() const override;
    const Key& indexKey() const override { return currentIndexKey_; }

    std::string type() const override;

    void checkUID() const override { /* nothing to do */ }
    bool exists() const override;
    void dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const override {
        out << "RadosCatalogue(" << type() << ":" << dbKey_ << ")";
    }
    const Schema& schema() const override;

    StatsReportVisitor* statsReportVisitor() const override { NOTIMP; };
    PurgeVisitor* purgeVisitor(const Store& store) const override { NOTIMP; };
    MoveVisitor* moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest,
                             eckit::Queue<MoveElement>& queue) const override {
        NOTIMP;
    };

    void loadSchema() override;

    std::vector<Index> indexes(bool sorted = false) const override;

    // No masking metadata is persisted for this backend; wipe removes entries directly, so there is
    // nothing to enumerate here.
    void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata,
                   std::set<eckit::URI>& data) const override {}

    // Control access properties of the DB
    // @todo: control identifiers are not persisted for RADOS yet; wipe/coordinator invocations rely
    //        on default-enabled semantics.
    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override {}

    const Rule& rule() const override;

    bool uriBelongs(const eckit::URI& uri) const override;

    void maskIndexEntries(const std::set<Index>& indexes) const override;

    // Wipe-related methods
    CatalogueWipeState wipeInit() const override;
    bool markIndexForWipe(const Index& index, bool include, CatalogueWipeState& wipeState) const override;
    void finaliseWipeState(CatalogueWipeState& wipeState) const override;
    bool doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const override;
    bool doWipeURIs(const CatalogueWipeState& wipeState) const override;
    void doWipeEmptyDatabase() const override;
    bool doUnsafeFullWipe() const override;

protected:  // members

    Key currentIndexKey_;

private:  // members

    Schema schema_;
    const RuleDatabase* rule_{nullptr};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

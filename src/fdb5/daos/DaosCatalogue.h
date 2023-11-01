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
/// @date Feb 2022

#pragma once

#include "fdb5/database/DB.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/daos/DaosCommon.h"
#include "fdb5/daos/DaosEngine.h"
#include "fdb5/daos/DaosOID.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on DAOS

class DaosCatalogue : public Catalogue, public DaosCommon {

public: // methods

    DaosCatalogue(const Key& key, const fdb5::Config& config);
    DaosCatalogue(const eckit::URI& uri, const ControlIdentifiers& controlIdentifiers, const fdb5::Config& config);

    static const char* catalogueTypeName() { return fdb5::DaosEngine::typeName(); }
    
    eckit::URI uri() const override;
    const Key& indexKey() const override { return currentIndexKey_; }

    static void remove(const fdb5::DaosNameBase&, std::ostream& logAlways, std::ostream& logVerbose, bool doit);

    std::string type() const override;

    void checkUID() const override { NOTIMP; };
    bool exists() const override;
    void visitEntries(EntryVisitor& visitor, const Store& store, bool sorted) override;
    void dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const override { NOTIMP; };
    std::vector<eckit::PathName> metadataPaths() const override { NOTIMP; };
    const Schema& schema() const override;

    StatsReportVisitor* statsReportVisitor() const override { NOTIMP; };
    PurgeVisitor* purgeVisitor(const Store& store) const override { NOTIMP; };
    WipeVisitor* wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const override;
    MoveVisitor* moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const override { NOTIMP; };
    void maskIndexEntry(const Index& index) const override { NOTIMP; };

    void loadSchema() override;

    std::vector<Index> indexes(bool sorted=false) const override;

    void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata,
                   std::set<eckit::URI>& data) const override { NOTIMP; };

    // Control access properties of the DB
    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override { NOTIMP; };

protected: // members

    Key currentIndexKey_;

private: // members

    Schema schema_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamCatalogue.h
/// @author Metin Cakircali
/// @date   Jul 2024

#pragma once

#include "eckit/io/fam/FamObjectName.h"
#include "eckit/io/fam/FamPath.h"
#include "fdb5/database/Index.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamEngine.h"
#include "fdb5/fam/FamHandler.h"
#include "fdb5/rules/Schema.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// @brief FDB Catalogue for Fabric Attached Memory systems
// class FamCatalogue: protected FamCommon, public Catalogue, public FamHandler {
class FamCatalogue: protected FamCommon, public Catalogue {
    friend class FamWipeVisitor;
    friend class FamMoveVisitor;

public:  // types
    using FamCommon::typeName;

    static constexpr auto DUMP_PARAM_WALK = "walk";

public:  // methods
    FamCatalogue(const eckit::FamRegionName& region, const ControlIdentifiers& identifiers, const Config& config);

    FamCatalogue(const eckit::URI& uri, const Config& config);

    // FamCatalogue(const Key& key, const Config& config);

    ~FamCatalogue() = default;

    // static auto catalogueTypeName() -> const char* { return typeName; }

    static void remove(const eckit::FamObjectName& name, std::ostream& logAlways, std::ostream& logVerbose, bool doit);

    // const eckit::PathName& basePath() const override;
    auto uri() const -> eckit::URI override;

    auto indexKey() const -> const Key& override { return indexKey_; }

    auto enabled(const ControlIdentifier& identifier) const -> bool override;

protected:  // methods
    auto type() const -> std::string override { return typeName; }

    bool exists() const override;

    void loadSchema() override;

    auto schema() const -> const Schema& override;

    auto indexes(bool sorted = false) const -> std::vector<Index> override;

    void dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const override;

    auto metadataPaths() const -> std::vector<eckit::PathName> override;

    auto statsReportVisitor() const -> StatsReportVisitor* override;

    auto purgeVisitor(const Store& store) const -> PurgeVisitor* override;

    auto wipeVisitor(const Store&                     store,
                     const metkit::mars::MarsRequest& request,
                     std::ostream&                    out,
                     bool                             doit,
                     bool                             porcelain,
                     bool                             unsafe) const -> WipeVisitor* override;

    auto moveVisitor(const Store&                     store,
                     const metkit::mars::MarsRequest& request,
                     const eckit::URI&                dest,
                     eckit::Queue<MoveElement>&       queue) const -> MoveVisitor* override;

    void maskIndexEntry(const Index& index) const override;

    void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata, std::set<eckit::URI>& data) const override;

    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override;

private:  // members
    const FamRecordVersion version_;

    eckit::FamObjectName table_;

    Schema schema_;

    Key indexKey_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

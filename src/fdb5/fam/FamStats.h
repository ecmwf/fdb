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

/// @file   FamStats.h
/// @author Metin Cakircali
/// @date   Jul 2026

#pragma once

#include "fdb5/database/DbStats.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/IndexStats.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/StatsReportVisitor.h"

#include "eckit/serialisation/Reanimator.h"

#include <cstddef>
#include <iosfwd>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace fdb5 {

class FamCatalogue;

//----------------------------------------------------------------------------------------------------------------------

class FamDbStats : public DbStatsContent {
public:

    FamDbStats() = default;
    explicit FamDbStats(eckit::Stream& stream);

    static DbStats make() { return {new FamDbStats()}; }

    size_t dbCount_{0};
    size_t indexCount_{0};
    size_t dataObjectCount_{0};
    unsigned long long dataObjectSize_{0};
    unsigned long long dataReachableSize_{0};

    FamDbStats& operator+=(const FamDbStats& rhs);

    void add(const DbStatsContent& rhs) override;

    void report(std::ostream& out, const char* indent) const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    void encode(eckit::Stream& stream) const override;
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<FamDbStats> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

class FamIndexStats : public IndexStatsContent {
public:

    FamIndexStats() = default;
    explicit FamIndexStats(eckit::Stream& stream);

    size_t fieldsCount_{0};
    size_t duplicatesCount_{0};
    unsigned long long fieldsSize_{0};
    unsigned long long duplicatesSize_{0};

    size_t fieldsCount() const override { return fieldsCount_; }
    size_t duplicatesCount() const override { return duplicatesCount_; }
    size_t fieldsSize() const override { return fieldsSize_; }
    size_t duplicatesSize() const override { return duplicatesSize_; }

    size_t addFieldsCount(size_t i) override { return fieldsCount_ += i; }
    size_t addDuplicatesCount(size_t i) override { return duplicatesCount_ += i; }
    size_t addFieldsSize(size_t i) override { return fieldsSize_ += i; }
    size_t addDuplicatesSize(size_t i) override { return duplicatesSize_ += i; }

    FamIndexStats& operator+=(const FamIndexStats& rhs);

    void add(const IndexStatsContent& rhs) override;

    void report(std::ostream& out, const char* indent) const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    void encode(eckit::Stream& stream) const override;
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<FamIndexStats> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

/// Traverses a FAM catalogue's entries to compute statistics.
class FamStatsReportVisitor : public virtual StatsReportVisitor {
public:

    explicit FamStatsReportVisitor(const FamCatalogue& catalogue);

    // rules
    FamStatsReportVisitor(const FamStatsReportVisitor&) = delete;
    FamStatsReportVisitor& operator=(const FamStatsReportVisitor&) = delete;
    FamStatsReportVisitor(FamStatsReportVisitor&&) = delete;
    FamStatsReportVisitor& operator=(FamStatsReportVisitor&&) = delete;

    ~FamStatsReportVisitor() override;

    IndexStats indexStatistics() const override;
    DbStats dbStatistics() const override;

protected:  // members

    std::map<Index, IndexStats> indexStats_;

    std::unordered_set<std::string> seen_;

    std::unordered_map<std::string, size_t> dataUsage_;

    std::unordered_set<std::string> indexObjects_;

    /// Total archived data payload (bytes), including duplicates.
    unsigned long long dataSize_{0};

    /// Reachable (non-duplicated) archived data payload (bytes).
    unsigned long long reachableSize_{0};

private:  // methods

    bool visitDatabase(const Catalogue& catalogue) override;

    bool visitIndex(const Index& index) override;

    void visitDatum(const Field& field, const Key& datum_key) override;
    void visitDatum(const Field& field, const std::string& key_fingerprint) override;

    void catalogueComplete(const Catalogue& catalogue) override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

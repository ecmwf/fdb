// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamPurgeVisitor.h
/// @author Metin Cakircali
/// @date   Jul 2026

#pragma once

#include "fdb5/database/PurgeVisitor.h"
#include "fdb5/fam/FamStats.h"

#include <iosfwd>
#include <set>
#include <string>

namespace fdb5 {

class FamCatalogue;
class Store;

//----------------------------------------------------------------------------------------------------------------------

/// Purge removes those unreferenced data objects (and any data referenced only by masked indexes).
class FamPurgeVisitor : public PurgeVisitor, public FamStatsReportVisitor {
public:

    FamPurgeVisitor(const FamCatalogue& catalogue, const Store& store);

    bool visitDatabase(const Catalogue& catalogue) override;

    void report(std::ostream& out) const override;

    void purge(std::ostream& out, bool porcelain, bool doit) const override;

private:  // members

    const Store& store_;

    std::set<std::string> maskedData_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   PurgeVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_PurgeVisitor_H
#define fdb5_PurgeVisitor_H

#include <iosfwd>
#include <set>
#include <map>
#include <vector>

#include "eckit/filesystem/PathName.h"

#include "fdb5/Index.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct Stats {

    Stats() : totalFields(0), duplicates(0), totalSize(0), duplicatesSize(0) {}

    size_t totalFields;
    size_t duplicates;
    eckit::Length totalSize;
    eckit::Length duplicatesSize;

    Stats& operator+=(const Stats& rhs) {
        totalFields += rhs.totalFields;
        duplicates += rhs.duplicates;
        totalSize += rhs.totalSize;
        duplicatesSize += rhs.duplicatesSize;
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& s,const Stats& x) { x.print(s); return s; }

    void print(std::ostream& out) const;

};
//----------------------------------------------------------------------------------------------------------------------

struct PurgeVisitor : public EntryVisitor {

    PurgeVisitor(const eckit::PathName& dir);

    Stats totals() const;

    void currentIndex(const eckit::PathName& path);

    void report(std::ostream& out) const;

    void purge() const;

private: // methods

    std::vector<eckit::PathName> filesToBeDeleted(size_t& adopted, size_t& duplicated, size_t& duplicatedAdopted) const;

    friend std::ostream& operator<<(std::ostream& s,const PurgeVisitor& x) { x.print(s); return s; }

    void print(std::ostream& out) const;

    virtual void visit(const std::string& index,
                       const std::string& key,
                       const eckit::PathName& path,
                       eckit::Offset offset,
                       eckit::Length length);

private: // members

    eckit::PathName dir_;
    eckit::PathName current_; //< current Index being scanned

    std::set<eckit::PathName> activeDataFiles_;
    std::set<eckit::PathName> allDataFiles_;

    std::set<std::string> active_;

    std::map<eckit::PathName, Stats> indexStats_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

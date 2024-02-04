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
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_pmem_PMemIndex_H
#define fdb5_pmem_PMemIndex_H

#include "eckit/eckit.h"

#include "fdb5/database/Index.h"

#include "fdb5/pmem/PMemIndexLocation.h"

namespace fdb5 {
namespace pmem {

class PBranchingNode;

//----------------------------------------------------------------------------------------------------------------------


class PMemIndex : public IndexBase {

public: // methods

    PMemIndex(const Key &key, PBranchingNode& node, DataPoolManager& mgr, const std::string& type=defaulType());

    virtual ~PMemIndex() override;

    virtual void visit(IndexLocationVisitor& visitor) const;

    static std::string defaulType();

protected: // methods

    virtual const IndexLocation& location() const { return location_; }
    virtual const std::vector<eckit::URI> dataURIs() const override;

    virtual bool dirty() const;

    virtual void open();
    virtual void close();
    virtual void reopen();

    virtual bool get( const Key &key, Field &field ) const;
    virtual void add( const Key &key, const Field &field );
    virtual void flush();
    virtual void encode(eckit::Stream &s) const override;
    virtual void entries(EntryVisitor &visitor) const;

    virtual void print( std::ostream &out ) const override;
    virtual void dump(std::ostream& out, const char* indent, bool simple = false, bool dumpFields = false) const;

    virtual IndexStats statistics() const;

private: // members

    PMemIndexLocation location_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif

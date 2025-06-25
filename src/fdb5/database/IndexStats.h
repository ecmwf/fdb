/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   IndexStats.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_IndexStats_H
#define fdb5_IndexStats_H

#include <iosfwd>

#include "eckit/log/Statistics.h"
#include "eckit/memory/Counted.h"
#include "eckit/serialisation/Streamable.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class IndexStatsContent : public eckit::Counted, public eckit::Statistics, public eckit::Streamable {
public:

    ~IndexStatsContent() override;

    virtual size_t fieldsCount() const     = 0;
    virtual size_t duplicatesCount() const = 0;

    virtual size_t fieldsSize() const     = 0;
    virtual size_t duplicatesSize() const = 0;

    virtual size_t addFieldsCount(size_t)     = 0;
    virtual size_t addDuplicatesCount(size_t) = 0;

    virtual size_t addFieldsSize(size_t i)     = 0;
    virtual size_t addDuplicatesSize(size_t i) = 0;

    virtual void add(const IndexStatsContent&) = 0;

    virtual void report(std::ostream& out, const char* indent) const = 0;

public:  // For Streamable

    void encode(eckit::Stream& s) const override = 0;

protected:  // For Streamable

    static eckit::ClassSpec classSpec_;
};

//----------------------------------------------------------------------------------------------------------------------

/// @invariant content is always valid by contruction

class IndexStats {

public:  // methods

    IndexStats();
    IndexStats(IndexStatsContent*);

    ~IndexStats();

    IndexStats(const IndexStats&);

    IndexStats& operator=(const IndexStats&);

    IndexStats& operator+=(const IndexStats& rhs);

    size_t fieldsCount() const { return content_->fieldsCount(); }
    size_t duplicatesCount() const { return content_->duplicatesCount(); }

    virtual size_t addFieldsCount(size_t i) { return content_->addFieldsCount(i); }
    virtual size_t addDuplicatesCount(size_t i) { return content_->addDuplicatesCount(i); }

    size_t fieldsSize() const { return content_->fieldsSize(); }
    size_t duplicatesSize() const { return content_->duplicatesSize(); }

    virtual size_t addFieldsSize(size_t i) { return content_->addFieldsSize(i); }
    virtual size_t addDuplicatesSize(size_t i) { return content_->addDuplicatesSize(i); }

    void add(const IndexStats&);

    void report(std::ostream& out, const char* indent = "") const;

    //    template <class T>
    //    T& as() {
    //        return dynamic_cast<T&>(*content_);
    //    }

private:  // methods

    void print(std::ostream&) const;
    void encode(eckit::Stream& s) const;

    friend std::ostream& operator<<(std::ostream& s, const IndexStats& o) {
        o.print(s);
        return s;
    }

    friend eckit::Stream& operator<<(eckit::Stream& s, const IndexStats& r) {
        r.encode(s);
        return s;
    }

private:  // members

    IndexStatsContent* content_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

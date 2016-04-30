/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_Index_H
#define fdb5_Index_H

#include "eckit/eckit.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"

#include "fdb5/FileStore.h"
#include "fdb5/IndexAxis.h"
#include "fdb5/Key.h"

namespace eckit {
class Stream;
}

namespace fdb5 {

class Key;
class Index;

//----------------------------------------------------------------------------------------------------------------------

class EntryVisitor : private eckit::NonCopyable {
public:
    virtual void visit(const Index& index,
                       const std::string &indexFingerprint,
                       const std::string &fieldFingerprint,
                       const eckit::PathName &path,
                       eckit::Offset offset,
                       eckit::Length length) = 0;
};

//----------------------------------------------------------------------------------------------------------------------

class Index : private eckit::NonCopyable {

public: // types

    enum Mode { WRITE, READ };

    /// A field location on the FDB
    struct Field {

        Field() {}

        Field(const eckit::PathName &path, eckit::Offset offset, eckit::Length length ) :
            path_(path),
            offset_(offset),
            length_(length)
#if 0
            ,
            referenceValue_(0),
            binaryScaleFactor_(0),
            decimalScaleFactor_(0),
            bitsPerValue_(0),
            offsetBeforeData_(0),
            offsetBeforeBitmap_(0),
            numberOfValues_(0),
            numberOfDataPoints_(0),
            sphericalHarmonics_(0)
#endif
        {
        }

        eckit::PathName path_;
        eckit::Offset   offset_;
        eckit::Length   length_;

        ///////////////////////////////////////////////
#if 0
        double        referenceValue_;
        long          binaryScaleFactor_;
        long          decimalScaleFactor_;
        unsigned long bitsPerValue_;
        unsigned long offsetBeforeData_;
        unsigned long offsetBeforeBitmap_;
        unsigned long numberOfValues_;
        unsigned long numberOfDataPoints_;
        long          sphericalHarmonics_;

        eckit::FixedString<32>   gridMD5_; ///< md5 of the grid geometry section in GRIB
#endif
        ///////////////////////////////////////////////


        friend std::ostream &operator<<(std::ostream &s, const Field &x) { x.print(s); return s; }

        void print( std::ostream &out ) const;

    };

public: // methods


    Index(const Key &key, const eckit::PathName &path, off_t offset, Index::Mode mode, const std::string& type );
    Index(eckit::Stream &, const eckit::PathName &directory, const eckit::PathName &path, off_t offset);
    virtual ~Index();

    virtual void open() = 0;
    virtual void reopen() = 0;
    virtual void close() = 0;
    virtual void flush() = 0;

    const eckit::PathName& path() const ;
    off_t offset() const ;
    const std::string& type() const ;

    const IndexAxis &axes() const ;
    const Key &key() const;

    virtual bool get(const Key &key, Field &field) const = 0;
    virtual void put(const Key &key, const Field &field);
    virtual void encode(eckit::Stream &s) const;
    virtual void entries(EntryVisitor &visitor) const = 0;

private: // methods

    virtual void add(const Key &key, const Field &field) = 0;
    virtual void print( std::ostream &out ) const = 0;

protected: // members

    const Mode            mode_;
    const eckit::PathName path_;
    off_t           offset_;
    std::string type_;

    // Order is important here...
    FileStore   files_;
    IndexAxis   axes_;
    const Key key_; ///< key that selected this index
    std::string prefix_;


    friend std::ostream &operator<<(std::ostream &s, const Index &x) {
        x.print(s);
        return s;
    }

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

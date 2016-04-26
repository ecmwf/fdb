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

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class Index : private eckit::NonCopyable {

public: // types

    enum Mode { WRITE, READ };

    /// A field location on the FDB
    struct Field {

        Field() {}

        Field(const eckit::PathName& path, eckit::Offset offset, eckit::Length length ) :
            path_(path),
            offset_(offset),
            length_(length) {
        }

        eckit::PathName path_;
        eckit::Offset   offset_;
        eckit::Length   length_;

        /// @todo
        /// Should return here the information about bit encoding for direct value access
        /// this should also persits in some other class

        void load( std::istream& s );

        void dump( std::ostream& s ) const;

        friend std::ostream& operator<<(std::ostream& s,const Field& x) { x.print(s); return s; }

		void print( std::ostream& out ) const { out << path_ << "," << offset_ << "+" << length_ ; }

    };

public: // methods

    static Index* create(const Key& key, const std::string& type, const eckit::PathName& path, Index::Mode mode);

    Index(const Key& key, const eckit::PathName& path, Index::Mode mode );

    virtual ~Index();

    virtual bool    exists( const Key& key ) const = 0;

    virtual bool    get( const Key& key, Field& field ) const = 0;

    virtual Field   get( const Key& key ) const = 0;

    virtual void    put( const Key& key, const Field& field );

    virtual bool    remove( const Key& key ) = 0;

    virtual void flush();

    virtual void print( std::ostream& out ) const = 0;

    friend std::ostream& operator<<(std::ostream& s, const Index& x) { x.print(s); return s; }

	eckit::PathName path() const { return path_; }

	Mode mode() const { return mode_; }

    const IndexAxis& axes() const { return axes_; }

    const Key& key() const;

protected: // methods

    virtual void put_( const Key& key, const Field& field ) = 0;
    eckit::PathName jsonFile() const;

protected: // members

	Mode            mode_;

	eckit::PathName path_;

    FileStore   files_;
    IndexAxis   axes_;

    Key key_; ///< key that selected this index

};

//----------------------------------------------------------------------------------------------------------------------


class IndexFactory {

    std::string name_;

    virtual Index* make(const Key& key, const eckit::PathName& path, Index::Mode mode) const = 0 ;

protected:

    IndexFactory(const std::string&);
    virtual ~IndexFactory();

public:

    static void list(std::ostream &);
    static Index* build(const std::string&,
        const Key& key, const eckit::PathName& path, Index::Mode mode);

private: // methods

    static const IndexFactory& findFactory(const std::string&);
};

template< class T>
class IndexBuilder : public IndexFactory {

    virtual Index* make(const Key& key, const eckit::PathName& path, Index::Mode mode) const {
        return new T(key, path, mode);
    }

public:
    IndexBuilder(const std::string &name) : IndexFactory(name) {}
};

} // namespace fdb5

#endif

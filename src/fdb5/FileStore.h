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

#ifndef fdb5_FileStore_H
#define fdb5_FileStore_H

#include "eckit/eckit.h"

#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/io/Offset.h"
#include "eckit/filesystem/PathName.h"

namespace eckit {
class Stream;
}

namespace fdb5 {

class FileStore : private eckit::NonCopyable {

public: // types

    typedef size_t PathID;

    struct FieldRef {

        PathID          pathId_;
        eckit::Offset   offset_;
        eckit::Length   length_;

        /// @todo
        /// this is where the information of field encoding should be placed

        void load( std::istream &s );

        void dump( std::ostream &s ) const;

        friend std::ostream &operator<<(std::ostream &s, const FieldRef &x) {
            x.print(s);
            return s;
        }

        void print( std::ostream &out ) const {
            out << " FileId " << pathId_
                << " eckit::Offset " << offset_
                << " eckit::Length " << length_ ;
        }
    };

    typedef std::map< FileStore::PathID, eckit::PathName >     PathStore;
    typedef std::map< eckit::PathName, FileStore::PathID >     IdStore;

public: // methods

    FileStore(const eckit::PathName &directory);
    FileStore(const eckit::PathName &directory, eckit::Stream & );

    ~FileStore();

    /// Inserts the path in the store
    /// @returns PathID associated to the provided eckit::PathName
    FileStore::PathID insert( const eckit::PathName &path );

    /// @returns a PathID associated to the provided eckit::PathName
    FileStore::PathID get( const eckit::PathName &path) const;

    /// @returns a eckit::PathName associated to the provided id
    /// @pre assumes that the eckit::filesystem/PathName.has already been inserted
    eckit::PathName get( const FileStore::PathID id ) const;

    /// check that a PathID is valid and has a eckit::PathName associated to it
    bool exists( const FileStore::PathID id ) const;

    /// check the eckit::PathName exists in the FileStore
    bool exists( const eckit::PathName &path ) const;

    void encode(eckit::Stream &s) const;

    friend std::ostream &operator<<(std::ostream &s, const FileStore &x) {
        x.print(s);
        return s;
    }

private: // members

    void print( std::ostream &out ) const;

private: // members

    FileStore::PathID   next_;
    bool                readOnly_;

    eckit::PathName     directory_;
    PathStore           paths_;
    IdStore             ids_;


};

} // namespace fdb5

#endif

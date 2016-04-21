/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   AdoptVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_FDBFileHandle_h
#define fdb5_FDBFileHandle_h

#include "eckit/io/DataHandle.h"
#include "eckit/io/Buffer.h"
#include "eckit/memory/ScopedPtr.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// This class was adapted from eckit::FileHandle
/// The differences are:
///   * it does not fsync() on flush, only on close()
///   * it fails on ENOSPC

class FDBFileHandle : public eckit::DataHandle {
public:

// -- Contructors

    FDBFileHandle(const std::string&,bool = false);
    FDBFileHandle(eckit::Stream&);

// -- Destructor

    ~FDBFileHandle();

// --  Methods

    void advance(const eckit::Length&);
	const std::string& path() const { return name_; }

// -- Overridden methods

    // From eckit::DataHandle

    virtual eckit::Length openForRead();
    virtual void   openForWrite(const eckit::Length&);
    virtual void   openForAppend(const eckit::Length&);

	virtual long   read(void*,long);
	virtual long   write(const void*,long);
    virtual void   close();
    virtual void   flush();
	virtual void   rewind();
	virtual void   print(std::ostream&) const;
    virtual eckit::Length estimate();
    virtual eckit::Length saveInto(eckit::DataHandle&,eckit::TransferWatcher& = eckit::TransferWatcher::dummy());
    virtual eckit::Offset position();
	virtual bool isEmpty() const;
    virtual void restartReadFrom(const eckit::Offset& from);
    virtual void restartWriteFrom(const eckit::Offset& from);
    virtual void toRemote(eckit::Stream&) const;
    virtual void cost(std::map<std::string,eckit::Length>&, bool) const;
    virtual std::string title() const;
    virtual bool moveable() const { return true; }

    virtual eckit::Offset seek(const eckit::Offset&);
    virtual void skip(const eckit::Length&);

    virtual eckit::DataHandle* clone() const;

	// From Streamable

    virtual void encode(eckit::Stream&) const;
    virtual const eckit::ReanimatorBase& reanimator() const { return reanimator_; }

// -- Class methods

    static  const eckit::ClassSpec&  classSpec()        { return classSpec_;}

private: // members

    std::string         name_;
	FILE*               file_;

    eckit::ScopedPtr<eckit::Buffer>    buffer_;

    bool                read_;
    bool                overwrite_;

private: // methods

	void open(const char*);

// -- Class members

    static  eckit::ClassSpec               classSpec_;
    static  eckit::Reanimator<FDBFileHandle>  reanimator_;

    void sync();
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   FileSpaceHandler.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Jun 2016

#ifndef fdb5_FileSpaceHandler_H
#define fdb5_FileSpaceHandler_H

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"
#include "eckit/utils/Regex.h"

#include "fdb5/toc/Root.h"

namespace fdb5 {

class FileSpace;
class Key;

//----------------------------------------------------------------------------------------------------------------------

class FileSpaceHandlerInstance;

class FileSpaceHandler : private eckit::NonCopyable {

public:  // methods

    static const FileSpaceHandler& lookup(const std::string& name);
    static void regist(const std::string& name, FileSpaceHandlerInstance* h);
    static void unregist(const std::string& name);

    virtual ~FileSpaceHandler();

    virtual eckit::PathName selectFileSystem(const Key& key, const FileSpace& fs) const = 0;

protected:  // methods

    FileSpaceHandler();
};

//----------------------------------------------------------------------------------------------------------------------

class FileSpaceHandlerInstance {
public:

    const FileSpaceHandler& get();

protected:

    FileSpaceHandlerInstance(const std::string& name);

    virtual ~FileSpaceHandlerInstance();

private:

    virtual FileSpaceHandler* make() const = 0;

    std::string name_;
    mutable FileSpaceHandler* instance_;
};

template <class T>
class FileSpaceHandlerRegister : public FileSpaceHandlerInstance {
public:

    FileSpaceHandlerRegister(const std::string& name) : FileSpaceHandlerInstance(name) {}

private:

    FileSpaceHandler* make() const override { return new T(); }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

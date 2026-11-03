// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   FileSpaceHandler.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Jun 2016

#pragma once

#include "eckit/types/Types.h"
#include "eckit/utils/Regex.h"

#include "fdb5/config/Config.h"
#include "fdb5/toc/Root.h"

namespace fdb5 {

class FileSpace;
class Key;

//----------------------------------------------------------------------------------------------------------------------

class FileSpaceHandlerInstance;

class FileSpaceHandler {

public:  // methods

    static const FileSpaceHandler& lookup(const std::string& name, const Config& config);
    static void regist(const std::string& name, FileSpaceHandlerInstance* h);
    static void unregist(const std::string& name);

    FileSpaceHandler() = delete;
    FileSpaceHandler(const FileSpaceHandler&) = delete;
    FileSpaceHandler& operator=(const FileSpaceHandler&) = delete;
    FileSpaceHandler(FileSpaceHandler&&) = delete;
    FileSpaceHandler& operator=(FileSpaceHandler&&) = delete;

    virtual ~FileSpaceHandler();

    virtual eckit::PathName selectFileSystem(const Key& key, const FileSpace& fs) const = 0;

protected:  // methods

    FileSpaceHandler(const Config& config);

    const Config& config_;
};

//----------------------------------------------------------------------------------------------------------------------

class FileSpaceHandlerInstance {
public:

    const FileSpaceHandler& get(const Config& config);

protected:

    FileSpaceHandlerInstance(const std::string& name);

    virtual ~FileSpaceHandlerInstance();

private:

    virtual FileSpaceHandler* make(const Config& config) const = 0;

    std::string name_;
    mutable FileSpaceHandler* instance_;
};

template <class T>
class FileSpaceHandlerRegister : public FileSpaceHandlerInstance {
public:

    FileSpaceHandlerRegister(const std::string& name) : FileSpaceHandlerInstance(name) {}

private:

    FileSpaceHandler* make(const Config& config) const override { return new T(config); }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

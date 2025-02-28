/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   EnvVarFileSpaceHandler.h
/// @author Tiago Quintino
/// @date   Nov 2018

#ifndef fdb5_EnvVarFileSpaceHandler_H
#define fdb5_EnvVarFileSpaceHandler_H

#include <map>
#include <string>

#include "eckit/filesystem/PathName.h"
#include "eckit/thread/Mutex.h"

#include "fdb5/database/Key.h"
#include "fdb5/toc/FileSpaceHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class EnvVarFileSpaceHandler : public FileSpaceHandler {

    typedef std::map<std::string, eckit::PathName> PathTable;

public:  // methods

    EnvVarFileSpaceHandler();

    ~EnvVarFileSpaceHandler() override;

    eckit::PathName selectFileSystem(const Key& key, const FileSpace& fs) const override;

protected:  // methods

    void load() const;

    eckit::PathName append(const std::string& expver, const eckit::PathName& path) const;

    eckit::PathName select(const Key& key, const FileSpace& fs) const;

    std::string fdbFileSpaceSHandlerEnvVarName_;

    mutable eckit::Mutex mutex_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

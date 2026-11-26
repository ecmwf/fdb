// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   EnvVarFileSpaceHandler.h
/// @author Tiago Quintino
/// @date   Nov 2018

#pragma once

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

    EnvVarFileSpaceHandler(const Config& config);

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

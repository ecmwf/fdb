// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   ExpverFileSpaceHandler.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Oct 2016

#pragma once

#include <map>
#include <string>

#include "eckit/filesystem/PathName.h"
#include "eckit/thread/Mutex.h"

#include "fdb5/database/Key.h"
#include "fdb5/toc/FileSpaceHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class ExpverFileSpaceHandler : public FileSpaceHandler {

    typedef std::map<std::string, eckit::PathName> PathTable;

public:  // methods

    ExpverFileSpaceHandler(const Config& config);

    ~ExpverFileSpaceHandler() override;

    eckit::PathName selectFileSystem(const Key& key, const FileSpace& fs) const override;

protected:  // methods

    void load() const;

    eckit::PathName append(const std::string& expver, const eckit::PathName& path) const;

    eckit::PathName select(const Key& key, const FileSpace& fs) const;

    eckit::PathName fdbExpverFileSystems_;

    mutable eckit::Mutex mutex_;

    mutable PathTable table_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

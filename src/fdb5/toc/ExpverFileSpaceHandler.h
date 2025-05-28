/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   ExpverFileSpaceHandler.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Oct 2016

#ifndef fdb5_ExpverFileSpaceHandler_H
#define fdb5_ExpverFileSpaceHandler_H

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

#endif

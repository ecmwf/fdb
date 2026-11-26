// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <sys/file.h>

#include "fdb5/toc/EnvVarFileSpaceHandler.h"

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileLock.h"
#include "eckit/thread/AutoLock.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/FileSpace.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

EnvVarFileSpaceHandler::EnvVarFileSpaceHandler(const Config& config) :
    FileSpaceHandler(config),
    fdbFileSpaceSHandlerEnvVarName_(
        eckit::Resource<std::string>("fdbFileSpaceSHandlerEnvVarName;$FDB_FILESPACEHANDLER_ENVVARNAME", "STHOST")) {}

EnvVarFileSpaceHandler::~EnvVarFileSpaceHandler() {}


PathName EnvVarFileSpaceHandler::select(const Key& key, const FileSpace& fs) const {
    return FileSpaceHandler::lookup("WeightedRandom", config_).selectFileSystem(key, fs);
}

eckit::PathName EnvVarFileSpaceHandler::selectFileSystem(const Key& key, const FileSpace& fs) const {

    AutoLock<Mutex> lock(mutex_);

    LOG_DEBUG_LIB(LibFdb5) << "Selecting a file system based on environment variable "
                           << fdbFileSpaceSHandlerEnvVarName_ << std::endl;

    const char* value = ::getenv(fdbFileSpaceSHandlerEnvVarName_.c_str());
    if (value) {
        eckit::PathName path(value);

        if (path.exists()) {
            return path;
        }

        std::ostringstream msg;
        msg << "";
        throw ReadError(msg.str(), Here());
    }
    else {
        std::ostringstream msg;
        msg << "";
        throw UserError(msg.str(), Here());
    }
}

static FileSpaceHandlerRegister<EnvVarFileSpaceHandler> envvar("envvar");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

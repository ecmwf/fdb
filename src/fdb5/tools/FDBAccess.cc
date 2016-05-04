/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/CmdArgs.h"
#include "eckit/types/Date.h"

#include "fdb5/config/MasterConfig.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/TocDB.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBAccess.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


FDBAccess::FDBAccess(int argc, char **argv):
    FDBTool(argc, argv) {
}



void FDBAccess::usage(const std::string &tool) {
    FDBTool::usage(tool);
}



//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5


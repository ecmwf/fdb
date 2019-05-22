/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Tiago Quintino
/// @date Sep 2012

#include <cstring>
#include <memory>

#include "fdb5/legacy/LegacyArchiver.h"

#include "eckit/testing/Test.h"

using namespace std;
using namespace eckit;
using namespace eckit::testing;
using namespace fdb5;


namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------


CASE ( "test_defaults" ) {

    LocalConfiguration cfg;
    cfg.set("type", "fdb5");

    legacy::LegacyArchiver la(cfg);

    ASSERT(la.fdb().name() == "local");
    ASSERT(!la.fdb().config().has("useSubToc"));
}

CASE ( "test_set_subtoc" ) {

    LocalConfiguration cfg;
    cfg.set("type", "fdb5");
    cfg.set("useSubToc", true);

    legacy::LegacyArchiver la(cfg);

    ASSERT(la.fdb().name() == "local");
    ASSERT(la.fdb().config().has("useSubToc"));
    ASSERT(la.fdb().config().getBool("useSubToc") == true);
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    eckit::Main::initialise(argc, argv, "FDB_HOME");
    return run_tests ( argc, argv, false );
}

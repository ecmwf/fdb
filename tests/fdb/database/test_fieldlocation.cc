/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/FieldLocation.h"

#include "eckit/filesystem/URI.h"
#include "eckit/testing/Test.h"

#include <memory>

using namespace eckit;
using namespace eckit::testing;

namespace fdb::test {

//----------------------------------------------------------------------------------------------------------------------

CASE("FieldLocation - shared_ptr") {
    URI uri {"dummy_uri"};

    // GOOD (not best)
    {
        auto location = std::shared_ptr<fdb5::FieldLocation>(fdb5::FieldLocationFactory::instance().build("file", uri));

        auto loc1 = location->make_shared();
        EXPECT_EQUAL(loc1.use_count(), 2);

        auto loc2 = location->make_shared();
        EXPECT_EQUAL(loc2.use_count(), 3);

        // check that the shared pointers are the same
        EXPECT_EQUAL(loc2, loc1);

        {
            auto loc3 = location->make_shared();
            EXPECT_EQUAL(loc3.use_count(), 4);
            EXPECT_EQUAL(loc3, loc1);
        }

        auto loc4 = location->make_shared();
        EXPECT_EQUAL(loc4.use_count(), 4);
        EXPECT_EQUAL(loc4, loc1);
    }

    // BAD: this is a how NOT to use shared_ptr on FieldLocation
    {
        auto location = std::unique_ptr<fdb5::FieldLocation>(fdb5::FieldLocationFactory::instance().build("file", uri));
        EXPECT_THROWS_AS(location->make_shared(), std::bad_weak_ptr);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

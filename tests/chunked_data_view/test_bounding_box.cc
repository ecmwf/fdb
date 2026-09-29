// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0


#include "chunked_data_view/ViewPart.h"
#include "chunked_data_view/exception/BoundingBoxException.h"

#include "eckit/testing/Test.h"

#include <sstream>

CASE("Bounding Box | Initialization") {

    chunked_data_view::BoundingBox bb{{3, 2, 1}, {5, 3, 2}};


    EXPECT_EQUAL(bb.lower()[0], 3);
    EXPECT_EQUAL(bb.lower()[1], 2);
    EXPECT_EQUAL(bb.lower()[2], 1);

    EXPECT_EQUAL(bb.upper()[0], 5);
    EXPECT_EQUAL(bb.upper()[1], 3);
    EXPECT_EQUAL(bb.upper()[2], 2);

    EXPECT_EQUAL(bb.entries(), 12);

    EXPECT_EQUAL(bb.contains(bb), true);
};

CASE("Bounding Box | Initialization mismatch dimensions | Error") {
    EXPECT_THROWS_AS((chunked_data_view::BoundingBox{{3, 2, 0, 1}, {5, 3, 2}}),
                     chunked_data_view::BoundingBoxException);
    EXPECT_THROWS_AS((chunked_data_view::BoundingBox{{3, 2, 0}, {5, 3, 2, 1}}),
                     chunked_data_view::BoundingBoxException);
};

CASE("Bounding Box | Lower, upper orientation wrong | Error") {
    EXPECT_THROWS_AS((chunked_data_view::BoundingBox{{2}, {1}}), chunked_data_view::BoundingBoxException);
    EXPECT_THROWS_AS((chunked_data_view::BoundingBox{{3, 2, 1}, {3, 2, 0}}), chunked_data_view::BoundingBoxException);
};

CASE("Bounding Box | Contains test") {

    chunked_data_view::BoundingBox bb{{3, 2, 1}, {5, 3, 2}};

    // Contains itself
    EXPECT_EQUAL(bb.contains(bb), true);

    // Contains itself
    EXPECT_EQUAL(bb.contains(chunked_data_view::BoundingBox{{3, 2, 1}, {5, 3, 2}}), true);

    // Contains lower/upper
    EXPECT_EQUAL(bb.contains(chunked_data_view::BoundingBox{{3, 2, 1}, {3, 2, 1}}), true);
    EXPECT_EQUAL(bb.contains(chunked_data_view::BoundingBox{{5, 3, 2}, {5, 3, 2}}), true);

    chunked_data_view::BoundingBox single_point{{3, 2, 1}, {3, 2, 1}};


    EXPECT_EQUAL(bb.contains(chunked_data_view::BoundingBox{{3, 2, 1}, {3, 2, 1}}), true);
};

CASE("Bounding Box | Intersection test") {

    chunked_data_view::BoundingBox bb{{3, 2, 1}, {5, 3, 2}};

    // Idempotent
    EXPECT(bb.intersect(chunked_data_view::BoundingBox{{3, 2, 1}, {5, 3, 2}}) == bb);

    // Contains lower/upper
    chunked_data_view::BoundingBox unit{{0, 0, 0}, {1, 1, 1}};
    EXPECT(bb.intersect(unit).has_value() == false);
    EXPECT(unit.intersect(bb).has_value() == false);


    chunked_data_view::BoundingBox single_point{{3, 2, 1}, {3, 2, 1}};

    EXPECT(bb.intersect(single_point) == single_point);
    EXPECT(single_point.intersect(bb) == single_point);
};

CASE("Bounding Box | Drop last dimension") {

    chunked_data_view::BoundingBox bb{{3, 2, 1}, {5, 3, 2}};
    chunked_data_view::BoundingBox dimension_reducted{{3, 2}, {5, 3}};

    EXPECT(bb.dropLastDimension() == dimension_reducted);
};

CASE("Bounding Box | Subtract/Translate") {

    chunked_data_view::BoundingBox bb{{3, 2, 1}, {5, 3, 2}};
    chunked_data_view::BoundingBox expected_translated{{0, 0, 0}, {2, 1, 1}};

    EXPECT(bb.subtract({3, 2, 1}) == expected_translated);
    EXPECT_EQUAL(bb.entries(), expected_translated.entries());
};

CASE("Bounding Box | Equals pperator") {

    chunked_data_view::BoundingBox bb{{3, 2, 1}, {5, 3, 2}};
    chunked_data_view::BoundingBox equal{{3, 2, 1}, {5, 3, 2}};
    chunked_data_view::BoundingBox not_equal{{3, 2, 1}, {5, 3, 1}};

    EXPECT(bb == equal);
    EXPECT(bb != not_equal);
};

CASE("Bounding Box | operator<< prints per-dimension intervals") {
    chunked_data_view::BoundingBox bb{{1, 2, 3}, {4, 5, 6}};
    std::ostringstream oss;
    oss << bb;
    EXPECT_EQUAL(oss.str(), std::string("[1, 4] x [2, 5] x [3, 6]"));
};

CASE("Bounding Box | operator<< prints [] for zero-dimension box") {
    chunked_data_view::BoundingBox bb{};
    std::ostringstream oss;
    oss << bb;
    EXPECT_EQUAL(oss.str(), std::string("[]"));
};


int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}

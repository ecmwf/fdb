// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "chunked_data_view/ListIterator.h"

#include "eckit/testing/Test.h"

#include "test_mock_helpers.h"

#include <memory>
#include <type_traits>
#include <vector>

// Regression guard for a leak: fdb5::FieldLocation::dataHandle() returns a raw *owning*
// pointer, and ListElement::dataHandle() used to hand it straight through, so every caller
// leaked one DataHandle (and its file descriptor) per field per chunk read.
CASE("ListElement | dataHandle returns an owning unique_ptr") {

    using Returned = decltype(std::declval<const chunked_data_view::ListElement&>().dataHandle());

    static_assert(std::is_same_v<Returned, std::unique_ptr<eckit::DataHandle>>,
                  "ListElement::dataHandle() must return an owning unique_ptr, never a raw "
                  "eckit::DataHandle* — a raw pointer here leaks on every chunk read.");

    const std::vector<double> values = {1, 2, 3, 4};
    const chunked_data_view::ListElement element{fdb5::Key(), std::make_shared<const MockFieldLocation>(values)};

    auto first = element.dataHandle();
    EXPECT(first != nullptr);

    // Each call must open its own handle; the caller owns and releases each one.
    auto second = element.dataHandle();
    EXPECT(second != nullptr);
    EXPECT(first.get() != second.get());
};

CASE("ListElement | the handle from dataHandle is readable and self-owned") {

    const std::vector<double> values = {7, 8, 9};
    const chunked_data_view::ListElement element{fdb5::Key(), std::make_shared<const MockFieldLocation>(values)};

    size_t countValues = 0;
    size_t bytesPerValue = 0;
    {
        auto handle = element.dataHandle();
        handle->openForRead();
        EXPECT_EQUAL(handle->read(&countValues, sizeof(countValues)), sizeof(countValues));
        EXPECT_EQUAL(handle->read(&bytesPerValue, sizeof(bytesPerValue)), sizeof(bytesPerValue));
        handle->close();
    }  // handle destroyed here — under -fsanitize=leak this scope must not report a leak

    EXPECT_EQUAL(countValues, values.size());
    EXPECT_EQUAL(bytesPerValue, 8);
};

CASE("ListElement | MockListIterator yields a usable location") {

    // The mock used to return a null location, which is why the leak above went unnoticed.
    // NOTE: MockListIterator pre-increments before returning, so createMockFDB(n) yields
    // n - 1 elements (see the comment in test_view.cc).
    auto fdb = createMockFDB(/* fieldAmount = */ 2);
    auto iterator = fdb->inspect(metkit::mars::MarsRequest{});

    size_t seen = 0;
    while (const auto element = iterator->next()) {
        EXPECT(element->location != nullptr);
        EXPECT(element->dataHandle() != nullptr);
        ++seen;
    }
    EXPECT_EQUAL(seen, 1);
};

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}

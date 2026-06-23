// fdb exception-handling smoke tests — implementation.

#include "fdb_exceptions.h"

#include "ExceptionTest.h"

#include "eckit/exception/Exceptions.h"

#include <stdexcept>

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

void ExceptionTest::throw_eckit_exception() {
    throw eckit::Exception("test eckit exception");
}

void ExceptionTest::throw_std_exception() {
    throw std::runtime_error("test std exception");
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge

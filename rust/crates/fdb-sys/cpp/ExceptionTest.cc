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

void ExceptionTest::throw_eckit_serious_bug() {
    throw eckit::SeriousBug("test serious bug");
}

void ExceptionTest::throw_eckit_user_error() {
    throw eckit::UserError("test user error");
}

void ExceptionTest::throw_std_exception() {
    throw std::runtime_error("test std exception");
}

void ExceptionTest::throw_int() {
    throw 42;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge

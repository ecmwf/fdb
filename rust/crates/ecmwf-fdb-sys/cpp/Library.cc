// fdb library metadata + runtime initialisation bridge — implementation.

#include "fdb_exceptions.h"

#include "Library.h"

#include "fdb5/fdb5_version.h"

#include "eckit/runtime/Main.h"

#include <mutex>

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

namespace {

std::once_flag g_init_flag;

}  // namespace

void Library::initialise() {
    std::call_once(g_init_flag, []() {
        if (!eckit::Main::ready()) {
            static const char* argv[] = {"fdb-sys", nullptr};
            eckit::Main::initialise(1, const_cast<char**>(argv));
        }
    });
}

rust::String Library::version() {
    return rust::String(fdb5_version_str());
}

rust::String Library::git_sha1() {
    return rust::String(fdb5_git_sha1());
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge

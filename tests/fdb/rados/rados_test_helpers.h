// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Metin Cakircali
/// @date   Nov 2026

#pragma once

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/WipeIterator.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/rados/RadosCluster.h"
#include "eckit/io/rados/RadosNamespace.h"

#include <cstddef>
#include <iostream>
#include <string>
#include <vector>

namespace fdb::test::rados {

inline void deldir(eckit::PathName& p) {
    if (!p.exists()) {
        return;
    }

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;
    p.children(files, dirs);

    for (auto& f : files) {
        f.unlink();
    }
    for (auto& d : dirs) {
        deldir(d);
    }

    p.rmdir();
}

// Destroys the namespaces of `pool` starting with `prefix`. The prefix must be specific enough not to
// clobber unrelated namespaces in a shared pool.
inline void ensureCleanNamespaces(const std::string& pool, const std::string& prefix) {
    ASSERT(prefix.length() > 3);
    for (const std::string& name : eckit::RadosCluster::instance().listNamespaces(pool)) {
        if (name.rfind(prefix, 0) == 0) {
            eckit::RadosNamespace{pool, name}.destroy();
        }
    }
}

// Counts only the URIs that would actually be deleted, leaving out safe/info/error elements, so that a
// too specific request yields zero.
inline size_t countWipeable(fdb5::WipeIterator& wipeObject, bool print = true) {
    size_t count = 0;
    fdb5::WipeElement elem;
    while (wipeObject.next(elem)) {
        if (print) {
            std::cout << elem << std::endl;
        }
        if (elem.type() != fdb5::WipeElementType::ERROR && elem.type() != fdb5::WipeElementType::CATALOGUE_INFO &&
            elem.type() != fdb5::WipeElementType::CATALOGUE_SAFE && elem.type() != fdb5::WipeElementType::STORE_SAFE) {
            count += elem.uris().size();
        }
    }
    return count;
}

inline size_t countListed(fdb5::FDB& fdb, const fdb5::FDBToolRequest& request) {
    size_t count = 0;
    fdb5::ListElement info;
    auto listObject = fdb.list(request);
    while (listObject.next(info)) {
        ++count;
    }
    return count;
}

// What differs between the FDB configurations of the RADOS tests.
struct ConfigSpec {
    std::string root;  // filesystem root; left out of the configuration when empty
    std::string pool;
    std::string testId;  // prefix of the root and database namespaces
    std::string schema;
    std::string engine{"rados"};
    std::string store{"rados"};
    std::string extra;  // appended verbatim, e.g. a `rados:` block
};

// YAML with a single space and root placing databases in `<testId>_<db key>` of `pool`.
inline std::string config(const ConfigSpec& spec) {
    std::string yaml{"spaces:\n- roots:\n  - "};
    if (!spec.root.empty()) {
        yaml += "path: " + spec.root + "\n    ";
    }
    yaml += "pool: " + spec.pool + "\n    root_namespace: " + spec.testId +
            "_root\n    namespace_prefix: " + spec.testId + "\ntype: local\nschema : " + spec.schema +
            "\nengine: " + spec.engine + "\nstore: " + spec.store + "\n" + spec.extra;
    return yaml;
}

}  // namespace fdb::test::rados

// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   Archiver.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include <cstddef>
#include <ctime>
#include <iosfwd>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "fdb5/api/helpers/Callback.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"

namespace eckit {
class DataHandle;
}

class MarsTask;

namespace fdb5 {

class Key;
class BaseArchiveVisitor;
class Schema;

//----------------------------------------------------------------------------------------------------------------------

struct Database {
    time_t time_;
    std::shared_ptr<CatalogueWriter> catalogue_;
    std::unique_ptr<Store> store_;
};
class Archiver {

public:  // methods

    Archiver(const Config& dbConfig = Config().expandConfig(), const ArchiveCallback& callback = CALLBACK_ARCHIVE_NOOP);

    Archiver(const Archiver&) = delete;
    Archiver& operator=(const Archiver&) = delete;
    Archiver(Archiver&&) = delete;
    Archiver& operator=(Archiver&&) = delete;

    virtual ~Archiver();

    void archive(const Key& key, BaseArchiveVisitor& visitor);
    void archive(const Key& key, const void* data, size_t len);

    /// Flushes all buffers and closes all data handles into a consistent DB state
    /// @note always safe to call
    void flush();

    friend std::ostream& operator<<(std::ostream& s, const Archiver& x) {
        x.print(s);
        return s;
    }

protected:  // methods

    virtual void flushDatabase(Database& db);

private:  // methods

    void print(std::ostream& out) const;

    void selectDatabase(const Key& key);

protected:  // members

    std::map<Key, Database> databases_;

private:  // members

    friend class BaseArchiveVisitor;

    Config dbConfig_;

    std::vector<Key> prev_;

    Database* db_;

    std::recursive_mutex flushMutex_;
    std::mutex cacheMutex_;
    const ArchiveCallback& callback_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

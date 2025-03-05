/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocStore.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_TocStore_H
#define fdb5_TocStore_H

#include <map>
#include <memory>
#include <mutex>
#include <set>

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/TocCommon.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocStore : public Store, public TocCommon {

public:  // methods

    TocStore(const Key& key, const Config& config);

    ~TocStore() override {}

    eckit::URI uri() const override;
    bool uriBelongs(const eckit::URI&) const override;
    bool uriExists(const eckit::URI&) const override;
    std::vector<eckit::URI> collocatedDataURIs() const override;
    std::set<eckit::URI> asCollocatedDataURIs(const std::vector<eckit::URI>&) const override;

    bool open() override { return true; }
    size_t flush() override;
    void close() override;

    void checkUID() const override { TocCommon::checkUID(); }

    bool canMoveTo(const Key& key, const Config& config, const eckit::URI& dest) const override;
    void moveTo(const Key& key, const Config& config, const eckit::URI& dest,
                eckit::Queue<MoveElement>& queue) const override;
    void remove(const Key& key) const override;

    std::vector<eckit::URI> getAuxiliaryURIs(const eckit::URI&) const override;
    bool auxiliaryURIExists(const eckit::URI&) const override;
    std::set<std::string> auxFileExtensions() const;

protected:  // methods

    std::string type() const override { return "file"; }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    std::unique_ptr<const FieldLocation> archive(const Key& idxKey, const void* data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    eckit::DataHandle* getCachedHandle(const eckit::PathName& path) const;
    void closeDataHandles();
    std::unique_ptr<eckit::DataHandle> createFileHandle(const eckit::PathName& path);
    std::unique_ptr<eckit::DataHandle> createAsyncHandle(const eckit::PathName& path);
    std::unique_ptr<eckit::DataHandle> createDataHandle(const eckit::PathName& path);
    eckit::DataHandle& getDataHandle(const eckit::PathName& path);
    eckit::PathName generateDataPath(const Key& key) const;
    eckit::PathName getDataPath(const Key& key) const;
    void flushDataHandles();

    void print(std::ostream& out) const override;

private:  // methods

    eckit::URI getAuxiliaryURI(const eckit::URI&, const std::string& ext) const;

private:  // types

    typedef std::map<std::string, std::unique_ptr<eckit::DataHandle>> HandleStore;
    typedef std::map<Key, std::string> PathStore;

private:  // members

    mutable std::recursive_mutex handlesMutex_;
    HandleStore handles_;  ///< stores the DataHandles being used by the Session

    mutable PathStore dataPaths_;
    size_t archivedFields_;
    std::set<std::string> auxFileExtensions_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_TocStore_H

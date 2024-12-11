
#pragma once

#include "fdb5/api/FDBStats.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"
#include "fdb5/remote/client/Client.h"

namespace fdb5::remote {

// class RemoteCatalogueArchiver;
//----------------------------------------------------------------------------------------------------------------------

class RemoteCatalogue : public CatalogueReader, public CatalogueWriter, public CatalogueImpl, public Client {

public:

    RemoteCatalogue(const Key& key, const Config& config);
    RemoteCatalogue(const eckit::URI& uri, const Config& config);

    ~RemoteCatalogue() override;

    // From CatalogueWriter
    const Index& currentIndex() override;
    void archive(const Key& idxKey, const Key& datumKey, std::shared_ptr<const FieldLocation> fieldLocation) override;
    void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) override;
    void index(const Key& key, const eckit::URI& uri, eckit::Offset offset, eckit::Length length) override;
    void reconsolidate() override;

    //From CatalogueReader
    DbStats stats() const override { return DbStats(); }
    bool retrieve(const Key& key, Field& field) const override { return false; }

    // From Catalogue
    bool selectIndex(const Key& idxKey) override;
    const Key currentIndexKey() override;
    void deselectIndex() override;
    const Schema& schema() const override;

    std::vector<eckit::PathName> metadataPaths() const override;
    void visitEntries(EntryVisitor& visitor, bool sorted = false) override;
    void dump(std::ostream& out, bool simple=false, const eckit::Configuration& conf = eckit::LocalConfiguration()) const override;
    StatsReportVisitor* statsReportVisitor() const override;
    PurgeVisitor* purgeVisitor(const Store& store) const override;
    WipeVisitor* wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const override;
    MoveVisitor* moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const override;
    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override;
    std::vector<fdb5::Index> indexes(bool sorted=false) const override;
    void maskIndexEntry(const Index& index) const override;
    void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata, std::set<eckit::URI>& data) const override;
    void print( std::ostream &out ) const override;
    std::string type() const override;
    bool open() override;
    void flush(size_t archivedFields) override;
    void clean() override;
    void close() override;
    bool exists() const override;
    void checkUID() const override;
    eckit::URI uri() const override;

protected:

    void loadSchema() override;

private:
    // From Client
    // handlers for incoming messages - to be defined in the client class
    bool handle(Message message, bool control, uint32_t requestID) override;
    bool handle(Message message, bool control, uint32_t requestID, eckit::Buffer&& payload) override;

protected:

    Config config_;
    ControlIdentifiers controlIdentifiers_;

private:

    Key currentIndexKey_;
    std::unique_ptr<Schema> schema_;

    std::mutex archiveMutex_;
    size_t numLocations_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote

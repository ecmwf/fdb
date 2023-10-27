
#pragma once

#include "fdb5/api/FDBStats.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"
#include "fdb5/remote/client/Client.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class RemoteCatalogue : public CatalogueWriter, public CatalogueImpl, public Client {
public: //types
    using ArchiveQueue = eckit::Queue<std::pair<uint32_t, std::pair<Key, std::unique_ptr<FieldLocation> > > >;

public:

    RemoteCatalogue(const Key& key, const Config& config);
    
    RemoteCatalogue(const eckit::URI& uri, const Config& config);

    ~RemoteCatalogue() {}


    // From CatalogueWriter
    const Index& currentIndex() override;
    void archive(const InspectionKey& key, std::unique_ptr<FieldLocation> fieldLocation) override;
    void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) override;
    void index(const InspectionKey& key, const eckit::URI& uri, eckit::Offset offset, eckit::Length length) override;
    void reconsolidate() override;

    // From Catalogue
    bool selectIndex(const Key& idxKey) override;
    const Key currentIndexKey() override;
    void deselectIndex() override;
    const Schema& schema() const override;

    std::vector<eckit::PathName> metadataPaths() const override;
    void visitEntries(EntryVisitor& visitor, /*const Store& store,*/ bool sorted = false) override;
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
    void flush() override;
    void clean() override;
    void close() override;
    bool exists() const override;
    void checkUID() const override;
    eckit::URI uri() const override;


protected:

    Schema getSchema();
    void loadSchema() override;

private:
    // From Client

    // handlers for incoming messages - to be defined in the client class
    bool handle(Message message, uint32_t requestID) override;
    bool handle(Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload) override;
    void handleException(std::exception_ptr e) override;
    const Key& key() const override { return CatalogueImpl::key(); }


    // note: in common with remotestore
    FDBStats archiveThreadLoop();
    void sendArchiveData(uint32_t id, const Key& key, std::unique_ptr<FieldLocation> fieldLocation);

protected:

    Config config_;
    ControlIdentifiers controlIdentifiers_;

private:
    Key currentIndexKey_;
    std::unique_ptr<Schema> schema_;

    size_t maxArchiveQueueLength_;
    std::mutex archiveQueuePtrMutex_;
    std::future<FDBStats> archiveFuture_;
    std::unique_ptr<ArchiveQueue> archiveQueue_;


};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote

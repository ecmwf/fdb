/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/api/LocalFDB.h"

#include "eckit/container/Queue.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/local/AxesVisitor.h"
#include "fdb5/api/local/ControlVisitor.h"
#include "fdb5/api/local/DumpVisitor.h"
#include "fdb5/api/local/ListVisitor.h"
#include "fdb5/api/local/MoveVisitor.h"
#include "fdb5/api/local/PurgeVisitor.h"
#include "fdb5/api/local/StatsVisitor.h"
#include "fdb5/api/local/StatusVisitor.h"
#include "fdb5/api/local/WipeVisitor.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Inspector.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/Schema.h"


using namespace fdb5::api::local;
using namespace eckit;


namespace fdb5 {

void LocalFDB::archive(const Key& key, const void* data, size_t length) {

    if (!archiver_) {
        LOG_DEBUG_LIB(LibFdb5) << *this << ": Constructing new archiver" << std::endl;
        archiver_.reset(new Archiver(config_, archiveCallback_));
    }

    archiver_->archive(key, data, length);
}

void LocalFDB::reindex(const Key& key, const FieldLocation& location) {
    if (!reindexer_) {
        LOG_DEBUG_LIB(LibFdb5) << *this << ": Constructing new reindexer" << std::endl;
        reindexer_.reset(new Reindexer(config_));
    }

    reindexer_->reindex(key, location);
}

ListIterator LocalFDB::inspect(const metkit::mars::MarsRequest& request) {

    if (!inspector_) {
        LOG_DEBUG_LIB(LibFdb5) << *this << ": Constructing new retriever" << std::endl;
        inspector_.reset(new Inspector(config_));
    }

    return inspector_->inspect(request);
}

template <typename VisitorType, typename... Ts>
APIIterator<typename VisitorType::ValueType> LocalFDB::queryInternal(const FDBToolRequest& request, Ts... args) {

    using ValueType     = typename VisitorType::ValueType;
    using QueryIterator = APIIterator<ValueType>;
    using AsyncIterator = APIAsyncIterator<ValueType>;

    auto async_worker = [this, request, args...](Queue<ValueType>& queue) {
        EntryVisitMechanism mechanism(config_);
        VisitorType visitor(queue, request.request(), args...);
        mechanism.visit(request, visitor);
    };

    return QueryIterator(new AsyncIterator(async_worker));
}

ListIterator LocalFDB::list(const FDBToolRequest& request, const int level) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::list() : " << request << std::endl;
    return queryInternal<ListVisitor>(request, level);
}

DumpIterator LocalFDB::dump(const FDBToolRequest& request, bool simple) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::dump() : " << request << std::endl;
    return queryInternal<DumpVisitor>(request, simple);
}

StatusIterator LocalFDB::status(const FDBToolRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::status() : " << request << std::endl;
    return queryInternal<StatusVisitor>(request);
}

namespace  {

// Use WipeState from the Catalogue to assign safe and data URIs to the corresponding stores
std::unordered_map<eckit::URI, std::unique_ptr<StoreWipeState>> get_stores(WipeState& wipeState) {
    std::unordered_map<eckit::URI, std::unique_ptr<StoreWipeState>> storeWipeStates;

    auto storeState = [&](const eckit::URI& storeURI) -> StoreWipeState& {
        auto [it, inserted] = storeWipeStates.try_emplace(storeURI, nullptr);
        if (inserted || !it->second) {
            it->second = std::make_unique<StoreWipeState>(it->first, wipeState.config());
        }
        return *it->second;
    };

    for (const auto& dataURI : wipeState.includeURIs()) {
        eckit::URI storeURI = StoreFactory::instance().uri(dataURI);
        storeState(storeURI).addDataURI(dataURI);
    }

    for (const auto& dataURI : wipeState.excludeURIs()) {
        eckit::URI storeURI = StoreFactory::instance().uri(dataURI);
        storeState(storeURI).addSafeURI(dataURI);
    }

    return storeWipeStates;
}

} // namespace

WipeIterator LocalFDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::wipe() : " << request << std::endl;
    // return queryInternal<WipeVisitor>(request, doit, porcelain, unsafeWipeAll);
    using ValueType     = std::unique_ptr<WipeState>;
    using QueryIterator = APIIterator<ValueType>;
    using AsyncIterator = APIAsyncIterator<ValueType>;


    // insert into another iterator to drain it here...

    auto out_worker = [this, request, doit, porcelain, unsafeWipeAll](Queue<WipeElement>& out_queue) {
        
        auto async_worker = [this, request, doit, porcelain, unsafeWipeAll](Queue<ValueType>& queue) {
            EntryVisitMechanism mechanism(config_);
            WipeCatalogueVisitor visitor(queue, request.request(), doit, porcelain, unsafeWipeAll);
            mechanism.visit(request, visitor);
        };

        auto it = QueryIterator(new AsyncIterator(async_worker));
        std::unique_ptr<WipeState> catalogueWipeState;
        while (it.next(catalogueWipeState)) {
            if (!catalogueWipeState) {
                continue;
            }

            /// Per catalogue....
                    
            bool error   = false;  /// ?
            bool canWipe = true;   /// !!! unused.
            bool wipeAll = true;   /// ?
            std::map<eckit::URI, std::optional<eckit::URI>> unknownURIs;
            std::map<WipeElementType, std::shared_ptr<WipeElement>> storeElements;

            // Gather unknowns from the catalogue
            for (const auto& el : catalogueWipeState->wipeElements()) {
                if (el->type() == WipeElementType::WIPE_CATALOGUE_SAFE && !el->uris().empty()) {
                    wipeAll = false;
                }
                // if wipeAll, collect all the unknown URIs
                if (wipeAll && el->type() == WipeElementType::WIPE_UNKNOWN && !el->uris().empty()) {
                    for (const auto& uri : el->uris()) {
                        unknownURIs.emplace(uri, std::nullopt);
                    }
                }
            }

            auto storeWipeStates = get_stores(*catalogueWipeState);

            for (const auto& [storeURI, storeStatePtr] : storeWipeStates) {
                auto& storeState = *storeStatePtr;

                if (!storeState.safeURIs().empty()) {
                    wipeAll = false;
                }

                std::unique_ptr<Store> store = storeState.getStore();

                // This could act on the storeWipeState directly.
                auto elements = store->prepareWipe(storeState.dataURIs(), storeState.safeURIs(), wipeAll);

                for (const auto& el : elements) {
                    storeState.wipeElements().push_back(el); // could be done in prepare wipe.

                    const auto type = el->type();

                    // If wipeAll, collect all the unknown URIs
                    if (wipeAll && type == WipeElementType::WIPE_UNKNOWN) {
                        for (const auto& uri : el->uris()) {
                            unknownURIs.try_emplace(uri, storeURI);
                        }
                        continue;
                    }

                    // Group elements by type, merging URIs if the type already exists
                    auto [it, inserted] = storeElements.emplace(type, el);
                    if (!inserted) {
                        auto& target = *it->second;
                        for (const auto& uri : el->uris()) {
                            target.add(uri);
                        }
                    }
                }
            }

            auto isCatalogueElement = [](const auto& el) {
            const auto t = el->type();
            return (t == WipeElementType::WIPE_CATALOGUE || t == WipeElementType::WIPE_CATALOGUE_AUX) && !el->uris().empty();
            };

            auto isStoreElement = [](WipeElementType t) {
                return t == WipeElementType::WIPE_STORE || t == WipeElementType::WIPE_STORE_AUX;
            };


            ASSERT(storeElements.size() != 0);
            if (wipeAll && unknownURIs.size() > 0) {
                
                auto it = unknownURIs.begin();
                while (it != unknownURIs.end()) {

                    const auto& uri = it->first;

                    // Remove uri from the unknowns if it is among the catalogue wipe elements
                    bool found = false;
                    for (const auto& el : catalogueWipeState->wipeElements()) {
                        if (isCatalogueElement(el) && el->uris().count(uri)) {
                            it = unknownURIs.erase(it);
                            found = true;
                            break;
                        }
                    }

                    if (found) {
                        continue;
                    }
                    
                    // NB: Up to the store to do this now
                    for (const auto& [type, el] : storeElements) {
                        if (isStoreElement(type) && el->uris().count(uri)) {
                            it    = unknownURIs.erase(it);
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        ++it;  // Move to the next URI
                    }
                }
            }
            

            // PUSHING TO THE REPORT QUEUE

            if (wipeAll && doit && !unknownURIs.empty() && !unsafeWipeAll) {
                WipeElement el(WipeElementType::WIPE_ERROR, "Cannot fully wipe unclean FDB database:");
                out_queue.push(el);
                error = true;
            }

                // gather all non-unknown elements from the catalogue and the stores
            for (const auto& el : catalogueWipeState->wipeElements()) {
                if (el->type() != WipeElementType::WIPE_UNKNOWN) {
                    out_queue.push(*el);
                }
            }

            for (const auto& [type, el] : storeElements) {
                if (type != WipeElementType::WIPE_UNKNOWN) {
                    out_queue.push(*el);
                }
            }

            // gather the unknowns.
            // This can surely be done better...

            std::set<eckit::URI> unknownURIsSet;
            std::vector<eckit::URI> unknownURIsCatalogue;
            std::map<eckit::URI, std::vector<eckit::URI>> unknownURIsStore;
            for (const auto& [uri, storeURI] : unknownURIs) {
                unknownURIsSet.insert(uri);

                if (storeURI) {
                    auto it = unknownURIsStore.find(*storeURI);
                    if (it == unknownURIsStore.end()) {
                        unknownURIsStore.emplace(*storeURI, std::vector<eckit::URI>{uri});
                    } else {
                        it->second.push_back(uri);
                    }
                } else {
                    unknownURIsCatalogue.push_back(uri);
                }
            }

            out_queue.emplace(WipeElementType::WIPE_UNKNOWN, "Unexpected entries in FDB database:", std::move(unknownURIsSet));

            if (doit && !error) {
                std::unique_ptr<Catalogue> catalogue = catalogueWipeState->getCatalogue();
                catalogue->doWipe(unknownURIsCatalogue, *catalogueWipeState);
                for (const auto& [uri, storeState] : storeWipeStates) {
                    std::unique_ptr<Store> store = storeState->getStore();

                    auto it = unknownURIsStore.find(uri);
                    if (it == unknownURIsStore.end()) {
                        store->doWipe(std::vector<eckit::URI>{});
                    }
                    else {
                        store->doWipe(it->second);
                    }
                }
                catalogue->doWipe(*catalogueWipeState);
                for (const auto& [uri, storeState] : storeWipeStates) { // so much repetition...
                    std::unique_ptr<Store> store = storeState->getStore();
                    store->doWipe(*storeState);
                }

            }


            // wip wip wip wip

            // temp...
            // for (const auto& we : catalogueWipeState->wipeElements()) {
            //     out_queue.push(*we);
            // }
        }
        out_queue.close();
    };

    return APIIterator<WipeElement>(new APIAsyncIterator<WipeElement>(out_worker));
}

MoveIterator LocalFDB::move(const FDBToolRequest& request, const eckit::URI& dest) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::move() : " << request << std::endl;
    return queryInternal<fdb5::api::local::MoveVisitor>(request, dest);
}

PurgeIterator LocalFDB::purge(const FDBToolRequest& request, bool doit, bool porcelain) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::purge() : " << request << std::endl;
    return queryInternal<fdb5::api::local::PurgeVisitor>(request, doit, porcelain);
}

StatsIterator LocalFDB::stats(const FDBToolRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::stats() : " << request << std::endl;
    return queryInternal<StatsVisitor>(request);
}

ControlIterator LocalFDB::control(const FDBToolRequest& request, ControlAction action, ControlIdentifiers identifiers) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::control() : " << request << std::endl;
    return queryInternal<ControlVisitor>(request, action, identifiers);
}

AxesIterator LocalFDB::axesIterator(const FDBToolRequest& request, int level) {
    LOG_DEBUG_LIB(LibFdb5) << "LocalFDB::axesIterator() : " << request << std::endl;
    return queryInternal<AxesVisitor>(request, level);
}

void LocalFDB::flush() {
    ASSERT(!(archiver_ && reindexer_));
    if (archiver_) {
        archiver_->flush();
        flushCallback_();
    }
    else if (reindexer_) {
        reindexer_->flush();
        flushCallback_();
    }
}


void LocalFDB::print(std::ostream& s) const {
    s << "LocalFDB(home=" << config_.expandPath("~fdb") << ")";
}


static FDBBuilder<LocalFDB> localFdbBuilder("local");
static FDBBuilder<LocalFDB> builder("catalogue");  // Enable type=catalogue to build localFDB (serverside).
//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

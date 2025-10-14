
/*
 * (C) Copyright 2018- ECMWF.
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

#include "eckit/filesystem/URI.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/api/local/WipeVisitor.h"

#include <dirent.h>
#include <iostream>
#include <memory>
#include <sys/stat.h>

#include "eckit/os/Stat.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"
#include "fdb5/database/WipeState.h"

using namespace eckit;

namespace fdb5::api::local {

//----------------------------------------------------------------------------------------------------------------------

WipeCatalogueVisitor::WipeCatalogueVisitor(eckit::Queue<std::unique_ptr<WipeState>>& queue, const metkit::mars::MarsRequest& request, bool doit,
                         bool porcelain, bool unsafeWipeAll) :
    QueryVisitor<std::unique_ptr<WipeState>>(queue, request), doit_(doit), porcelain_(porcelain), unsafeWipeAll_(unsafeWipeAll) {}


bool WipeCatalogueVisitor::visitDatabase(const Catalogue& catalogue) {

    EntryVisitor::visitDatabase(catalogue);

    // If the Catalogue is locked for wiping, then it "doesn't exist"
    if (!catalogue.enabled(ControlIdentifier::Wipe)) {
        return false;
    }

    // // Check that we are in a clean state (i.e. we only visit one DB).
    // ASSERT(stores_.empty());
    // ASSERT(catalogue.wipeElements().empty());
    // ASSERT(catalogueWipeElements_.empty());
    // ASSERT(storeWipeElements_.empty());

    // @todo: Catalogue specific checks
    // wipeElements = catalogue.wipeInit();

    ASSERT(!catalogueWipeState_);
    catalogueWipeState_ = catalogue.wipeInit();
    if (!catalogueWipeState_) {
    //     for (const auto& el : catalogueWipeState_->wipeElements()) {
    //         queue_.push(*el); // but isnt this empty atm?
    //     }

        // Log::error() << "Catalogue " << catalogue.key() << " cannot be wiped." << std::endl;
        // auto errorIt = catalogueWipeElements_.find(WipeElementType::WIPE_ERROR);
        // if (errorIt != catalogueWipeElements_.end() && !errorIt->second.empty()) {
        //     for (const auto& el : errorIt->second) {
        //         Log::error() << "Error: " << el.msg() << std::endl;
        //         for (const auto& uri : el.uris()) {
        //             Log::error() << "    " << uri << std::endl;
        //         }
        //     }
        // }

        // If the catalogue cannot be wiped, then we don't proceed.
        // This is a safeguard against wiping a catalogue that is not in a state to be wiped.
        return false;
    }

    // Having selected a DB, construct the residual request. This is the request that is used for
    // matching Index(es) -- which is relevant if there is subselection of the database.
    indexRequest_ = request_;
    for (const auto& kv : catalogue.key()) {
        indexRequest_.unsetValues(kv.first);
    }

    return true;  // Explore contained indexes
}

void WipeCatalogueVisitor::aggregateURIs(const eckit::URI& dataURI, bool include, WipeState& wipeState) {

    if (include) {
        wipeState.include(dataURI);
    }
    else {
        wipeState.exclude(dataURI);
    }
}

bool WipeCatalogueVisitor::visitIndex(const Index& index) {

    EntryVisitor::visitIndex(index);

    // Is this index matched by the supplied request?
    // n.b. If the request is over-specified (i.e. below the index level), nothing will be removed
    bool include = index.key().match(indexRequest_);

    include = currentCatalogue_->wipeIndex(index, include, *catalogueWipeState_);

    // Enumerate data files
    for (auto& dataURI : index.dataURIs()) {
        aggregateURIs(dataURI, include, *catalogueWipeState_);
    }
    return true;
}


// this will go elsewhere eventually...
void WipeCatalogueVisitor::get_stores(const Catalogue& catalogue, const std::set<eckit::URI>& dataURIs, bool include) {

    for (const auto& dataURI : dataURIs) {
        auto storeURI = StoreFactory::instance().uri(dataURI);
        auto storeIt  = storeWipeStates_.find(storeURI);
        if (storeIt == storeWipeStates_.end()) {
            auto state = std::make_unique<StoreWipeState>(storeURI, catalogue.config());
            storeIt = storeWipeStates_.emplace(storeURI, std::move(state)).first;
        }
        if (include) {
            storeIt->second->addDataURI(dataURI);
        } else {
            storeIt->second->addSafeURI(dataURI);
        }
    }

}

void WipeCatalogueVisitor::catalogueComplete(const Catalogue& catalogue) {

    // We will be aggregating the following:
    std::map<eckit::URI, std::optional<eckit::URI>> unknownURIs;
    std::map<WipeElementType, std::shared_ptr<WipeElement>> storeElements;

    bool error   = false;  /// ?
    bool canWipe = true;   /// !!! unused.
    bool wipeAll = true;   /// ?

    ASSERT(currentCatalogue_ == &catalogue);

    auto maskedDataPaths = catalogue.wipeFinialise(*catalogueWipeState_); // why returning uris
    for (const auto& uri : maskedDataPaths) {
        /// XXX : Why are these always included?
        aggregateURIs(uri, true, *catalogueWipeState_);
    }

    const auto& catalogueElements = catalogueWipeState_->wipeElements();

    for (const auto& el : catalogueElements) {
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
    
    //  ----- begin store comms
    // Most of this logic now belongs outside the visitor.

    // send all the data and safe URIs to the stores
    get_stores(catalogue, catalogueWipeState_->includeURIs(), true);

    for (const auto& [storeURI, storeStatePtr] : storeWipeStates_) {
         
        auto& storeState = *storeStatePtr;

        if (!storeState.safeURIs().empty()) {
            wipeAll = false;
        }

        std::unique_ptr<Store> store = storeState.getStore();

        auto elements = store->prepareWipe(storeState.dataURIs(), storeState.safeURIs(), wipeAll);

        for (const auto& el : elements) {
            // !!! but this won't work when we're dividing into multiple stores
            storeState.wipeElements().push_back(el);

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
            for (const auto& el : catalogueElements) {
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

    if (wipeAll && doit_ && !unknownURIs.empty() && !unsafeWipeAll_) {
        WipeElement el(WipeElementType::WIPE_ERROR, "Cannot fully wipe unclean FDB database:");
        // queue_.push(el);
        catalogueWipeState_->outElements().push_back(std::make_shared<WipeElement>(el));
        error = true;
    }

    // gather all non-unknown elements from the catalogue and the stores
    for (const auto& el : catalogueElements) {
        if (el->type() != WipeElementType::WIPE_UNKNOWN) {
            // queue_.push(*el);
            catalogueWipeState_->outElements().push_back(el);
        }
    }

    for (const auto& [type, el] : storeElements) {
        if (type != WipeElementType::WIPE_UNKNOWN) {
            // queue_.push(*el);
            catalogueWipeState_->outElements().push_back(el);
        }
    }

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
    // queue_.emplace(WipeElementType::WIPE_UNKNOWN, "Unexpected entries in FDB database:", std::move(unknownURIsSet));
    catalogueWipeState_->outElements().push_back(
        std::make_shared<WipeElement>(WipeElementType::WIPE_UNKNOWN, "Unexpected entries in FDB database:", std::move(unknownURIsSet)));
    
    // -- This is no longer the right place to be checking do it, as we need to coordinate with the stores first.


    // todo, get rid of doit here...
    if (doit_ && !error) {
        catalogue.doWipe(unknownURIsCatalogue, *catalogueWipeState_);
        for (const auto& [uri, storeState] : storeWipeStates_) {
            std::unique_ptr<Store> store = storeState->getStore();

            auto it = unknownURIsStore.find(uri);
            if (it == unknownURIsStore.end()) {
                store->doWipe(std::vector<eckit::URI>{});
            }
            else {
                store->doWipe(it->second);
            }
        }
        catalogue.doWipe(*catalogueWipeState_);
        for (const auto& [uri, storeState] : storeWipeStates_) { // so much repetition...
            std::unique_ptr<Store> store = storeState->getStore();
            store->doWipe(*storeState);
        }
        storeWipeStates_.clear();
    }
    
    queue_.emplace(std::move(catalogueWipeState_));
    catalogueWipeState_.reset();
    EntryVisitor::catalogueComplete(catalogue);
}

// void WipeVisitor::doit(){
//     ASSERT(doit_);
//     std::cout << "WipeVisitor::catalogueComplete: DOIT wipe" << std::endl;
//     catalogue.doWipe(unknownURIsCatalogue, *wipeState_);
//     for (const auto& [uri, storeState] : stores) {
//         std::unique_ptr<Store> store = storeState.getStore();

//         auto it = unknownURIsStore.find(uri);
//         if (it == unknownURIsStore.end()) {
//             std::cout << "WipeVisitor::catalogueComplete: wipe store with no unknown URIs: " << *(store) << std::endl;
//             store->doWipe(std::vector<eckit::URI>{});
//         }
//         else {
//             std::cout << "WipeVisitor::catalogueComplete: wipe store with unknown URIs: " << *(store) << std::endl;
//             store->doWipe(it->second);
//         }
//     }
//     catalogue.doWipe(*wipeState_);
//     for (const auto& [uri, storeState] : stores) { // so much repetition...
//         std::unique_ptr<Store> store = storeState.getStore();
//         std::cout << "WipeVisitor::catalogueComplete: final wipe store: " << *(store) << std::endl;
//         store->doWipe(*wipeState_);
//     }
// }

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local
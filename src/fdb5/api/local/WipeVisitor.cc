
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

#include "fdb5/api/local/WipeVisitor.h"

#include <dirent.h>
#include <sys/stat.h>

#include "eckit/os/Stat.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"

using namespace eckit;

namespace fdb5::api::local {

//----------------------------------------------------------------------------------------------------------------------

WipeVisitor::WipeVisitor(eckit::Queue<WipeElement>& queue, const metkit::mars::MarsRequest& request, bool doit,
                         bool porcelain, bool unsafeWipeAll) :
    QueryVisitor<WipeElement>(queue, request), doit_(doit), porcelain_(porcelain), unsafeWipeAll_(unsafeWipeAll) {}


bool WipeVisitor::visitDatabase(const Catalogue& catalogue) {

    EntryVisitor::visitDatabase(catalogue);

    // If the Catalogue is locked for wiping, then it "doesn't exist"
    if (!catalogue.enabled(ControlIdentifier::Wipe)) {
        return false;
    }

    // // Check that we are in a clean state (i.e. we only visit one DB).
    ASSERT(stores_.empty());
    ASSERT(catalogue.wipeElements().empty());
    // ASSERT(catalogueWipeElements_.empty());
    // ASSERT(storeWipeElements_.empty());

    // @todo: Catalogue specific checks
    if (!catalogue.wipeInit()) {
        for (const auto& el : catalogue.wipeElements()) {
            queue_.push(*el);
        }

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

void WipeVisitor::storeURI(const eckit::URI& dataURI, bool include) {

    auto storeURI = StoreFactory::instance().uri(dataURI);
    auto storeIt  = stores_.find(storeURI);
    if (storeIt == stores_.end()) {
        auto store = StoreFactory::instance().build(storeURI, currentCatalogue_->config());
        ASSERT(store);
        storeIt = stores_.emplace(storeURI, StoreURIs{std::move(store), {}, {}}).first;
    }
    if (include) {
        storeIt->second.dataURIs.insert(dataURI);
    }
    else {
        storeIt->second.safeURIs.insert(dataURI);
    }
}

bool WipeVisitor::visitIndex(const Index& index) {

    EntryVisitor::visitIndex(index);

    // Is this index matched by the supplied request?
    // n.b. If the request is over-specified (i.e. below the index level), nothing will be removed
    bool include = index.key().match(indexRequest_);

    include = currentCatalogue_->wipeIndex(index, include);

    // Enumerate data files
    for (auto& dataURI : index.dataURIs()) {
        storeURI(dataURI, include);
    }
    return true;
}

void WipeVisitor::catalogueComplete(const Catalogue& catalogue) {

    bool error = false;
    ASSERT(currentCatalogue_ == &catalogue);

    auto maskedDataPaths = catalogue.wipeFinish();
    for (const auto& uri : maskedDataPaths) {
        storeURI(uri, true);
    }

    std::map<eckit::URI, std::optional<eckit::URI>> unknownURIs;
    const auto& catalogueElements = catalogue.wipeElements();
    bool wipeAll                  = true;

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
    
    // send all the data and safe URIs to the stores
    bool canWipe = true;
    for (const auto& [uri, ss] : stores_) {
        LOG_DEBUG_LIB(LibFdb5) << "WipeVisitor::catalogueComplete: contacting store " << *(ss.store) << std::endl
                               << "  dataURIs: " << ss.dataURIs.size() << ", safeURIs: " << ss.safeURIs.size()
                               << std::endl;
        for (const auto& dataURI : ss.dataURIs) {
            LOG_DEBUG_LIB(LibFdb5) << "  dataURI: " << dataURI << std::endl;
        }
        canWipe = canWipe && ss.store->canWipe(ss.dataURIs, ss.safeURIs, wipeAll, unsafeWipeAll_);

        if (ss.safeURIs.size() > 0) {
            wipeAll = false;
        }
    }

    std::map<WipeElementType, std::shared_ptr<WipeElement>> storeElements;
    for (const auto& [storeURI, ss] : stores_) {
        for (const auto& el : ss.store->wipeElements()) {
            auto t  = el->type();
            // if wipeAll, collect all the unknown URIs
            if (wipeAll && t == WipeElementType::WIPE_UNKNOWN) {
                // collect the unknown URIs from the store
                // these are the URIs that are not in the catalogue
                for (const auto& uri : el->uris()) {
                    if (unknownURIs.find(uri) == unknownURIs.end()) {
                        // if the URI is not in the unknownURIs, then add it
                        // this is to ensure that we don't delete URIs that are not in the catalogue
                        unknownURIs.emplace(uri, storeURI);
                    }
                }
            }
            else {
                auto it = storeElements.find(t);
                if (it == storeElements.end()) {
                    it = storeElements.emplace(t, el).first;
                }
                else {
                    // if the element is already in the map, we need to merge the URIs
                    for (const auto& uri : el->uris()) {
                        it->second->add(uri);
                    }
                }
            }
        }
    }

    if (wipeAll && unknownURIs.size() > 0) {
        auto it = unknownURIs.begin();
        while (it != unknownURIs.end()) {
            // check if the URI is in the catalogue wipe elements
            // if it is, then remove it from the unknownURIs
            bool found = false;
            for (const auto& el: catalogueElements) {
                if ((el->type() == WipeElementType::WIPE_CATALOGUE || el->type() == WipeElementType::WIPE_CATALOGUE_AUX) && !el->uris().empty()) {
                    if (el->uris().find(it->first) != el->uris().end()) {
                        it = unknownURIs.erase(it);
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                for (const auto& [type, el]: storeElements) {
                    if (!found && (type == WipeElementType::WIPE_STORE || type == WipeElementType::WIPE_STORE_AUX)) {
                        // check if the URI is in the store wipe elements
                        // if it is, then remove it from the unknownURIs
                        if (el->uris().find(it->first) != el->uris().end()) {
                            it = unknownURIs.erase(it);
                            found = true;
                            break;
                        }
                    }
                }
            }
            if (!found) {
                ++it;  // Move to the next URI
            }
        }
    }

    if (wipeAll && doit_ && !unknownURIs.empty() && !unsafeWipeAll_) {
        WipeElement el(WipeElementType::WIPE_ERROR, "Cannot fully wipe unclean FDB database:");
        queue_.push(el);
        error = true;
    }

    std::cout << "WipeVisitor::catalogueComplete: STEP1 " << std::endl;
    /// gather all the wipe elements from the catalogue and the stores
    for (const auto& el : catalogueElements) {
        if (el->type() != WipeElementType::WIPE_UNKNOWN) {
            queue_.push(*el);
        }
    }
    std::cout << "WipeVisitor::catalogueComplete: STEP2 " << std::endl;

    for (const auto& [type, el] : storeElements) {
        if (type != WipeElementType::WIPE_UNKNOWN) {
            queue_.push(*el);
        }
    }
    std::cout << "WipeVisitor::catalogueComplete: STEP3 " << std::endl;

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
        }
        else {
            unknownURIsCatalogue.push_back(uri);
        }
    }
    queue_.emplace(WipeElementType::WIPE_UNKNOWN, "Unexpected entries in FDB database:", std::move(unknownURIsSet));
    std::cout << "WipeVisitor::catalogueComplete: STEP4 " << std::endl;

    if (doit_ && !error) {
        currentCatalogue_->doWipe(unknownURIsCatalogue);
    std::cout << "WipeVisitor::catalogueComplete: STEP4.1 " << std::endl;
        for (const auto& [uri, ss] : stores_) {
            auto it = unknownURIsStore.find(uri);
            if (it == unknownURIsStore.end()) {
                std::cout << "STEP store " << uri << " - no Unknown" << std::endl;
                ss.store->doWipe(std::vector<eckit::URI>{});
            } else {
                std::cout << "STEP store " << uri << " - Unknowns: " << it->second.size() << std::endl;
                ss.store->doWipe(it->second);
            }
        }
    std::cout << "WipeVisitor::catalogueComplete: STEP4.2 " << std::endl;
        currentCatalogue_->doWipe();
    std::cout << "WipeVisitor::catalogueComplete: STEP4.3 " << std::endl;
        for (const auto& [uri, ss] : stores_) {
            ss.store->doWipe();
        }
    }
    std::cout << "WipeVisitor::catalogueComplete: STEP5 " << std::endl;

    stores_.clear();
    std::cout << "WipeVisitor::catalogueComplete: STEP6 " << std::endl;

    EntryVisitor::catalogueComplete(catalogue);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local
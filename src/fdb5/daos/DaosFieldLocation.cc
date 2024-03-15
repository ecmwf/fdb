/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/filesystem/URIManager.h"
#include "fdb5/daos/DaosFieldLocation.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/LibFdb5.h"

namespace fdb5 {

::eckit::ClassSpec DaosFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "DaosFieldLocation",};
::eckit::Reanimator<DaosFieldLocation> DaosFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

DaosFieldLocation::DaosFieldLocation(const DaosFieldLocation& rhs) :
    FieldLocation(rhs.uri_, rhs.offset_, rhs.length_, rhs.remapKey_) {}

DaosFieldLocation::DaosFieldLocation(const eckit::URI &uri) : FieldLocation(uri) {}

/// @todo: remove remapKey from signature and always pass empty Key to FieldLocation
DaosFieldLocation::DaosFieldLocation(const eckit::URI &uri, eckit::Offset offset, eckit::Length length, const Key& remapKey) :
    FieldLocation(uri, offset, length, remapKey) {}

DaosFieldLocation::DaosFieldLocation(eckit::Stream& s) :
    FieldLocation(s) {}

std::shared_ptr<FieldLocation> DaosFieldLocation::make_shared() const {
    return std::make_shared<DaosFieldLocation>(std::move(*this));
}

eckit::DataHandle* DaosFieldLocation::dataHandle() const {

    return fdb5::DaosArrayName(uri_).dataHandle(offset(), length());
    
}

void DaosFieldLocation::print(std::ostream &out) const {
    out << "DaosFieldLocation[uri=" << uri_ << "]";
}

void DaosFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

static FieldLocationBuilder<DaosFieldLocation> builder("daos");

//----------------------------------------------------------------------------------------------------------------------

class DaosURIManager : public eckit::URIManager {
    virtual bool query() override { return true; }
    virtual bool fragment() override { return true; }

    virtual eckit::PathName path(const eckit::URI& f) const override { return f.name(); }

    virtual bool exists(const eckit::URI& f) override {

        using namespace std::placeholders;
        eckit::Timer& timer = fdb5::DaosManager::instance().miscTimer();
        fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

        fdb5::StatsTimer st{"misc 00 daos uri manager check uri exists", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

        return fdb5::DaosName(f).exists();

    }

    virtual eckit::DataHandle* newWriteHandle(const eckit::URI& f) override {
        
        using namespace std::placeholders;
        eckit::Timer& timer = fdb5::DaosManager::instance().miscTimer();
        fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

        fdb5::StatsTimer st{"misc 01 daos uri manager new write handle", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

        if (fdb5::DaosName(f).OID().otype() != DAOS_OT_ARRAY) NOTIMP;
        
        return fdb5::DaosArrayName(f).dataHandle();
        
    }

    virtual eckit::DataHandle* newReadHandle(const eckit::URI& f) override {
        
        using namespace std::placeholders;
        eckit::Timer& timer = fdb5::DaosManager::instance().miscTimer();
        fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

        fdb5::StatsTimer st{"misc 02 daos uri manager new read handle", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

        if (fdb5::DaosName(f).OID().otype() != DAOS_OT_ARRAY) NOTIMP;
        
        return fdb5::DaosArrayName(f).dataHandle();
        
    }

    virtual eckit::DataHandle* newReadHandle(const eckit::URI& f, const eckit::OffsetList& ol, const eckit::LengthList& ll) override {
         
        using namespace std::placeholders;
        eckit::Timer& timer = fdb5::DaosManager::instance().miscTimer();
        fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

        fdb5::StatsTimer st{"misc 03 daos uri manager new read handle with off and len", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};

        if (fdb5::DaosName(f).OID().otype() != DAOS_OT_ARRAY) NOTIMP;
        
        return fdb5::DaosArrayName(f).dataHandle();
        
    }

    virtual std::string asString(const eckit::URI& uri) const override {
        std::string q = uri.query();
        if (!q.empty())
            q = "?" + q;
        std::string f = uri.fragment();
        if (!f.empty())
            f = "#" + f;

        return uri.scheme() + ":" + uri.name() + q + f;
    }
public:
    DaosURIManager(const std::string& name) : eckit::URIManager(name) {}
};

static DaosURIManager daos_uri_manager("daos");

} // namespace fdb5

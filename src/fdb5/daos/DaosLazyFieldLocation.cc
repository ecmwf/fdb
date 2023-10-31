/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/daos/DaosLazyFieldLocation.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosLazyFieldLocation::DaosLazyFieldLocation(const fdb5::DaosKeyValueName& index, const std::string& key) :
    FieldLocation(), index_(index), key_(key) {}

std::shared_ptr<FieldLocation> DaosLazyFieldLocation::make_shared() const {
    return realise()->make_shared();
}

eckit::DataHandle* DaosLazyFieldLocation::dataHandle() const {

    return realise()->dataHandle();
    
}

void DaosLazyFieldLocation::print(std::ostream &out) const {
    out << *realise();
}

void DaosLazyFieldLocation::visit(FieldLocationVisitor& visitor) const {
    realise()->visit(visitor);
}

std::unique_ptr<fdb5::FieldLocation>& DaosLazyFieldLocation::realise() const {

    if (fl_) return fl_;

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    /// @note: performed RPCs:
    /// - close index kv (daos_obj_close)
    fdb5::StatsTimer st{"list 011 index kv get field location", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    fdb5::DaosSession s{};
    fdb5::DaosKeyValue index_kv{s, index_};
    daos_size_t size{index_kv.size(key_)};
    std::vector<char> v((long) size);
    index_kv.get(key_, &v[0], size);
    st.stop();

    eckit::MemoryStream ms{&v[0], size};

    /// @note: timestamp read for informational purpoes. See note in DaosIndex::add.
    time_t ts;
    ms >> ts;

    fl_.reset(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));

    return fl_;

}

} // namespace fdb5

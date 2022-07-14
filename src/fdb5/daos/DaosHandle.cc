/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
///
/// @date Jul 2022

#include "eckit/config/Resource.h"
#include "eckit/utils/Tokenizer.h"

#include "fdb5/daos/DaosHandle.h"
#include "fdb5/daos/DaosCluster.h"

namespace fdb5 {

std::string oidToStr(const daos_obj_id_t& oid) {
    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;
    return os.str();
}

bool strToOid(const std::string& x, daos_obj_id_t *oid) {
    if (x.length() != 33)
        return false;
    // TODO: do better checks and avoid copy
    std::string y(x);
    y[16] = '0';
    if (!std::all_of(y.begin(), y.end(), ::isxdigit))
        return false;

    std::string hi = x.substr(0, 16);
    std::string lo = x.substr(17, 16);
    oid->hi = std::stoull(hi, nullptr, 16);
    oid->lo = std::stoull(lo, nullptr, 16);

    return true;
}

void DaosHandle::construct(std::string& title) {

    static const std::string defaultDaosPool = eckit::Resource<std::string>("defaultDaosPool", "default");

    eckit::Tokenizer parse(":");

    std::vector<std::string> bits;
    // pool:cont:oidhi.oidlo
    parse(title, bits);

    ASSERT(bits.size() == 1 || bits.size() == 2 || bits.size() == 3);

    bool get_oid = false;
    daos_obj_id_t oid;
    bool last_is_oid;

    if (bits.size() == 1) {

        pool_ = defaultDaosPool;
        cont_ = title;
        get_oid = true;

    } else if (bits.size() == 2) {

        last_is_oid = strToOid(bits[1], &oid);

        if (last_is_oid) {

            pool_ = defaultDaosPool;
            cont_ = bits[0];
            oid_ = oid;

        } else {

            pool_ = bits[0];
            cont_ = bits[1];
            get_oid = true;

        }

    } else {

        pool_ = bits[0];
        cont_ = bits[1];
        ASSERT(strToOid(bits[2], &oid_));

    }

    if (get_oid) {
        oid_ = DaosCluster::instance().getNextOid(pool_, cont_);
    }

}

DaosHandle::DaosHandle(std::string& title) : open_(false), offset_(0) {

    construct(title);

}

DaosHandle::DaosHandle(std::string& pool, std::string& cont) : open_(false), offset_(0) {

    std::string title = pool + ":" + cont;
    construct(title);

}

DaosHandle::DaosHandle(std::string& pool, std::string& cont, std::string& oid) : open_(false), offset_(0) {

    std::string title = pool + ":" + cont + ":" + oid;
    construct(title);

}

DaosHandle::DaosHandle(URI& uri) : open_(false), offset_(0) {

    std::string title = uri.name();
    construct(title);

}

DaosHandle::~DaosHandle() {

    if (open_) close();

}

void DaosHandle::openForWrite(const Length& len) {

    if (open_) NOTIMP;

    offset_ = eckit::Offset(0);

    DaosCluster::instance().poolConnect(pool_, &poh_);

    // TODO: improve this
    DaosCluster::instance().contCreateWithLabel(poh_, cont_);

    DaosCluster::instance().contOpen(poh_, cont_, &coh_);

    DaosCluster::instance().arrayCreate(coh_, oid_, &oh_);

    open_ = true;

}

void DaosHandle::openForAppend(const Length&) {

    if (open_) NOTIMP;

    DaosCluster::instance().poolConnect(pool_, &poh_);

    // TODO: should this opener crash if object not exists?
    DaosCluster::instance().contCreateWithLabel(poh_, cont_);

    DaosCluster::instance().contOpen(poh_, cont_, &coh_);

    DaosCluster::instance().arrayCreate(coh_, oid_, &oh_);

    open_ = true;

}

Length DaosHandle::openForRead() {

    if (open_) NOTIMP;

    offset_ = eckit::Offset(0);

    DaosCluster::instance().poolConnect(pool_, &poh_);

    DaosCluster::instance().contOpen(poh_, cont_, &coh_);

    DaosCluster::instance().arrayOpen(coh_, oid_, &oh_);

    open_ = true;

    return Length(DaosCluster::instance().arrayGetSize(oh_));

}

long DaosHandle::write(const void* buf, long len) {

    ASSERT(open_);

    daos_array_iod_t iod;
    daos_range_t rg;

    d_sg_list_t sgl;
    d_iov_t iov;

    iod.arr_nr = 1;
    rg.rg_len = (daos_size_t) len;
    rg.rg_idx = (daos_off_t) offset_;
    //rg.rg_idx = (daos_off_t) 0;
    iod.arr_rgs = &rg;

    sgl.sg_nr = 1;
    d_iov_set(&iov, (void*) buf, (size_t) len);
    sgl.sg_iovs = &iov;

    DAOS_CALL(daos_array_write(oh_, DAOS_TX_NONE, &iod, &sgl, NULL));

    offset_ += len;

    return len;

}

long DaosHandle::read(void* buf, long len) {

    ASSERT(open_);

    daos_array_iod_t iod;
    daos_range_t rg;

    d_sg_list_t sgl;
    d_iov_t iov;

    iod.arr_nr = 1;
    rg.rg_len = len;
    rg.rg_idx = (daos_off_t) offset_;
    //rg.rg_idx = (daos_off_t) 0;
    iod.arr_rgs = &rg;

    sgl.sg_nr = 1;
    d_iov_set(&iov, buf, (size_t) len);
    sgl.sg_iovs = &iov;

    DAOS_CALL(daos_array_read(oh_, DAOS_TX_NONE, &iod, &sgl, NULL));

    offset_ += len;

    return len;

}

void DaosHandle::close() {

    DaosCluster::instance().arrayClose(oh_);

    DaosCluster::instance().contClose(coh_);

    DaosCluster::instance().poolDisconnect(poh_);

    open_ = false;

}

void DaosHandle::flush() {

    // empty implmenetation

}

Length DaosHandle::size() {

    ASSERT(open_);
    return Length(DaosCluster::instance().arrayGetSize(oh_));

}

Offset DaosHandle::position() {

    return offset_;

}

Offset DaosHandle::seek(const Offset& offset) {

    offset_ = offset;
    // TODO: assert offset <= size() ?
    return offset_;

}

bool DaosHandle::canSeek() const {

    return true;

}

void DaosHandle::skip(const Length& len) {

    offset_ = Offset(len);

}

std::string DaosHandle::title() const {
    
    return pool_ + ':' + cont_ + ':' + oidToStr(oid_);

}

void DaosHandle::print(std::ostream& s) const {
    s << "DaosHandle[notimp]";
}

}  // namespace fdb5
// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/api/helpers/AxesIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

AxesElement::AxesElement(Key&& dbKey, IndexAxis&& axes) : dbKey_(std::move(dbKey)), axes_(std::move(axes)) {}

AxesElement::AxesElement(eckit::Stream& s) {
    s >> dbKey_;
    axes_ = IndexAxis(s, IndexAxis::currentVersion());
}

void AxesElement::print(std::ostream& out) const {
    out << "Axes(db=" << dbKey_ << ", axes=" << axes_ << ")";
}

size_t AxesElement::encodeSize() const {
    return dbKey_.encodeSize() + axes_.encodeSize(IndexAxis::currentVersion());
}

void AxesElement::encode(eckit::Stream& s) const {
    s << dbKey_;
    axes_.encode(s, IndexAxis::currentVersion());
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

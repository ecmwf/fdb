// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/fdb5_config.h"

#if fdb5_HAVE_GRIB
#include "fdb5/io/SingleGribMungePartFileHandle.h"
#endif

namespace fdb5 {

::eckit::ClassSpec TocFieldLocation::classSpec_ = {
    &FieldLocation::classSpec(),
    "TocFieldLocation",
};
::eckit::Reanimator<TocFieldLocation> TocFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

// TocFieldLocation::TocFieldLocation() {}

TocFieldLocation::TocFieldLocation(const eckit::PathName path, eckit::Offset offset, eckit::Length length,
                                   const Key& remapKey) :
    FieldLocation(eckit::URI("file", path), offset, length, remapKey) {}

TocFieldLocation::TocFieldLocation(const eckit::URI& uri) : FieldLocation(uri) {}

TocFieldLocation::TocFieldLocation(const eckit::URI& uri, eckit::Offset offset, eckit::Length length,
                                   const Key& remapKey) :
    FieldLocation(uri, offset, length, remapKey) {}

TocFieldLocation::TocFieldLocation(const TocFieldLocation& rhs) :
    FieldLocation(rhs.uri_, rhs.offset_, rhs.length_, rhs.remapKey_) {}

TocFieldLocation::TocFieldLocation(const UriStore& store, const FieldRef& ref) :
    FieldLocation(store.get(ref.uriId()), ref.offset(), ref.length(), Key()) {}

TocFieldLocation::TocFieldLocation(eckit::Stream& s) : FieldLocation(s) {}

std::shared_ptr<const FieldLocation> TocFieldLocation::make_shared() const {
    return std::make_shared<TocFieldLocation>(std::move(*this));
}

eckit::DataHandle* TocFieldLocation::dataHandle() const {
    if (remapKey_.empty()) {
        return uri_.path().partHandle(offset(), length());
    }
    else {
#if fdb5_HAVE_GRIB
        return new SingleGribMungePartFileHandle(uri_.path(), offset(), length(), remapKey_);
#else
        NOTIMP;
#endif
    }
}

void TocFieldLocation::print(std::ostream& out) const {
    out << "TocFieldLocation[uri=" << uri_ << ",offset=" << offset() << ",length=" << length()
        << ",remapKey=" << remapKey_ << "]";
}

void TocFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

void TocFieldLocation::encode(eckit::Stream& s) const {
    LOG_DEBUG(LibFdb5::instance().debug(), LibFdb5)
        << "TocFieldLocation encode URI " << uri_.asRawString() << std::endl;

    FieldLocation::encode(s);
}

static FieldLocationBuilder<TocFieldLocation> builder("file");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

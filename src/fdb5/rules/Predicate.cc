/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <ostream>
#include <string>
#include <utility>

#include "eckit/serialisation/Stream.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/Key.h"
#include "fdb5/rules/Matcher.h"
#include "fdb5/rules/Predicate.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec Predicate::classSpec_ = {&eckit::Streamable::classSpec(), "Predicate"};

eckit::Reanimator<Predicate> Predicate::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

Predicate::Predicate(std::string keyword, Matcher* matcher) : keyword_{std::move(keyword)}, matcher_{matcher} {}

Predicate::Predicate(eckit::Stream& stream) {
    stream >> keyword_;
    matcher_.reset(eckit::Reanimator<Matcher>::reanimate(stream));
}

//----------------------------------------------------------------------------------------------------------------------

void Predicate::encode(eckit::Stream& out) const {
    out << keyword_;
    out << *matcher_;
}

bool Predicate::match(const Key& key) const {
    return matcher_->match(keyword_, key);
}

bool Predicate::match(const std::string& value) const {
    return matcher_->match(value);
}

void Predicate::dump(std::ostream& out, const TypesRegistry& registry) const {
    matcher_->dump(out, keyword_, registry);
}

void Predicate::print(std::ostream& out) const {
    out << "Predicate[keyword=" << keyword_ << ",matcher=" << *matcher_ << "]";
}

bool Predicate::optional() const {
    return matcher_->optional();
}

const std::string& Predicate::value(const Key& key) const {
    return matcher_->value(key, keyword_);
}

const std::vector<std::string>& Predicate::values(const metkit::mars::MarsRequest& rq) const {
    return matcher_->values(rq, keyword_);
}

void Predicate::fill(Key& key, const std::string& value) const {
    matcher_->fill(key, keyword_, value);
}

const std::string& Predicate::defaultValue() const {
    return matcher_->defaultValue();
}

std::ostream& operator<<(std::ostream& out, const Predicate& predicate) {
    predicate.print(out);
    return out;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

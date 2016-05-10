/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/memory/ScopedPtr.h"

#include "fdb5/io/HandleGatherer.h"
#include "fdb5/config/MasterConfig.h"
#include "fdb5/database/ReadVisitor.h"
#include "fdb5/database/Retriever.h"
#include "fdb5/types/Type.h"

#include "marslib/MarsTask.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Retriever::Retriever(const MarsTask &task) :
    task_(task) {

    /// @note We may want to canonicalise the request immedietly HERE

}

Retriever::~Retriever() {
}

class RetrieveVisitor : public ReadVisitor {

    Retriever &owner_;

    eckit::ScopedPtr<DB> db_;

    std::string fdbReaderDB_;
    HandleGatherer &gatherer_;

public:
    RetrieveVisitor(Retriever &owner, const MarsTask &task, HandleGatherer &gatherer):
        owner_(owner),
        gatherer_(gatherer) {
        fdbReaderDB_ = eckit::Resource<std::string>("fdbReaderDB", "toc.reader");
    }

    ~RetrieveVisitor() {
    }

    // From Visitor

    virtual bool selectDatabase(const Key &key, const Key &full) {
        eckit::Log::info() << "selectDatabase " << key << std::endl;
        db_.reset(DBFactory::build(fdbReaderDB_, key));

        if (!db_->open()) {
            eckit::Log::info() << "Database does not exists " << key << std::endl;
            return false;
        } else {
            db_->checkSchema(key);
            return true;
        }
    }

    virtual bool selectIndex(const Key &key, const Key &full) {
        ASSERT(db_);
        eckit::Log::info() << "selectIndex " << key << std::endl;
        return db_->selectIndex(key);
    }

    virtual bool selectDatum(const Key &key, const Key &full) {
        ASSERT(db_);
        // eckit::Log::info() << "selectDatum " << key << ", " << full << std::endl;

        eckit::DataHandle *dh = db_->retrieve(key);

        if (dh) {
            gatherer_.add(dh);
        }

        return (dh != 0);
    }

    virtual void values(const MarsRequest &request, const std::string &keyword,
                        const TypesRegistry &registry,
                        eckit::StringList &values) {
        registry.lookupType(keyword).getValues(request, keyword, values, owner_.task_, db_.get());
    }

    virtual void print( std::ostream &out ) const {
        out << "RetrieveVisitor("
            << ")"
            << std::endl;
    }

};


eckit::DataHandle *Retriever::retrieve(const Schema &schema, bool sorted) {
    HandleGatherer result(sorted);

    RetrieveVisitor visitor(*this, task_, result);
    schema.expand(task_.request(), visitor);

    return result.dataHandle();
}

eckit::DataHandle *Retriever::retrieve() {
    //    Log::info() << std::endl
    //                << "---------------------------------------------------------------------------------------------------"
    //                << std::endl
    //                << std::endl
    //                << *this
    //                << std::endl;

    bool sorted = false;
    std::vector<std::string> sort;
    task_.request().getValues("optimise", sort);

    if (sort.size() == 1 && sort[0] == "on") {
        sorted = true;
    }

    const Schema &schema = MasterConfig::instance().schema();

    // TODO: this logic does not work if a retrieval spans several
    // databases with different schemas. Another SchemaHasChanged will be thrown.
    try {

        return retrieve(schema, sorted);

    } catch (SchemaHasChanged &e) {

        eckit::Log::error() << e.what() << std::endl;
        eckit::Log::error() << "Trying with old schema: " << e.path() << std::endl;

        return retrieve(Schema(e.path()), sorted);
    }

}

void Retriever::print(std::ostream &out) const {
    out << "Retriever[request" << eckit::setformat(out, Log::compactFormat) << task_.request()
        << "]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

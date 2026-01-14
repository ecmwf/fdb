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

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDBFactory.h"

#include <random>

using namespace eckit;


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class RandomFDBBuilder : public FDBBuilderBase {

public:  // methods

    RandomFDBBuilder() : FDBBuilderBase("random") {}

private:

    std::shared_ptr<FDBBase> make(const Config& config) const {

        // Get a list of FDBs to choose between

        if (!config.has("fdbs"))
            throw UserError("No FDBs configured for random FDB frontend", Here());

        auto fdbConfigs = config.getSubConfigurations("fdbs");

        if (fdbConfigs.empty())
            throw UserError("No FDBs configured for random FDB frontend", Here());

        // And pick one of them at random to build

        std::random_device rd;
        int choice = std::uniform_int_distribution<int>(0, fdbConfigs.size() - 1)(rd);

        LOG_DEBUG_LIB(LibFdb5) << "Constructing random API instance: " << choice + 1 << " / " << fdbConfigs.size()
                               << std::endl;

        ASSERT(choice >= 0);
        ASSERT(choice < fdbConfigs.size());

        return FDBFactory::instance().build(fdbConfigs[choice]);
    }
};

static RandomFDBBuilder randomFDBBuilder;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

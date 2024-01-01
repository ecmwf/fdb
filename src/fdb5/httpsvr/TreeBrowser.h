/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   Dec 2023

#pragma once

#include "eckit/web/HttpResource.h"

namespace fdb5 {
namespace httpsvr {

//----------------------------------------------------------------------------------------------------------------------

class TreeBrowser : public eckit::HttpResource {

public: // methods

    TreeBrowser();
    ~TreeBrowser() override;

    void GET(std::ostream& out, eckit::Url& url) override;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace httpsvr
} // namespace fdb5

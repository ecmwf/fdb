/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Retriever.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_Retriever_H
#define fdb_Retriever_H

#include <cstdlib>
#include <vector>

#include "eckit/memory/NonCopyable.h"

#include "fdb5/Winds.h"

namespace eckit { class DataHandle; }
namespace marskit { class MarsRequest; }

namespace fdb {

class FdbTask;
class Op;

//----------------------------------------------------------------------------------------------------------------------

class Retriever : public eckit::NonCopyable {

public: // methods

    Retriever(const FdbTask& task);
    
    ~Retriever();

    /// Retrieves the data selected by the MarsRequest to the provided DataHandle
    /// @returns  data handle to read from

    eckit::DataHandle* retrieve();

private: // methods

    void retrieve(marskit::MarsRequest& field,
                  const std::vector<std::string>& paramsList,
                  size_t pos,
                  Op& op);

private: // members

    const FdbTask& task_;

    Winds winds_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif

/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   UVOp.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_UVOp_H
#define fdb5_UVOp_H

#include <map>

#include "fdb5/Key.h"
#include "fdb5/Op.h"
#include "fdb5/Winds.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class UVOp : public fdb5::Op {

public: // methods

    UVOp(const MarsTask& task, Op& parent);

    /// Destructor

    virtual ~UVOp();

private: // methods

    virtual void enter(const std::string& param, const std::string& value);
    virtual void leave();

    virtual void execute(const MarsTask& task, const Key& key, Op &tail);

    virtual void fail(const MarsTask& task, const Key& key, Op& tail);

    virtual void print( std::ostream& out ) const;

private:

    typedef std::map<Key, Winds> WindsMap;

    Op& parent_;
    Winds userWinds_;
    Key windsKey_;

    WindsMap winds_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

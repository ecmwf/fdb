/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Tiago Quintino
/// @date Dec 2014

#ifndef fdb5_TocInitialiser_H
#define fdb5_TocInitialiser_H

#include "fdb5/TocHandler.h"

namespace fdb5 {

class TocSchema;
class Key;

//-----------------------------------------------------------------------------

/// Initialises an FDB directory with an empty Toc
/// On existing directories has no effect.
class TocInitialiser : public TocHandler {

public: // methods

    TocInitialiser( const eckit::PathName& dir, const Key& tocKey );

};

//-----------------------------------------------------------------------------

} // namespace fdb5

#endif

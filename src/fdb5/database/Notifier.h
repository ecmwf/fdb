/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Notifier.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_Notifier_H
#define fdb5_Notifier_H


namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------

class Notifier {

public:  // methods

    Notifier();

    virtual ~Notifier();

    virtual void notifyWind() const = 0;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

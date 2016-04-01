/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Winds.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_Winds_H
#define fdb_Winds_H

#include <string>

namespace marskit { class MarsRequest; }

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

class Winds {

public: // methods

    Winds();
    Winds(const marskit::MarsRequest& field);

    void reset();

    static bool isU(const std::string& param);
    static bool isV(const std::string& param);

    static bool isVO(const std::string& param);
    static bool isD(const std::string& param);

    static std::string getVO(const std::string& param);
    static std::string getD(const std::string& param);

    void print( std::ostream& out ) const;

    friend std::ostream& operator<<(std::ostream& s,const Winds& x);

public: // members

    bool wantU_;
    bool wantV_;
    bool wantVO_;
    bool wantD_;

    bool UfromVOD_;
    bool VfromVOD_;

    bool gotVO_;
    bool gotD_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif

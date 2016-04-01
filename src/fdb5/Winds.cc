/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "marskit/MarsRequest.h"

#include "fdb5/Winds.h"

using namespace eckit;
using namespace marskit;

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

Winds::Winds() :
    wantU_(false),
    wantV_(false),
    wantVO_(false),
    wantD_(false),
    UfromVOD_(false),
    VfromVOD_(false),
    gotVO_(false),
    gotD_(false)
{
}

Winds::Winds(const marskit::MarsRequest& field) :
    wantU_(false),
    wantV_(false),
    wantVO_(false),
    wantD_(false),
    UfromVOD_(false),
    VfromVOD_(false),
    gotVO_(false),
    gotD_(false)
{
    std::vector<std::string> params;
    field.getValues("param", params);

    for(std::vector<std::string>::const_iterator itr = params.begin(); itr != params.end(); ++itr) {

        /// @TODO implement a match with a table of parameters that represent UV
        ///       Use the Wind Table used in (Mars)Param.cc

        if(isU(*itr)) {
            wantU_ = true;
        }
        if(isV(*itr)) {
            wantV_ = true;
        }
        if(isVO(*itr)) {
            wantVO_ = true;
        }
        if(isD(*itr)) {
            wantD_ = true;
        }
    }
}

void Winds::reset()
{
    UfromVOD_ = false;
    VfromVOD_ = false;
    gotVO_ = false;
    gotD_ = false;
}

std::string Winds::getVO(const std::string& param)
{
    return "138";
}

std::string Winds::getD(const std::string& param)
{
    return "155";
}

bool Winds::isU(const std::string& param)
{
    return param == "131";
}

bool Winds::isV(const std::string& param)
{
    return param == "132";
}

bool Winds::isVO(const std::string& param)
{
    return param == "138";
}

bool Winds::isD(const std::string& param)
{
    return param == "155";
}

void Winds::print(std::ostream &out) const
{
    out << "Winds("
        << "wantU=" << wantU_ << ","
        << "wantV=" << wantV_ << ","
        << "wantVO=" << wantVO_ << ","
        << "wantD=" << wantD_ << ","
        << "UfromVOD=" << UfromVOD_ << ","
        << "VfromVOD=" << VfromVOD_ << ","
        << "gotVO=" << gotVO_ << ","
        << "gotD=" << gotD_ << ","
        << ")";
}

std::ostream& operator<<(std::ostream& s, const Winds& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

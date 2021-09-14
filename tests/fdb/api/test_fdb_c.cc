/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstdlib>
#include <string.h>

#include "eckit/config/Resource.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpFile.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/fdb_c.h"

using namespace eckit::testing;
using namespace eckit;


namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------
int fdb_request_add1(fdb_request_t* req, const char* param, const char* value) {
    return fdb_request_add(req, param, &value, 1);
}

CASE( "fdb_c - archive & list" ) {
    size_t length;
    DataHandle *dh;

    fdb_handle_t* fdb;
    fdb_new_handle(&fdb);

    fdb_key_t* key;
    fdb_new_key(&key);
    fdb_key_add(key, "domain", "g");
    fdb_key_add(key, "stream", "oper");
    fdb_key_add(key, "levtype", "pl");
    fdb_key_add(key, "levelist", "300");
    fdb_key_add(key, "date", "20191110");
    fdb_key_add(key, "time", "0000");
    fdb_key_add(key, "step", "0");
    fdb_key_add(key, "param", "138");
    fdb_key_add(key, "class", "rd");
    fdb_key_add(key, "type", "an");
    fdb_key_add(key, "expver", "xxxx");

    eckit::PathName grib1("x138-300.grib");
    length = grib1.size();
    eckit::Buffer buf1(length);
    dh = grib1.fileHandle();
    dh->openForRead();
    dh->read(buf1, length);
    dh->close();

    EXPECT(FDB_SUCCESS == fdb_archive(fdb, key, buf1, length));
    EXPECT(FDB_SUCCESS == fdb_flush(fdb));

    fdb_request_t* request;
    fdb_new_request(&request);
    fdb_request_add1(request, "domain", "g");
    fdb_request_add1(request, "stream", "oper");
    fdb_request_add1(request, "levtype", "pl");
    fdb_request_add1(request, "levelist", "300");
    fdb_request_add1(request, "date", "20191110");
    fdb_request_add1(request, "time", "0000");
    fdb_request_add1(request, "step", "0");
    fdb_request_add1(request, "param", "138");
    fdb_request_add1(request, "class", "rd");
    fdb_request_add1(request, "type", "an");
    fdb_request_add1(request, "expver", "xxxx");

    const char **item= new const char*;
    bool exist;
    fdb_listiterator_t* it;
    fdb_new_listiterator(&it);
    fdb_list(fdb, request, it);
    fdb_listiterator_next(it, &exist, item);
    ASSERT(exist);
    std::string s1("{class=rd,expver=xxxx,stream=oper,date=20191110,time=0000,domain=g}{type=an,levtype=pl}{step=0,levelist=300,param=138}");
    EXPECT_NOT_EQUAL(std::string(*item).find(s1), std::string::npos);
    fdb_delete_listiterator(it);

    fdb_request_add1(request, "param", "139");
    fdb_new_listiterator(&it);
    fdb_list(fdb, request, it);
    fdb_listiterator_next(it, &exist, item);
    ASSERT(!exist);
    fdb_delete_listiterator(it);

    fdb_key_add(key, "levelist", "400");

    eckit::PathName grib2("x138-400.grib");
    length = grib2.size();
    eckit::Buffer buf2(length);
    dh = grib2.fileHandle();
    dh->openForRead();
    dh->read(buf2, length);
    dh->close();

    EXPECT(FDB_SUCCESS == fdb_archive(fdb, key, buf2, length));
    EXPECT(FDB_SUCCESS == fdb_flush(fdb));

    fdb_request_add1(request, "levelist", "400");
    fdb_new_listiterator(&it);
    fdb_list(fdb, request, it);
    fdb_listiterator_next(it, &exist, item);
    ASSERT(!exist);
    fdb_delete_listiterator(it);


    fdb_request_add1(request, "param", "138");
    fdb_new_listiterator(&it);
    fdb_list(fdb, request, it);
    fdb_listiterator_next(it, &exist, item);
    ASSERT(exist);
    std::string s2("{class=rd,expver=xxxx,stream=oper,date=20191110,time=0000,domain=g}{type=an,levtype=pl}{step=0,levelist=400,param=138}");
    EXPECT_NOT_EQUAL(std::string(*item).find(s2), std::string::npos);
    fdb_delete_listiterator(it);

    const char* values[] = {"400", "300"};
    fdb_request_add(request, "levelist", values, 2);
    fdb_new_listiterator(&it);
    fdb_list(fdb, request, it);
    fdb_listiterator_next(it, &exist, item);
    ASSERT(exist);
    EXPECT_NOT_EQUAL(std::string(*item).find(s2), std::string::npos);
    fdb_listiterator_next(it, &exist, item);
    ASSERT(exist);
    EXPECT_NOT_EQUAL(std::string(*item).find(s1), std::string::npos);
    fdb_delete_listiterator(it);


    fdb_key_add(key, "expver", "xxxy");

    eckit::PathName grib3("y138-400.grib");
    length = grib3.size();
    eckit::Buffer buf3(length);
    dh = grib3.fileHandle();
    dh->openForRead();
    dh->read(buf3, length);
    dh->close();

}


CASE( "fdb_c - multiple archive & list" ) {
    size_t length1, length2, length3;
    DataHandle *dh;

    fdb_handle_t* fdb;
    fdb_new_handle(&fdb);

    std::string s1("{class=rd,expver=xxxx,stream=oper,date=20191110,time=0000,domain=g}{type=an,levtype=pl}{step=0,levelist=300,param=138}");
    std::string s2("{class=rd,expver=xxxx,stream=oper,date=20191110,time=0000,domain=g}{type=an,levtype=pl}{step=0,levelist=400,param=138}");

    eckit::PathName grib1("x138-300.grib");
    length1 = grib1.size();
    eckit::PathName grib2("x138-400.grib");
    length2 = grib2.size();
    eckit::PathName grib3("y138-400.grib");
    length3 = grib3.size();

    eckit::Buffer buf(length1+length2+length3);
    dh = grib1.fileHandle();
    dh->openForRead();
    dh->read(buf, length1);
    dh->close();

    dh = grib2.fileHandle();
    dh->openForRead();
    dh->read(buf+length1, length2);
    dh->close();

    fdb_request_t* req;
    fdb_new_request(&req);
    fdb_request_add1(req, "domain", "g");
    fdb_request_add1(req, "stream", "oper");
    fdb_request_add1(req, "levtype", "pl");
    const char* levels[] = {"400", "300"};
    fdb_request_add(req, "levelist", levels, 2);
    fdb_request_add1(req, "date", "20191110");
    fdb_request_add1(req, "time", "0000");
    fdb_request_add1(req, "step", "0");
    fdb_request_add1(req, "param", "138");
    fdb_request_add1(req, "class", "rd");
    fdb_request_add1(req, "type", "an");
    fdb_request_add1(req, "expver", "xxxx");

    EXPECT(FDB_ERROR_GENERAL_EXCEPTION == fdb_archive_multiple(fdb, req, buf, length1));
    EXPECT(FDB_SUCCESS == fdb_flush(fdb));

    EXPECT(FDB_SUCCESS == fdb_archive_multiple(fdb, req, buf, length1+length2));
    EXPECT(FDB_SUCCESS == fdb_flush(fdb));

    dh = grib3.fileHandle();
    dh->openForRead();
    dh->read(buf+length1+length2, length3);
    dh->close();

    const char* expvers[] = {"xxxx", "xxxy"};
    fdb_request_add(req, "expver", expvers, 2);

    EXPECT(FDB_ERROR_GENERAL_EXCEPTION == fdb_archive_multiple(fdb, req, buf, length1+length2+length3));
    EXPECT(FDB_SUCCESS == fdb_flush(fdb));

    EXPECT(FDB_SUCCESS == fdb_archive_multiple(fdb, nullptr, buf, length1+length2+length3));
    EXPECT(FDB_SUCCESS == fdb_flush(fdb));

    fdb_request_t* request;
    fdb_new_request(&request);
    fdb_request_add1(request, "domain", "g");
    fdb_request_add1(request, "stream", "oper");
    fdb_request_add1(request, "levtype", "pl");
    fdb_request_add1(request, "levelist", "300");
    fdb_request_add1(request, "date", "20191110");
    fdb_request_add1(request, "time", "0000");
    fdb_request_add1(request, "step", "0");
    fdb_request_add1(request, "param", "138");
    fdb_request_add1(request, "class", "rd");
    fdb_request_add1(request, "type", "an");
    fdb_request_add1(request, "expver", "xxxx");

    const char **item= new const char*;
    bool exist;
    fdb_listiterator_t* it;
    fdb_new_listiterator(&it);
    fdb_list(fdb, request, it);
    fdb_listiterator_next(it, &exist, item);
    ASSERT(exist);
    EXPECT_NOT_EQUAL(std::string(*item).find(s1), std::string::npos);
    fdb_delete_listiterator(it);

    fdb_request_add1(request, "step", "1");
    fdb_new_listiterator(&it);
    fdb_list(fdb, request, it);
    fdb_listiterator_next(it, &exist, item);
    ASSERT(!exist);
    fdb_delete_listiterator(it);

    fdb_request_add1(request, "step", "0");
    const char* values[] = {"400", "300"};
    fdb_request_add(request, "levelist", values, 2);
    fdb_new_listiterator(&it);
    fdb_list(fdb, request, it);
    fdb_listiterator_next(it, &exist, item);
    ASSERT(exist);
    EXPECT_NOT_EQUAL(std::string(*item).find(s1), std::string::npos);
    fdb_listiterator_next(it, &exist, item);
    ASSERT(exist);
    EXPECT_NOT_EQUAL(std::string(*item).find(s2), std::string::npos);
    fdb_delete_listiterator(it);
}

CASE( "fdb_c - retrieve bad request" ) {

    fdb_handle_t* fdb;
    fdb_new_handle(&fdb);
    fdb_request_t* request;
    fdb_new_request(&request);
    fdb_request_add1(request, "domain", "g");
    fdb_request_add1(request, "stream", "oper");
    fdb_request_add1(request, "levtype", "pl");
    fdb_request_add1(request, "levelist", "300");
    fdb_request_add1(request, "date", "20191110");
    fdb_request_add1(request, "time", "0000");
    fdb_request_add1(request, "step", "0");
    fdb_request_add1(request, "param", "138/139");
    fdb_request_add1(request, "class", "rd");
    fdb_request_add1(request, "type", "an");
    fdb_request_add1(request, "expver", "xxxx");

    char buf[1000];
    char grib[4];
    long read = 0;
    long size;
    fdb_datareader_t* dr;
    fdb_new_datareader(&dr);
//  thrown by deduplication (now deactivted)
//    EXPECT(fdb_retrieve(fdb, request, dr) == FDB_ERROR_GENERAL_EXCEPTION);
}

CASE( "fdb_c - retrieve" ) {

    fdb_handle_t* fdb;
    fdb_new_handle(&fdb);
    fdb_request_t* request;
    fdb_new_request(&request);
    fdb_request_add1(request, "domain", "g");
    fdb_request_add1(request, "stream", "oper");
    fdb_request_add1(request, "levtype", "pl");
    fdb_request_add1(request, "levelist", "300");
    fdb_request_add1(request, "date", "20191110");
    fdb_request_add1(request, "time", "0000");
    fdb_request_add1(request, "step", "0");
    fdb_request_add1(request, "param", "138");
    fdb_request_add1(request, "class", "rd");
    fdb_request_add1(request, "type", "an");
    fdb_request_add1(request, "expver", "xxxx");

    char buf[1000];
    char grib[4];
    long read = 0;
    long size;
    fdb_datareader_t* dr;
    fdb_new_datareader(&dr);
    EXPECT(fdb_retrieve(fdb, request, dr) == FDB_SUCCESS);
    fdb_datareader_open(dr, &size);
    EXPECT_NOT_EQUAL(0, size);
    fdb_datareader_read(dr, grib, 4, &read);
    EXPECT_EQUAL(4, read);
    EXPECT_EQUAL(0, strncmp(grib, "GRIB", 4));
    fdb_datareader_tell(dr, &read);
    EXPECT_EQUAL(4, read);
    fdb_datareader_seek(dr, 3);
    fdb_datareader_tell(dr, &read);
    EXPECT_EQUAL(3, read);
    fdb_datareader_skip(dr, 3);
    fdb_datareader_tell(dr, &read);
    EXPECT_EQUAL(6, read);
    fdb_datareader_read(dr, buf, 1000, &read);
    EXPECT_EQUAL(1000, read);
    fdb_datareader_tell(dr, &read);
    EXPECT_EQUAL(1006, read);
    fdb_delete_datareader(dr);

    long size2;
    const char* values[] = {"400", "300"};
    fdb_request_add(request, "levelist", values, 2);
    fdb_new_datareader(&dr);
    EXPECT(fdb_retrieve(fdb, request, dr) == FDB_SUCCESS);
    fdb_datareader_open(dr, &size2);
    EXPECT_EQUAL(2*size, size2);
    fdb_datareader_seek(dr, size);
    fdb_datareader_read(dr, grib, 4, &read);
    EXPECT_EQUAL(4, read);
    EXPECT_EQUAL(0, strncmp(grib, "GRIB", 4));
    fdb_datareader_tell(dr, &read);
    EXPECT_EQUAL(4+size, read);
    fdb_datareader_seek(dr, size2-4);
    fdb_datareader_read(dr, grib, 6, &read);
    EXPECT_EQUAL(4, read);
    fdb_delete_datareader(dr);

}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    fdb_initialise();

    return run_tests ( argc, argv );
}

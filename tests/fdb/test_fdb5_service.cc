/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Tiago Quintino
/// @date Sep 2012

#include <cstring>

#include "eckit/io/DataHandle.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/MultiHandle.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/runtime/Main.h"
#include "eckit/types/Types.h"
#include "eckit/utils/Translator.h"

#include "metkit/MarsRequest.h"
#include "metkit/types/TypeAny.h"

#include "fdb5/database/Key.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/ArchiveVisitor.h"
#include "fdb5/database/Retriever.h"

#include "eckit/testing/Test.h"

using namespace std;
using namespace eckit;
using namespace eckit::testing;
using namespace fdb5;


namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

struct FixtureService {

    metkit::MarsRequest env;
    StringDict p;

	std::vector<std::string> modelParams_;

    FixtureService() : env("environ")
	{
        p["class"]  = "rd";
		p["stream"] = "oper";
		p["domain"] = "g";
		p["expver"] = "0001";
		p["date"] = "20120911";
		p["time"] = "0000";
        p["type"] = "fc";

        modelParams_.push_back( "130.128" );
        modelParams_.push_back( "138.128" );
	}

    void write_cycle(fdb5::Archiver& fdb, StringDict& p )
	{
		Translator<size_t,std::string> str;
		std::vector<std::string>::iterator param = modelParams_.begin();
		for( ; param != modelParams_.end(); ++param )
		{
			p["param"] = *param;

			p["levtype"] = "pl";

			for( size_t step = 0; step < 12; ++step )
			{
				p["step"] = str(step*3);

				for( size_t level = 0; level < 10; ++level )
				{
					p["levelist"] = str(level*100);

					std::ostringstream data;
					data << "Raining cats and dogs -- "
						 << " param " << *param
						 << " step "  << step
						 << " level " << level
						 << std::endl;
					std::string data_str = data.str();

                    fdb5::Key k(p);
                    ArchiveVisitor visitor(fdb, k, static_cast<const void *>(data_str.c_str()), data_str.size());
                    fdb.archive(k, visitor);
				}
			}
		}
	}
};

//----------------------------------------------------------------------------------------------------------------------

CASE ( "test_fdb_service" ) {

	SETUP("Fixture") {
		FixtureService f;

		SECTION( "test_fdb_service_write" )
		{
			fdb5::Archiver fdb;

            f.p["class"]  = "rd";
			f.p["stream"] = "oper";
			f.p["domain"] = "g";
			f.p["expver"] = "0001";

			f.p["date"] = "20120911";
			f.p["time"] = "0000";
			f.p["type"] = "fc";

			f.write_cycle(fdb, f.p);

			f.p["date"] = "20120911";
			f.p["time"] = "0000";
			f.p["type"] = "4v";

			f.write_cycle(fdb, f.p);

			f.p["date"] = "20120912";
			f.p["time"] = "0000";
			f.p["type"] = "fc";

			f.write_cycle(fdb, f.p);

			f.p["date"] = "20120912";
			f.p["time"] = "0000";
			f.p["type"] = "4v";

			f.write_cycle(fdb, f.p);
		}


		SECTION( "test_fdb_service_readtobuffer" )
		{
			fdb5::Retriever retriever;

			Buffer buffer(1024);

			Translator<size_t,std::string> str;
			std::vector<std::string>::iterator param = f.modelParams_.begin();
			for( ; param != f.modelParams_.end(); ++param )
			{
				f.p["param"] = *param;
				f.p["levtype"] = "pl";

				for( size_t step = 0; step < 2; ++step )
				{
					f.p["step"] = str(step*3);

					for( size_t level = 0; level < 3; ++level )
					{
						f.p["levelist"] = str(level*100);

						Log::info() << "Looking for: " << f.p << std::endl;

                        metkit::MarsRequest r("retrieve", f.p);
                        eckit::ScopedPtr<DataHandle> dh ( retriever.retrieve(r) );  AutoClose closer1(*dh);

						::memset(buffer, 0, buffer.size());

						dh->openForRead();
						dh->read(buffer, buffer.size());

						Log::info() << (char*) buffer << std::endl;

						std::ostringstream data;
						data << "Raining cats and dogs -- "
							<< " param " << *param
							<< " step "  << step
							<< " level " << level
							<< std::endl;

						EXPECT(::memcmp(buffer, data.str().c_str(), data.str().size()) == 0);
		//                EXPECT( std::string(buffer) == data.str() );
					}
				}
			}
		}

		SECTION( "test_fdb_service_marsreques" )
		{
			std::vector<string> steps;
			steps.push_back( "15" );
			steps.push_back( "18" );
			steps.push_back( "24" );

			std::vector<string> levels;
			levels.push_back( "100" );
			levels.push_back( "500" );

			std::vector<string> params;
			params.push_back( "130.128" );
			params.push_back( "138.128" );

			std::vector<string> dates;
			dates.push_back( "20120911" );
			dates.push_back( "20120912" );


            metkit::MarsRequest r("retrieve");

            r.setValue("class","rd");
			r.setValue("expver","0001");
			r.setValue("type","fc");
			r.setValue("stream","oper");
			r.setValue("time","0000");
			r.setValue("domain","g");
			r.setValue("levtype","pl");

            r.setValuesTyped(new metkit::TypeAny("param"), params);
            r.setValuesTyped(new metkit::TypeAny("step"), steps);
            r.setValuesTyped(new metkit::TypeAny("levelist"), levels);
            r.setValuesTyped(new metkit::TypeAny("date"), dates);

			Log::info() << r << std::endl;

			fdb5::Retriever retriever;

            eckit::ScopedPtr<DataHandle> dh ( retriever.retrieve(r) );

			PathName path ( "data_mars_request.data" );
			path.unlink();

			dh->saveInto(path);
		}
	}
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    eckit::Main::initialise(argc, argv, "FDB_HOME");
    return run_tests ( argc, argv, false );
}

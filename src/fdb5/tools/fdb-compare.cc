/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <unordered_set>
#include <iostream>
#include <fstream>
#include <cstring>
#include <ctime>
#include <limits>

#include "eckit/option/CmdArgs.h"
#include "eckit/config/Resource.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/log/JSON.h"

#include "metkit/hypercube/HyperCube.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBVisitTool.h"


#include "eccodes.h"
//#include "grib_api_internal.h"


using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

extern "C"
{
    int grib_get_message_headers(const grib_handle* h, const void** msg, size_t* size);
}
//----------------------------------------------------------------------------------------------------------------------
//Reorganise. Ideally this should be encapsulated in a class or somewhere and not just here
typedef std::tuple<std::string,long long int, long long int> GribLocation; //{Location,Offset,Length}

class FDBCompare : public FDBVisitTool {

  public: // methods

    FDBCompare(int argc, char **argv) :
        FDBVisitTool(argc, argv, "class,expver") {

        options_.push_back(new SimpleOption<std::string>("testConfig", "Path to a FDB config"));
        options_.push_back(new SimpleOption<std::string>("referenceConfig", "Path to a second FDB config"));
        options_.push_back(new SimpleOption<std::size_t>("level", "The difference can be evaluated at different levels, 1) Mars Metadata (default), 2) Mars and Grib Metadata, 3) Mars and Grib Metadata and data up to a defined tolerance (default bitexact) "));
        options_.push_back(new SimpleOption<double>("tolerance", "Floatinng point tolerance for comparison default=0. Tolerance will only be used if level=3 is set otherwise tolerance will have no effect"));
    }

  private: // methods

    virtual void execute(const CmdArgs& args);
    virtual void init(const CmdArgs &args);

    std::string testConfig_;
    std::string referenceConfig_;
    unsigned int level_;
    double tolerance_;
    bool detail_; // this parameter should be used to determine if an additional information is required why a two FDBs don't match. 
    //TODO decide if only first difference or all differences should be listed for header.
    
};


// std::string keySignature(const fdb5::Key& key) {
//     std::string signature;
//     std::string separator;
//     for (auto k : key.keys()) {
//         signature += separator+k;
//         separator=":";
//     }
//     return signature;
// }


void FDBCompare::init(const CmdArgs& args) {

    FDBVisitTool::init(args);


    ASSERT(args.has("testConfig"));
    ASSERT(args.has("referenceConfig"));
    
    testConfig_ = args.getString("testConfig");
    if(testConfig_.empty()){
        throw UserError("No path for FDB 1 specified",Here());
    }
    referenceConfig_ = args.getString("referenceConfig");
    if(referenceConfig_.empty()){
        throw UserError("No path for FDB 2 specified",Here());
    }
    level_ = args.getUnsigned("level",1);
    ASSERT((level_>=1) && (level_<=3));
    tolerance_ = args.getDouble("tolerance",0.0);
    ASSERT(tolerance_>=0);


}
// only compare the key of each map. the tupel is expected to diverge.
template <typename Map>
constexpr bool compare_keys(Map const &ref, Map const &test){
    return std::equal(ref.begin(),ref.end(),test.begin(),[] (auto a, auto b) {return a.first == b.first;});
}

void printInfo(codes_handle *h)
{
    char shortName[254] = {0,};
    size_t len = 254;
    char endSec[254] = {0,};
    size_t lenEnd = 254;
  
    codes_get_string(h,"shortName",shortName, &len);
    std::cout<<"HANDLE short Name "<<shortName<<std::endl;
 

    codes_get_string(h,"7777",endSec,&lenEnd);
    std::cout<<"HANDLE 7777 "<<endSec<<std::endl;

}

void extractGribMessage(GribLocation GribLocTuple ,codes_handle** h, char** buffer){
    const auto [location,offset,length] = GribLocTuple;
   //char* buffer = (char*)malloc(length);
    std::ifstream refFile(location,std::ios::binary);
    //get first message handle
   // std::ifstream infile()
    if(refFile.is_open())
    {
        refFile.seekg(offset,std::ios::beg);
        if(refFile.read(*buffer,length))
        {
            auto context = codes_context_get_default();
            if(!context)
            {
                throw FailedLibraryCall("eccodes","codes_context_get_default","Default context could not be created",Here());
                free(buffer);
                
            }
            
            *h= codes_handle_new_from_message(context,*buffer,length);
            if (!h)
            {
                throw FailedLibraryCall("eccodes","codes_handle_new_from_message","ECCODES Handle could not be created. Location = " + location + " offset = " + std::to_string(offset) + " length = " +std::to_string(length),Here());
                free(buffer);
                codes_context_delete(context);
                
            }
           
        }   
    }
    else{
        throw CantOpenFile("Data File can't be openeed at location="+location);
    }
    //atm we will close the file here. This is inefficient and it would be benefitial to keep the file open longer
    // to extract all fields first. 
    refFile.close();
    //free(buffer);
    // int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    // codes_dump_content(href, stdout, "wmo", dump_flags, NULL);
}
bool compare_header(codes_handle* hRef, codes_handle* hTest)
{
    size_t size1=0;
    size_t size2=0;
    const void *msg1 = NULL;
    const void *msg2 = NULL;

    if(0!=grib_get_message_headers(hRef,&msg1,&size1)){
        throw Abort("Grib Get message header failed");
    }
    if(0!=grib_get_message_headers(hTest,&msg2,&size2)){
        throw Abort("Grib get message header failed");
    }
    std::cout<<"Header Size:"<<size1<<std::endl;
    if(size1==size2 && (0==memcmp(msg1,msg2,size1))){
        return true;
    }

    //TODO check if it can be the case that they match but are not bitidentical? ASK someone
    else
    {
        //TODO check why they don't match
        return false;
    }

}

bool compare_md5sums(codes_handle* hRef, codes_handle* hTest)
{
    int err1,err2;
    int hashkeylength = 33;
    //codes_get_native_type(hRef,name,&typeRef);
    //std::cout<<typeRef<<std::endl;

    //In a first step get the gribEdition number, because we need to check different section numbers for different grib version. 
    //Ideally this could be done in eccodes then it could be done more general
    

    char md5HashValueRef[hashkeylength];//32 bits plus 0 terminator
    char md5HashValueTest[hashkeylength];
    size_t sizeHashValueRef = hashkeylength;
    size_t sizeHashValueTest = hashkeylength;
    long gribEditionRef = 0;
    long gribEditionTest = 0;

    err1 = codes_get_long( hRef,"editionNumber", &gribEditionRef);
    if(err1!=CODES_SUCCESS){
        std::cout<<"Could not extract Grib edition number"<<std::endl;
    }
    err2 = codes_get_long( hTest,"editionNumber", &gribEditionTest);
    if(err2 != CODES_SUCCESS){
        //TODO exit
    }
    if(gribEditionRef != gribEditionTest)
    {
        std::cout<<"Grib Editions don't match"<<std::endl;
        //TODO exit
    }
    if(!((gribEditionRef == 1) || (gribEditionRef == 2)))
    {
        std::cout<<"Undefined Gribedition for Reference Grib message"<<gribEditionRef<<std::endl;
        //TODO exit
    }
    if(!((gribEditionTest == 1) || (gribEditionTest == 2)))
    {
        std::cout<<"Undefined Gribedition for Test Grib message"<<gribEditionTest<<std::endl;
        //TODO exit
    }
    //Create a map with all relevant sections (TODO this was a map for development this can be a vector);
    std::map<std::string,std::string> grib_sections;
    //grib_sections["md5Headers"] = "md5Headers"; This is possible to add but is already covered in memcmp the header message (But the header message does not contain section data 7 (or 4 in Grib1) bitmap 6 (or 3 in Grib1) and section DRS (only present in Grib2))
    if(gribEditionRef == 2)
    {
        grib_sections["md5SectionData"] = "md5Section7";
        //grib_sections["md5SectionData"] = "md5DataSection";
        grib_sections["md5SectionBitmap"] = "md5Section6";
        grib_sections["md5SectionDRS"] = "md5Section5";
        //check MD5sums for Grib2 section 5,6 and 7
    }
    if(gribEditionRef ==1)
    {   
        grib_sections["md5SectionData"] = "md5Section4";
        grib_sections["md5SectionBitmap"] = "md5Section3";
        //compare MD5 Keys for Grib 1 bitmap and datasection 3 and 4
    }

    //iterate over map and extract the string hash key 
    for (const auto &[key, value]:grib_sections)
    {
        err1 = codes_get_string(hRef,value.c_str(),md5HashValueRef,&sizeHashValueRef);
        if(err1!= CODES_SUCCESS){
            //TODO exit
            std::cout<<"get_string failed hRef"<<std::endl;
        }
        err2 = codes_get_string(hTest,value.c_str(),md5HashValueTest,&sizeHashValueTest);
        if(err2 != CODES_SUCCESS){
            //TODO exit
            std::cout<<"get_string failed htest"<<std::endl;
        }
        if((sizeHashValueRef != sizeHashValueTest))
        {
            //Todo exit but probably this check is redundant
            std::cout<<"sizes don't match"<<std::endl;
        }

        if(std::strcmp(md5HashValueRef,md5HashValueTest) != 0)
        {
            std::cout<<key<<" MD5Hash don't match"<< std::endl;

        }
        
    }
}
//std::tuple<relativeError,absoluteError>
std::tuple<double,double> compare_DataSection(codes_handle* hRef, codes_handle* hTest)
{
    size_t valuesLenRef = 0;
    size_t valuesLenTest = 0;
    double maxError = 0.;
    double absoluteError=0.;
    double relativeError=0.;
    double *valuesRef;
    double *valuesTest;
    CODES_CHECK(codes_get_size(hRef,"values",&valuesLenRef),0);
    CODES_CHECK(codes_get_size(hTest,"values",&valuesLenTest),0);
    if(valuesLenRef != valuesLenTest)
    {
        std::cout<<"cannot compare values as number of entries doesn't match"<<std::endl;
        //todo exit
    }
    valuesRef = (double*)malloc(valuesLenRef*sizeof(double));
    if(!valuesRef)
    {
        std::cout<<"malloc failed"<<std::endl;
    }
    valuesTest = (double*)malloc(valuesLenTest*sizeof(double));
    if(!valuesTest)
    {
        std::cout<<"malloc failed"<<std::endl;
        free(valuesRef);
    }
    //const char dataSectionOffset[] = "offsetSection7";
    //const char dataSectionLength[] = "section7Length";
    //int typeRef;
    //codes_get_native_type(hRef,"values",&typeRef);
    //std::cout<<typeRef<<std::endl;//GRIB_TYPE_DOUBLE 2


    CODES_CHECK(codes_get_double_array(hTest,"values",valuesTest,&valuesLenTest),0);
    CODES_CHECK(codes_get_double_array(hRef,"values",valuesRef,&valuesLenRef),0);


    //TODO Do this before extracting the handle
    // if(memcmp(valuesRef,valuesTest,valuesLenRef*sizeof(double)))
    // {
    //     free(valuesTest);
    //     free(valuesRef);
    //     //set absolute and relative error to zero
    //     return std::tuple<
    // }
    double error = 0.0;
    const double epsilon = std::numeric_limits<double>::epsilon();
    double denominator;
    for(size_t i = 0; i<valuesLenRef;++i){
        error = std::fabs(valuesRef[i]-valuesTest[i]);
        absoluteError += error;
        denominator = std::fabs(valuesRef[i]) > epsilon ? std::fabs(valuesRef[i]) : epsilon;
        relativeError = error/denominator;
    }
    //TEST VALUES todo


    free(valuesTest);
    free(valuesRef);
    return {relativeError,absoluteError};
}


std::tuple<double,double> gribCompare(GribLocation gribLocRef, GribLocation gribLocTest)
{
    time_t start,stop;
    codes_handle* hRef;
    codes_handle* hTest;
    char* bufferRef = (char*)malloc(std::get<2>(gribLocRef));
    char* bufferTest = (char*)malloc(std::get<2>(gribLocTest));
    extractGribMessage(gribLocRef,&hRef,&bufferRef);
    if(hRef==NULL)
    {
        throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocRef)) + " , " + std::to_string(std::get<2>(gribLocRef)));
    }
    extractGribMessage(gribLocTest,&hTest,&bufferTest);
    if(hTest==NULL)
    {
        throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocTest) + " , " + std::to_string(std::get<1>(gribLocTest)) + " , " + std::to_string(std::get<2>(gribLocTest)));
    }
    // int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    // codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
    printInfo(hRef);
    printInfo(hTest);
    // std::cout<<check_message_complete(hRef)<<std::endl;
    // std::cout<<check_message_complete(hTest)<<std::endl;

    // time(&start);
    // auto errch = compare_header(hRef,hTest);
    // time(&stop);
    // std::cout<<difftime(stop,start)<<std::endl;

    if(!compare_header(hRef,hTest)){
        std::cout<<"Headers don't match"<<std::endl;
        throw Abort("Headers don't match Location 1 = " + std::get<0>(gribLocRef) + " offset 1 = " + std::to_string(std::get<1>(gribLocRef)) + " length 1 = " +std::to_string(std::get<2>(gribLocRef))+" Location 2 = " + std::get<0>(gribLocTest) + " offset 2 = " + std::to_string(std::get<1>(gribLocTest)) + " length 2 = " +std::to_string(std::get<2>(gribLocTest)));
    }


    // //// Key interator investigation
    // std::cout<<"this should only be done once"<<std::endl;
    // auto kiter =codes_keys_iterator_new(hRef,CODES_KEYS_ITERATOR_ALL_KEYS,NULL);
    // if(!kiter){
    //     std::cout<<"TODO errormessage"<<std::endl;
    // }
    // int type;
    // while(codes_keys_iterator_next(kiter))
    // {
    //     //char value[1024];//make it better
    //     //size_t vlen=1024;
    //     auto name = codes_keys_iterator_get_name(kiter);
    //     codes_get_native_type(hRef,name,&type);
    //     std::cout<<name<<" "<< type<<std::endl;
    // }

    // // //// end key iterator investigation
    // time(&start);
    
    
    
    auto err = compare_md5sums(hRef,hTest);
    
    
    //TODO check err


    // time(&stop);
    // std::cout<<difftime(stop,start)<<std::endl;
    //auto err = compare_md5sums(hRef, hTest);
    const auto [relativeError,absoluteError] = compare_DataSection(hRef,hTest);
    //std::cout<< relativeError<<" "<<absoluteError<<std::endl;
    double tolerance = std::numeric_limits<double>::epsilon();

    if( relativeError>tolerance || absoluteError>tolerance)
    {
        std::cout<<"Differenene in Grib message Grib Message failed {Location,offset,length} = " << std::get<0>(gribLocTest) << " , " << std::to_string(std::get<1>(gribLocTest)) << " , " << std::to_string(std::get<2>(gribLocTest))<<std::endl;
        std::cout<<"Difference in Grib message: Absolute Error = "<<absoluteError<<" relative Error = "<<relativeError<<std::endl;
    }
    codes_handle_delete(hRef);
    codes_handle_delete(hTest);
    free(bufferRef);
    free(bufferTest);
    return {relativeError,absoluteError};
    //todo check err
    //TODO free resources
}

void FDBCompare::execute(const CmdArgs& args) {

    // std::cout<<"FDB test object"<<std::endl;
    PathName configPathref(referenceConfig_);
    if (!configPathref.exists()) {
        std::ostringstream ss;
        ss << "Path " << referenceConfig_ << " does not exist";
        throw UserError(ss.str(), Here());
    }
    if (configPathref.isDir()) {
        std::ostringstream ss;
        ss << "Path " << referenceConfig_ << " is a directory. Expecting a file";
        throw UserError(ss.str(), Here());
    }
    
    FDB fdbref( Config::make(configPathref));

    PathName configPathtest(testConfig_);
    if (!configPathtest.exists()) {
        std::ostringstream ss;
        ss << "Path " << testConfig_ << " does not exist";
        throw UserError(ss.str(), Here());
    }
    if (configPathtest.isDir()) {
        std::ostringstream ss;
        ss << "Path " << testConfig_ << " is a directory. Expecting a file";
        throw UserError(ss.str(), Here());
    }
   
    FDB fdbtest( Config::make(configPathtest));



    for (const FDBToolRequest& request : requests()) {
        //potentially sub std::vector<Key> with metkit::mars::MarsRequest or with string
        std::unordered_map<std::string, std::tuple<std::string,long long int,long long int>> ref_map;
        std::unordered_map<std::string, std::tuple<std::string,long long int,long long int>> test_map;

        auto listObject = fdbref.list(request);
        ListElement elem;    
        while (listObject.next(elem)) {
            std::string str;
            for(const auto & bit : elem.key()) {
                str.append(bit);
                str.append(",");
            }
            ref_map.insert({str,{elem.location().uri().path(),static_cast<long long int>(elem.location().offset()),static_cast<long long int>(elem.location().length())}});
            // std::cout<<elem.location().uri().path()<<std::endl;
            // std::cout<<elem.location().offset()<<std::endl;
            // std::cout<<elem.location().length()<<std::endl;
        }
        listObject = fdbtest.list(request);
        while (listObject.next(elem)) {
            std::string str;
            for(const auto & bit : elem.key()) {
                str.append(bit);
                str.append(",");
                //to corrupt and test: 
                //str.append("...");
            }
            test_map.insert({str,{elem.location().uri().path(),static_cast<long long int>(elem.location().offset()),static_cast<long long int>(elem.location().length())}});
            long long zero = 0;
        
            //test_map.insert({str,{elem.location().uri(),elem.location().offset(),1.0}}); //corrupted map for tests

        }
        //First compare that the number of entries matches. 
        // -> potentially just an ASSERT is sufficient
        //tested separately to avoid having this test done in each key compare, otherwise it could be added to the comparison function return (size_compare && key_compare)
        if(test_map.size()!=ref_map.size()){
            std::cout<<"Number of messages in Databases don't match"<<std::endl;
            std::cout<<"Test FDB #entries in Toc: "<<test_map.size()<<" Reference FDB #entries in Toc: " << ref_map.size()<<std::endl;
            return;
        }
      //  std::cout<<test_map.key_eq()<<std::endl;
        //Chech that the keys match only continue with next comparison is this is the case
        if(!(compare_keys(ref_map,test_map))){
            std::cout<<"Mars Metadata Keys don't match"<<std::endl;
            return;
        }

        
        //      GRIB_CHECK_NOLINE(grib_get_message_headers(h1, &msg1, &size1), 0);
        // GRIB_CHECK_NOLINE(grib_get_message_headers(h2, &msg2, &size2), 0);
        // if ( size1 == size2 && (0 == memcmp(msg1, msg2, size1)) ) {
        //     return 0;
        // }
        int counter = 0;
        double tolerance = std::numeric_limits<double>::epsilon();

        int ref_size = ref_map.size();
        double absoluteErrorSum = 0.0;
        double absoluteErrorMin = 0.0;
        double absoluteErrorMax = 0.0;
        size_t countValidErrorAbs = 0;
    
        double relativeErrorSum = 0.0;
        double relativeErrorMax = 0.0;
        double relativeErrorMin = 0.0;
        size_t countValidErrorRelative = 0;
        for (auto& [key, value]:ref_map)
        {
            break;
            counter++;
            //wrap in exception block try catch
            const auto [absoluteError, relativeError] = gribCompare(value,test_map[key]);

            if(absoluteError > tolerance)
            {
                if(countValidErrorAbs == 0){
                    absoluteErrorMin = absoluteError;
                    absoluteErrorMax = absoluteError;
                }
                else{
                    if(absoluteError<absoluteErrorMin) absoluteErrorMin = absoluteError;
                    if(absoluteError>absoluteErrorMax) absoluteErrorMax = absoluteError;
                }
                absoluteErrorSum += absoluteError;
                ++countValidErrorAbs;
            }
            if(relativeError > tolerance)
            {
                if(countValidErrorRelative == 0){
                    relativeErrorMin = relativeError;
                    relativeErrorMax = relativeError;
                }
                else{
                    if(relativeError<relativeErrorMin) relativeErrorMin = relativeError;
                    if(relativeError>relativeErrorMax) relativeErrorMax = relativeError;
                }
                relativeErrorSum += relativeError;
                ++countValidErrorRelative;
            }
            
            //gribCompare("/lus/h2resw01/hpcperm/ecm4053/IFS/reference-fdb/climateDT/out/tco79l137/hz9m/hres/intel.hpcx/blq.intel.sp/h48.N4.T112xt4xh1+ioT8xt4xh0.nextgems_6h.i16r0w16.eORCA1_Z75.htco79-35661861/fdb/NATIVE_grids/root/d1:climate-dt:CMIP6:hist:1:IFS-NEMO:1:hz9m:clte:20200120/high:fc:sfc.20240611.145957.ac3-2065.bullx.384021615869965.data",138278,80599,"/lus/h2resw01/hpcperm/ecm4053/IFS/reference-fdb/climateDT/out/tco79l137/hz9m/hres/intel.hpcx/blq.intel.sp/h48.N4.T112xt4xh1+ioT8xt4xh0.nextgems_6h.i16r0w16.eORCA1_Z75.htco79-35661861/fdb/NATIVE_grids/root/d1:climate-dt:CMIP6:hist:1:IFS-NEMO:1:hz9m:clte:20200120/high:fc:sfc.20240611.145957.ac3-2065.bullx.384021615869965.data",138278,80599);
            
        }
        double relativeErrorAvg = (countValidErrorRelative > 0) ? (relativeErrorSum/countValidErrorRelative) : 0.0;
        double absoluteErrorAvg = (countValidErrorAbs > 0) ? (absoluteErrorSum/countValidErrorAbs) : 0.0;
        //gribCompare({"/lus/h2resw01/hpcperm/ecm4053/IFS/reference-fdb/climateDT/out/tco79l137/hz9m/hres/intel.hpcx/blq.intel.sp/h48.N4.T112xt4xh1+ioT8xt4xh0.nextgems_6h.i16r0w16.eORCA1_Z75.htco79-35661861/fdb/NATIVE_grids/root/d1:climate-dt:CMIP6:hist:1:IFS-NEMO:1:hz9m:clte:20200120/high:fc:sfc.20240611.145957.ac3-2065.bullx.384021615869965.data",138278,80599},{"/lus/h2resw01/hpcperm/ecm4053/IFS/reference-fdb/climateDT/out/tco79l137/hz9m/hres/intel.hpcx/blq.intel.sp/h48.N4.T112xt4xh1+ioT8xt4xh0.nextgems_6h.i16r0w16.eORCA1_Z75.htco79-35661861/fdb/NATIVE_grids/root/d1:climate-dt:CMIP6:hist:1:IFS-NEMO:1:hz9m:clte:20200120/high:fc:sfc.20240611.145957.ac3-2065.bullx.384021615869965.data",138278,80599});
        gribCompare({"/home/ecm4053/tmp/test.grib",0,80599},{"/home/ecm4053/tmp/test.grib",0,80599});
        //const auto [absoluteError, relativeError] = gribCompare({"/home/ecm4053/tmp/test.grib",0,80599},{"/home/ecm4053/tmp/test.grib",0,80599});

        std::cout<<"****************** SUMMARY **********************"<<std::endl;
        std::cout<< "Relative Error: abs((refvalue-testvalue)/refvalue)" <<std::endl;
        std::cout<< "    Minimum Error: "<<relativeErrorMin<<std::endl;
        std::cout<<"     Maximum Error: "<<relativeErrorMax<<std::endl;
        std::cout<<"     Average Error: "<<relativeErrorAvg<<std::endl;
        std::cout<< "Absolute Error: abs(refvalue-testvalue)" <<std::endl;
        std::cout<<"     Maximum Error: "<<absoluteErrorMax<<std::endl;
        std::cout<<"     Average Error: "<<absoluteErrorAvg<<std::endl;
        std::cout<< "    Minimum Error: "<<absoluteErrorMin<<std::endl;







        
        // // 
        // for(auto i = ref_map.begin(); i != ref_map.end(); i++ )
        //     std::cout<<i->first<<std::endl;
        // for(auto i = test_map.begin(); i != test_map.end(); i++ )
        //     std::cout<<i->first<<std::endl;
           
    }
    // std::unique_ptr<JSON> json;
    // if (json_) {
    //     json.reset(new JSON(Log::info()));
    //     json->startList();
    // }

    // for (const FDBToolRequest& request : requests()) {
 
    //     if (!porcelain_) {
    //         Log::info() << "Listing for request" << std::endl;
    //         request.print(Log::info());
    //         Log::info() << std::endl;
    //     }

    //     // If --full is supplied, then include all entries including duplicates.
    //     auto listObject = fdb.list(request, !full_ && !compact_);
    //     std::map<std::string, std::map<std::string, std::pair<metkit::mars::MarsRequest, std::unordered_set<Key>>>> requests;

    //     ListElement elem;
    //     while (listObject.next(elem)) {

    //         if (compact_) {
    //             std::vector<Key> keys = elem.key();
    //             ASSERT(keys.size() == 3);

    //             std::string treeAxes = keys[0];
    //             treeAxes += ",";
    //             treeAxes += keys[1];

    //             std::string signature=keySignature(keys[2]);  // i.e. step:levelist:param

    //             auto it = requests.find(treeAxes);
    //             if (it == requests.end()) {
    //                 std::map<std::string, std::pair<metkit::mars::MarsRequest, std::unordered_set<Key>>> leaves;
    //                 leaves.emplace(signature, std::make_pair(keys[2].request(), std::unordered_set<Key>{keys[2]}));
    //                 requests.emplace(treeAxes, leaves);
    //             } else {
    //                 auto h = it->second.find(signature);
    //                 if (h != it->second.end()) { // the hypercube request is already there... adding the 3rd level key
    //                     h->second.first.merge(keys[2].request());
    //                     h->second.second.insert(keys[2]);
    //                 } else {
    //                     it->second.emplace(signature, std::make_pair(keys[2].request(), std::unordered_set<Key>{keys[2]}));
    //                 }
    //             }
    //         } else {
    //             if (json_) {
    //                 (*json) << elem;
    //             } else {
    //                 elem.print(Log::info(), location_, !porcelain_);
    //                 Log::info() << std::endl;
    //             }
    //         }
    //     }
    //     if (compact_) {
    //         for (const auto& tree: requests) {
    //             for (const auto& leaf: tree.second) {
    //                 metkit::hypercube::HyperCube h{leaf.second.first};
    //                 if (h.size() == leaf.second.second.size()) {
    //                     Log::info() << "retrieve," << tree.first << ",";
    //                     leaf.second.first.dump(Log::info(), "", "", false);
    //                     Log::info() << std::endl;
    //                 } else {
    //                     for (const auto& k: leaf.second.second) {
    //                         h.clear(k.request());
    //                     }
    //                     for (const auto& r: h.requests()) {
    //                         Log::info() << "retrieve," << tree.first << ",";
    //                         r.dump(Log::info(), "", "", false);
    //                         Log::info() << std::endl;
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //     // n.b. finding no data is not an error for fdb-list
    // }

    // if (json_) {
    //     json->endList();
    // }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBCompare app(argc, argv);
    return app.start();
}



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
#include <bitset>

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
    //return std::equal(ref.begin(),ref.end(),test.begin(),[] (auto a, auto b) {return a.first == b.first;});
    if(test.size() != ref.size())
    {
        std::cout<<"The number of messages within the FDBs doesn't match. "<<std::endl;
    }
    int count = 0;
    std::cout<<"******* Checking that all Mars key entries from Test match entries in the reference FDB ****"<<std::endl;
    for(const auto& pair: test){
        if(ref.find(pair.first) == ref.end()){
            ++count;
            std::cout<<"TEST FDB message key not found in ref map: " <<pair.first<<std::endl;
        }
    }
    if(count==0 && (test.size()==ref.size()))
    {   
        std::cout<<"Mars Keys match"<<std::endl;
        return true; //works because it is assumed that both 
    }
    else{
        std::cout<<"******* There was a mismatch in keys in the forward search: We are now searching the reference FDB to list all Keys that don't match entries in the test FDB ****"<<std::endl;
        for(const auto& pair: ref){
            if(test.find(pair.first) == test.end()){
                ++count;
                std::cout<<"REF FDB message key not found in test map: " <<pair.first<<std::endl;
            }
        }
        std::cout<<"******* End of Mars Key Message comparison ********"<<std::endl;
    }
    return false;
}

void printInfo(codes_handle* h)
{
    char shortName[254] = {0,};
    size_t len = 254;
    char endSec[254] = {0,};
    size_t lenEnd = 254;
  
    CODES_CHECK(codes_get_string(h,"shortName",shortName, &len),0);
    std::cout<<"HANDLE short Name "<<shortName<<std::endl;
 

    CODES_CHECK(codes_get_string(h,"7777",endSec,&lenEnd),0);
    std::cout<<"HANDLE 7777 "<<endSec<<std::endl;

}
// Function to check endianness
bool isLittleEndian() {
    uint16_t test = 0x1;
    return *((uint8_t*)&test) == 0x1;
}

// Function to convert from little-endian to the host endianness if necessary
//Grib seems to be written in big endian format -> if I'm on a little endian machine I need to convert my input
uint64_t fromLittleEndian(uint64_t value) {
    if (!isLittleEndian()) {
        return value;  // No conversion needed
    } else {
        // Convert to big-endian 
        uint64_t result = 0;
        for (int i = 0; i < 8; ++i) {
            result |= ((value >> (i * 8)) & 0xFF) << ((7 - i) * 8);
        }
        return result;
    }
}
template <typename T>
void printBits(T value) {
    // Size of type T in bytes
    constexpr size_t size = sizeof(T);

    // Loop through each bit in reverse order for clarity
    for (int i = size * 8 - 1; i >= 0; --i) {
        std::cout << ((value >> i) & 1);
        if (i % 8 == 0) std::cout << " ";  // Space between bytes for clarity
    }
    std::cout << std::endl;
}

template <typename T>
T endianTest(const T& value) {
    if (!isLittleEndian()) {
        return value;  // No conversion needed
    } else {
        // Convert to big-endian
        int size = sizeof(value);
        T result = 0;
        for (int i = 0; i < size; ++i) {
            result |= ((value >> (i * 8)) & 0xFF) << ((size - 1 - i) * 8);
        }
        return result;
    }
}
// Function to read 3-byte section length (for edition 2)
uint32_t readValue3Byte(const char* buffer, std::size_t offset){
    uint32_t result = (static_cast<unsigned char>(buffer[offset + 2]) << 16) |
                      (static_cast<unsigned char>(buffer[offset + 1]) << 8) |
                      static_cast<unsigned char>(buffer[offset ]);
    return endianTest(result);
}

// Template function to read a value from a buffer using memcpy
template <typename T>
T readValue(const char* buffer, std::size_t offset) {
    static_assert(std::is_integral<T>::value, "Integral type required.");
    T value;
    //std::cout<<"offset "<<offset<<"sizeof(T) " << sizeof(T)<<std::endl;
    std::memcpy(&value, buffer + offset, sizeof(T));
   // printBits(value);
   // std::cout<<"value "<<value<<std::endl;
    return endianTest(value);
}
bool memComparison(const char* bufferRef, const char* bufferTest,std::size_t offset,uint32_t length){
    return std::memcmp(bufferRef+offset,bufferTest+offset,length) == 0;
}
/*bit_comparison 
* Arguments 
* const char* bufferRef - Grib message in bytes for the reference Grib message
* const char* bufferTest - Grib message in bytes for the test Grib message
* size_t length - sizeof(Buffer)
* Bit comparison without using eccodes 
* Return: Returns 10 if a bitexact comparison is true or the Integer number in which the comparison failed. If it fails in the data section a 9 is returned to distinguish 
* and allow further tests based on a tolerance value in the data section.
* Because 
*/
int bit_comparison(const char* bufferRef, const char * bufferTest,size_t length){

    //std::cout<<"BIT comparison"<<std::endl;
    std::size_t offset = 0;
    if(!bufferRef || !bufferTest){
        throw BadValue("Buffer corrupted");
    }
    ASSERT(length>16); // Needs to be true because those bytes are read without further checking.
    if( (static_cast<char>(bufferRef[0]) != 'G')||(static_cast<char>(bufferTest[0]) != 'G') ||
        (static_cast<char>(bufferRef[1]) != 'R')||(static_cast<char>(bufferTest[1]) != 'R') ||
        (static_cast<char>(bufferRef[2]) != 'I')||(static_cast<char>(bufferTest[2]) != 'I') ||
        (static_cast<char>(bufferRef[3]) != 'B')||(static_cast<char>(bufferTest[3]) != 'B') )
    {
        std::cout<<"Message is not a Grib message: Comparing data formats other than Grib messages is currently not implemented"<<std::endl;
        return 0;
    }

    //EditionNumber will is always the 8th Byte in a Grib message
    int editionnumberRef = (int)bufferRef[7];
    int editionnumberTest = (int)bufferRef[7];

    if(editionnumberRef != editionnumberTest){
        std::cout<<"Editionnumbers don't match -> cannot compare Grib message"<<std::endl;
        return 0;
    }
    if(editionnumberRef == 2){
        //Total Length Value starts at position 8 in the Grid2 header
        uint64_t totalLength = readValue<uint64_t>(bufferRef,8);
        if(length != totalLength){
            std::cout<<" Message length in Mars Key location and total message length specified in Grib message don't match"<<std::endl;
            return 0;
        }
        //GRIB2 Section 0 is always 16 Bytes long:
        offset += 16;
    }
    else if(editionnumberRef == 1){
        uint32_t totalLength = readValue3Byte(bufferRef,4);
        if(length != totalLength){
            std::cout<<" Message length in Mars Key location and total message length specified in Grib message don't match"<<std::endl;
            return 0;
        }
        //Grib1 Section 0 is always 8 Bytes long:
        offset += 8;
    }else{
        std::cout<<"Unknown Grib edition number "<< editionnumberRef<<std::endl;
        return 0;
    }
    // Determine number of sections based on the edition number 1 or 2
    int numSections = (editionnumberRef ==1) ? 4:7;
    
    // Process sections based on edition
    for (int i = 0; i<numSections;++i){
       // std::cout<<"offset " << offset<<std::endl;
        // Read section length
        uint32_t sectionLengthRef = 0;
        uint32_t sectionLengthTest = 0;
        if (editionnumberRef == 1){
            // Grib 1 Read 3 byte section length at the beginning of each section
            if (offset + 3 > length){
                std::cerr <<"Unexpected end of buffer while reading section length for section "<< i+1<<std::endl;
                return ((i+1)==numSections) ? 9:i+1;
            }
            sectionLengthRef = readValue3Byte(bufferRef,offset);
            sectionLengthTest = readValue3Byte(bufferTest,offset);
        }
        else {
            //Grib 2 stores a 4 byte section length value at the beginning of each section
            if (offset + 4 > length){
                std::cerr <<"Unexpected end of buffer while reading section length for section "<< i+1<<std::endl;
                return ((i+1)==numSections) ? 9:i+1;
            }
            sectionLengthRef = readValue<uint32_t>(bufferRef,offset);
            sectionLengthTest = readValue<uint32_t>(bufferTest,offset);
        }
        if(sectionLengthRef != sectionLengthTest){
            std::cout<<"The section "<<i+1<<" length parameter differs between the grib messages "<<std::endl;
            return ((i+1)==numSections) ? 9:i+1;
        }
        //Memory comparison for each section 
        if(! memComparison(bufferRef, bufferTest,offset,sectionLengthRef)){
            std::cout<<"Memorycomparison failed in section "<<i+1<<std::endl;
            return ((i+1)==numSections) ? 9:i+1;
        }
        offset += sectionLengthRef;
    }
    //Test that the final bit of the message 7777 is present in both
    while(offset<length){
        if((static_cast<char>(bufferRef[offset]) != '7')||(static_cast<char>(bufferTest[offset]) != '7')){
            std::cout<<"Message does not contain the correct section"<<std::endl;
        }
        ++offset;      
    }


    return 10;
}
//void extractGribMessage(GribLocation GribLocTuple ,std::unique_ptr<codes_handle,decltype(&codes_handle_delete)>& h, std::unique_ptr<char[]>& buffer,std::unique_ptr<codes_context,decltype(&codes_context_delete)>& context){
void extractGribMessage(GribLocation GribLocTuple ,char* buffer){

    const auto [location,offset,length] = GribLocTuple;
    std::ifstream refFile(location,std::ios::binary);
    //get first message handle
    if(refFile.is_open())
    {
        refFile.seekg(offset,std::ios::beg);
        if(!refFile.read(buffer,length)) {
            refFile.close();
            throw Abort("Could not read the data from file",Here());
        }
        

            // *h = codes_handle_new_from_message(NULL,buffer,length);
            // if (!h)
            // {
            //     refFile.close();

            //     throw FailedLibraryCall("eccodes","codes_handle_new_from_message","ECCODES Handle could not be created. Location = " + location + " offset = " + std::to_string(offset) + " length = " +std::to_string(length),Here());   
            // }
    }
    else{
        throw CantOpenFile("Data File can't be openeed at location="+location);
    }
    //atm we will close the file here. This is inefficient and it would be benefitial to keep the file open longer
    // to extract all fields first. 
    refFile.close();

}

bool compare_header(codes_handle * hRef, codes_handle * hTest)
{
    size_t size1=0;
    size_t size2=0;
    const void *msg1 = NULL; //potential source of mem leak
    const void *msg2 = NULL;

    if(!hRef || !hTest){
        throw BadValue("codes_handle are corrupted");
    }
    if(0!=grib_get_message_headers(hRef,&msg1,&size1)){
        throw FailedLibraryCall("internal grib api","grib_get_message_header","Potentially this is no longer supported as it was not the eccodes API",Here());
    
    }
    if(0!=grib_get_message_headers(hTest,&msg2,&size2)){
        throw FailedLibraryCall("internal grib api","grib_get_message_header","Potentially this is no longer supported as it was not the eccodes API",Here());
    }
    if(size1==size2 && (0==memcmp(msg1,msg2,size1))){
        return true;
    }
    std::cout<<"********************* Reference Grib Message Header **********************************"<<std::endl;
    int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
    std::cout<<"********************* Test Grib Message Header **********************************"<<std::endl;
    dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    codes_dump_content(hTest, stdout, "wmo", dump_flags, NULL);
    std::cout<<"********************** END DEBUG SUMMARY *****************************************"<<std::endl;
    return false;

}

bool compare_md5sums(codes_handle* hRef, codes_handle *hTest)
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

    CODES_CHECK(codes_get_long( hRef,"editionNumber", &gribEditionRef),0);

    CODES_CHECK(codes_get_long( hTest,"editionNumber", &gribEditionTest),0);
    
    if(gribEditionRef != gribEditionTest)
    {
        throw BadValue("The Grib edition numbers don't match Reference = Grib "+std::to_string(gribEditionRef)+" Test = "+std::to_string(gribEditionTest),Here());

    }
    if(!((gribEditionRef == 1) || (gribEditionRef == 2)))
    {
        throw BadValue("Currently only Grib 1 and Grib 2 are supported. The Gribnumber of these messages is: "+gribEditionRef,Here());
    }

    //Create a map with all relevant sections (TODO this was a map for development this can be a vector);
    std::map<std::string,std::string> grib_sections;
    //"md5Headers"; This is possible to add but is already covered in memcmp the header message (But the header message does not contain section data 7 (or 4 in Grib1) bitmap 6 (or 3 in Grib1) and section DRS (only present in Grib2))
    if(gribEditionRef == 2)
    {
        //check MD5sums for Grib2 section 5,6 and 7
        grib_sections["md5SectionData"] = "md5Section7";
        grib_sections["md5SectionBitmap"] = "md5Section6";
        grib_sections["md5SectionDRS"] = "md5Section5";
    }
    if(gribEditionRef ==1)
    {   
        //compare MD5 Keys for Grib 1 bitmap and datasection 3 and 4
        grib_sections["md5SectionData"] = "md5Section4";
        grib_sections["md5SectionBitmap"] = "md5Section3";
    }

    //iterate over map and extract the string hash key 
    for (const auto &[key, value]:grib_sections)
    {
        err1 = codes_get_string(hRef,value.c_str(),md5HashValueRef,&sizeHashValueRef);
        if(err1!= CODES_SUCCESS){
            throw FailedLibraryCall("eccodes","codes_get_string","could not retrieve hash key :"+value,Here()); 
        }
        err2 = codes_get_string(hTest,value.c_str(),md5HashValueTest,&sizeHashValueTest);
        if(err2 != CODES_SUCCESS){
            
            throw FailedLibraryCall("eccodes","codes_get_string","could not retrieve hash key :"+value,Here()); 
        }
        if((sizeHashValueRef != sizeHashValueTest))
        {
            std::string errorMsg = std::string("Hashkeys need to have the same size Ref Hash ")+md5HashValueTest+" Test "+md5HashValueTest + "Section "+value;
            throw BadValue(errorMsg,Here());
        }

        if(std::strcmp(md5HashValueRef,md5HashValueTest) != 0)
        {
            std::cout<<"Hash keys don't match for section "<<value<<std::endl;
            std::cout<<"********************* Reference Grib Message Header **********************************"<<std::endl;
            int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
            codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
            std::cout<<"********************* Test Grib Message Header **********************************"<<std::endl;
            dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
            codes_dump_content(hTest, stdout, "wmo", dump_flags, NULL);
            std::cout<<"********************** END DEBUG SUMMARY *****************************************"<<std::endl;
            return false;
        }
        
    }
    return true;
}
//std::tuple<relativeError,absoluteError>
void compare_DataSection(codes_handle* hRef, codes_handle* hTest,double relativeError, double absoluteError)
{
    size_t valuesLenRef = 0;
    size_t valuesLenTest = 0;
    double maxError = 0.;
    const double epsilon = std::numeric_limits<double>::epsilon();

    //make sure that this is set to zero 
    ASSERT(relativeError<=epsilon);
    ASSERT(absoluteError<=epsilon);
    double *valuesRef;
    double *valuesTest;
    CODES_CHECK(codes_get_size(hRef,"values",&valuesLenRef),0);
    CODES_CHECK(codes_get_size(hTest,"values",&valuesLenTest),0);
    if(valuesLenRef != valuesLenTest)
    {
        std::string errorMsg = std::string("Number of data entries does not match Reference number")+std::to_string(valuesLenRef)+" test Grib number "+std::to_string(valuesLenTest);
        throw BadValue(errorMsg,Here());
    }
    valuesRef = (double*)malloc(valuesLenRef*sizeof(double));
    if(!valuesRef)
    {
        throw std::bad_alloc();
    }
    valuesTest = (double*)malloc(valuesLenTest*sizeof(double));
    if(!valuesTest)
    {
        free(valuesRef);
        throw std::bad_alloc();
    }

    CODES_CHECK(codes_get_double_array(hTest,"values",valuesTest,&valuesLenTest),0);
    CODES_CHECK(codes_get_double_array(hRef,"values",valuesRef,&valuesLenRef),0);

    double error = 0.0;
    double denominator;
    for(size_t i = 0; i<valuesLenRef;++i){
        error = std::fabs(valuesRef[i]-valuesTest[i]);
        absoluteError += error;
        denominator = std::fabs(valuesRef[i]) > epsilon ? std::fabs(valuesRef[i]) : epsilon;
        relativeError = error/denominator;
    }


    free(valuesTest);
    free(valuesRef);
    return;
}


bool gribCompare(const GribLocation& gribLocRef,const GribLocation& gribLocTest, double relativeError, double absoluteError)
{
    time_t start,stop;
    codes_handle* hRef;
    codes_handle* hTest;
    ASSERT(relativeError==0.0);
    ASSERT(absoluteError==0.0);

    bool datamatch = true;
    char* bufferRef = (char*)malloc(std::get<2>(gribLocRef)*sizeof(char));
    if(!bufferRef)
    {
        std::cerr<<"Here()"<<std::endl;
        throw std::bad_alloc();
    }
    char* bufferTest = (char*)malloc(std::get<2>(gribLocTest)*sizeof(char));
    if(!bufferTest)
    {
        std::cerr<<Here()<<std::endl;
        throw std::bad_alloc();
    }
    try{
        extractGribMessage(gribLocRef,bufferRef);
        if(bufferRef==NULL)
        {
            throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocRef)) + " , " + std::to_string(std::get<2>(gribLocRef)));
        }
        extractGribMessage(gribLocTest,bufferTest);
        if(bufferTest==NULL)
        {
            throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocTest) + " , " + std::to_string(std::get<1>(gribLocTest)) + " , " + std::to_string(std::get<2>(gribLocTest)));
        }
        int i =  bit_comparison(bufferRef, bufferTest,std::get<2>(gribLocTest));
        if(i==10){
            free(bufferRef);
            free(bufferTest);
            return true;
        }
        if(i<9 && i>=0){
            std::cout<<"The Header Grib Header comparison failed  {Location,offset,length} = " << std::get<0>(gribLocTest) << " , " << std::to_string(std::get<1>(gribLocTest)) << " , " + std::to_string(std::get<2>(gribLocTest)) <<std::endl;
            free(bufferRef);
            free(bufferTest);
            return false;
        }
        if(i<0 && i>9){
            std::string errorMsg = std::string(" Unknown return value");
            free(bufferRef);
            free(bufferTest);
            throw BadValue(errorMsg,Here());
        }
        ASSERT(i==9); // Numerical check of the values stored in the data section
        hRef = codes_handle_new_from_message(NULL,bufferRef,std::get<2>(gribLocRef));
        if (!hRef)
        {
            throw FailedLibraryCall("eccodes","codes_handle_new_from_message","ECCODES Handle could not be created. Location, offset, length = " + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocRef)) + " , " + std::to_string(std::get<2>(gribLocRef)),Here());   
        }
        hTest = codes_handle_new_from_message(NULL,bufferTest,std::get<2>(gribLocTest));
        if (!hTest)
        {
            throw FailedLibraryCall("eccodes","codes_handle_new_from_message","ECCODES Handle could not be created. Location, offset, length = "  + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocTest)) + " , " + std::to_string(std::get<2>(gribLocTest)),Here());   
        }

        // // BitExactComparison with ECCODES begin
        // //TODO DECIDE HOW THIS SECTION SHOULD STILL STAY IN HERE OR IF JUST BYTEEXACT HEADERS IS OKAY
        // auto match = compare_header(hRef,hTest);
        // if(!match){
        //     free(bufferRef);
        //     free(bufferTest);
        
        //     codes_handle_delete(hRef);
        //     codes_handle_delete(hTest);
        //     throw Abort("Headers don't match Location 1 = " + std::get<0>(gribLocRef) + " offset 1 = " + std::to_string(std::get<1>(gribLocRef)) + " length 1 = " +std::to_string(std::get<2>(gribLocRef))+" Location 2 = " + std::get<0>(gribLocTest) + " offset 2 = " + std::to_string(std::get<1>(gribLocTest)) + " length 2 = " +std::to_string(std::get<2>(gribLocTest)));
        // }

        // auto match_md5sums = compare_md5sums(hRef,hTest);
        // if(match_md5sums){
        //     free(bufferRef);
        //     free(bufferTest);
        
        //     codes_handle_delete(hRef);
        //     codes_handle_delete(hTest);

        //     absoluteError = 0.0;
        //     relativeError = 0.0;
        //     return true;
        // }
        // ///std::cerr<<"should not reach"<<std::endl;
        // //BITExact Comparison with ECCODES END

   
        compare_DataSection(hRef,hTest,relativeError,absoluteError);

    // time(&stop);
    // std::cout<<difftime(stop,start)<<std::endl;
    //auto err = compare_md5sums(hRef, hTest);
    
    //std::cout<< relativeError<<" "<<absoluteError<<std::endl;
        double tolerance = std::numeric_limits<double>::epsilon();
        if( relativeError>tolerance || absoluteError>tolerance)
        {
            std::cerr<<"Differenene in Grib message Grib Message failed {Location,offset,length} = " << std::get<0>(gribLocTest) << " , " << std::to_string(std::get<1>(gribLocTest)) << " , " << std::to_string(std::get<2>(gribLocTest))<<std::endl;
            std::cerr<<"Difference in Grib message: Absolute Error = "<<absoluteError<<" relative Error = "<<relativeError<<std::endl;
            datamatch=false;
        }
    //codes_handle_delete(hRef);
    //codes_handle_delete(hTest);
    //free(bufferRef);
    //free(bufferTest);
    }
    catch(...){
        free(bufferRef);
        free(bufferTest);
    
        codes_handle_delete(hRef);
        codes_handle_delete(hTest);

        throw;
    }
    free(bufferRef);
    free(bufferTest);

    codes_handle_delete(hRef);
    codes_handle_delete(hTest);

    return datamatch;

}

void FDBCompare::execute(const CmdArgs& args) {

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
        // if(test_map.size()!=ref_map.size()){
        //     std::cout<<"Number of messages in Databases don't match"<<std::endl;
        //     std::cout<<"Test FDB #entries in Toc: "<<test_map.size()<<" Reference FDB #entries in Toc: " << ref_map.size()<<std::endl;
        //     return;
        // }
      //  std::cout<<test_map.key_eq()<<std::endl;
        //Chech that the keys match only continue with next comparison is this is the case
        if(!(compare_keys(ref_map,test_map))){
            std::cerr<<"Mars Metadata Keys don't match"<<std::endl;
            return;
        }


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
        double absoluteError;
        double relativeError;
        bool datamatch;
        for (auto& [key, value]:ref_map)
        {
            std::cerr<<"|";
            counter++;

            absoluteError=0.0;
            relativeError=0.0;
            //datamatch = gribCompare({"/home/ecm4053/tmp/test.grib",0,80599},{"/home/ecm4053/tmp/test.grib",0,80599},relativeError,absoluteError);
            datamatch = gribCompare(value,test_map[key],relativeError,absoluteError);
            
            if(datamatch) continue;
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
        //codes_context_delete(0);
        double relativeErrorAvg = (countValidErrorRelative > 0) ? (relativeErrorSum/countValidErrorRelative) : 0.0;
        double absoluteErrorAvg = (countValidErrorAbs > 0) ? (absoluteErrorSum/countValidErrorAbs) : 0.0;
        //gribCompare({"/lus/h2resw01/hpcperm/ecm4053/IFS/reference-fdb/climateDT/out/tco79l137/hz9m/hres/intel.hpcx/blq.intel.sp/h48.N4.T112xt4xh1+ioT8xt4xh0.nextgems_6h.i16r0w16.eORCA1_Z75.htco79-35661861/fdb/NATIVE_grids/root/d1:climate-dt:CMIP6:hist:1:IFS-NEMO:1:hz9m:clte:20200120/high:fc:sfc.20240611.145957.ac3-2065.bullx.384021615869965.data",138278,80599},{"/lus/h2resw01/hpcperm/ecm4053/IFS/reference-fdb/climateDT/out/tco79l137/hz9m/hres/intel.hpcx/blq.intel.sp/h48.N4.T112xt4xh1+ioT8xt4xh0.nextgems_6h.i16r0w16.eORCA1_Z75.htco79-35661861/fdb/NATIVE_grids/root/d1:climate-dt:CMIP6:hist:1:IFS-NEMO:1:hz9m:clte:20200120/high:fc:sfc.20240611.145957.ac3-2065.bullx.384021615869965.data",138278,80599});
        //gribCompare({"/home/ecm4053/tmp/test.grib",0,80599},{"/home/ecm4053/tmp/test.grib",0,80599});
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


           
    }
 
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBCompare app(argc, argv);
    return app.start();
}


//   std::cerr<<"1"<<std::endl;
//     try{
//         extractGribMessage(gribLocRef,hRef,bufferRef,contextRef);
//     }
//     catch(FailedLibraryCall& e){
//         std::cerr<<"Exception caught:"<<e.what()<<std::endl;
//         throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocRef)) + " , " + std::to_string(std::get<2>(gribLocRef)));
//     }
//     catch(CantOpenFile& e){
//         std::cerr<<"Exception caught"<<e.what()<<std::endl;
//         throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocRef)) + " , " + std::to_string(std::get<2>(gribLocRef)));

//     }
//     catch(...){
//         std::cerr<<"unknown exception caught"<<std::endl;
//         throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocRef)) + " , " + std::to_string(std::get<2>(gribLocRef)));
//     }
//     if(hRef==NULL)
//     {
//         throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocRef)) + " , " + std::to_string(std::get<2>(gribLocRef)));
//     }
//     std::cerr<<"2"<<std::endl;
//     try{
//         extractGribMessage(gribLocTest,hTest,bufferTest,contextTest);
//     }
//     catch(FailedLibraryCall& e){
//         std::cerr<<"Exception caught:"<<e.what()<<std::endl;
//         throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocRef)) + " , " + std::to_string(std::get<2>(gribLocRef)));
//     }
//     catch(CantOpenFile& e){
//         std::cerr<<"Exception caught"<<e.what()<<std::endl;
//         throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocRef)) + " , " + std::to_string(std::get<2>(gribLocRef)));

//     }
//     catch(...){
//         std::cerr<<"unknown exception caught"<<std::endl;
//         throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocRef) + " , " + std::to_string(std::get<1>(gribLocRef)) + " , " + std::to_string(std::get<2>(gribLocRef)));
//     }
//     if(hTest==NULL)
//     {
//         throw Abort("Extraction of Grib Message failed {Location,offset,length} = " + std::get<0>(gribLocTest) + " , " + std::to_string(std::get<1>(gribLocTest)) + " , " + std::to_string(std::get<2>(gribLocTest)));
//     }
//     // int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
//     // codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
//     // printInfo(hRef);
//     // printInfo(hTest);


//     std::cerr<<"3"<<std::endl;
//     try{
//         auto match = compare_header(hRef,hTest);
//         if(!match){
//             //TODO call compare_header_detail();
//             throw Abort("Headers don't match Location 1 = " + std::get<0>(gribLocRef) + " offset 1 = " + std::to_string(std::get<1>(gribLocRef)) + " length 1 = " +std::to_string(std::get<2>(gribLocRef))+" Location 2 = " + std::get<0>(gribLocTest) + " offset 2 = " + std::to_string(std::get<1>(gribLocTest)) + " length 2 = " +std::to_string(std::get<2>(gribLocTest)));
//         }
//     }
//     catch(FailedLibraryCall& e){
//         std::cerr<<"Failed Library Exception was caught"<<e.what()<<std::endl;
//         throw Abort("Headers don't match Location 1 = " + std::get<0>(gribLocRef) + " offset 1 = " + std::to_string(std::get<1>(gribLocRef)) + " length 1 = " +std::to_string(std::get<2>(gribLocRef))+" Location 2 = " + std::get<0>(gribLocTest) + " offset 2 = " + std::to_string(std::get<1>(gribLocTest)) + " length 2 = " +std::to_string(std::get<2>(gribLocTest)));
//     }


//     // //// Key interator investigation
//     // std::cerr<<"this should only be done once"<<std::endl;
//     // auto kiter =codes_keys_iterator_new(hRef,CODES_KEYS_ITERATOR_ALL_KEYS,NULL);
//     // if(!kiter){
//     //     std::cerr<<"TODO errormessage"<<std::endl;
//     // }
//     // int type;
//     // while(codes_keys_iterator_next(kiter))
//     // {
//     //     //char value[1024];//make it better
//     //     //size_t vlen=1024;
//     //     auto name = codes_keys_iterator_get_name(kiter);
//     //     codes_get_native_type(hRef,name,&type);
//     //     std::cerr<<name<<" "<< type<<std::endl;
//     // }

//     // // //// end key iterator investigation

//     std::cerr<<"4"<<std::endl;
//     try{
//         auto match_md5sums = compare_md5sums(hRef,hTest);
//         std::cerr<<"4a"<<std::endl;
//         if(match_md5sums){
//             std::cerr<<"4b"<<std::endl;
//             contextRef.reset();
//             contextTest.reset();
//             bufferRef.reset();
//             bufferTest.reset();
//             hRef.reset();
//             hTest.reset();
//             return {0.0,0.0};
//         }
//         std::cerr<<"should not reach"<<std::endl;
//     }
//     catch(BadValue& e){
//         std::cerr<<"Bad Value Exception C"<<e.what()<<std::endl;
//         throw Abort("Headers don't match Location 1 = " + std::get<0>(gribLocRef) + " offset 1 = " + std::to_string(std::get<1>(gribLocRef)) + " length 1 = " +std::to_string(std::get<2>(gribLocRef))+" Location 2 = " + std::get<0>(gribLocTest) + " offset 2 = " + std::to_string(std::get<1>(gribLocTest)) + " length 2 = " +std::to_string(std::get<2>(gribLocTest)));
//     }

//     // todo call header check in detail

//     std::cerr<<"5"<<std::endl;
//     const auto [relativeError,absoluteError] = compare_DataSection(hRef,hTest);

//     // time(&stop);
//     // std::cout<<difftime(stop,start)<<std::endl;
//     //auto err = compare_md5sums(hRef, hTest);
    
//     //std::cout<< relativeError<<" "<<absoluteError<<std::endl;
//     double tolerance = std::numeric_limits<double>::epsilon();

//     if( relativeError>tolerance || absoluteError>tolerance)
//     {
//         std::cerr<<"Differenene in Grib message Grib Message failed {Location,offset,length} = " << std::get<0>(gribLocTest) << " , " << std::to_string(std::get<1>(gribLocTest)) << " , " << std::to_string(std::get<2>(gribLocTest))<<std::endl;
//         std::cerr<<"Difference in Grib message: Absolute Error = "<<absoluteError<<" relative Error = "<<relativeError<<std::endl;
//     }
//     //codes_handle_delete(hRef);
//     //codes_handle_delete(hTest);
//     //free(bufferRef);
//     //free(bufferTest);
//     contextRef.reset();
//     contextTest.reset();
//     bufferRef.reset();
//     bufferTest.reset();
    
//     hRef.reset();
//     hTest.reset();
//     return {relativeError,absoluteError};
//     //return {2.0,2.0};
    
//     //todo check err
//     //TODO free resources
// }





    
    // //// Key interator investigation
    // std::cerr<<"this should only be done once"<<std::endl;
    // auto kiter =codes_keys_iterator_new(hRef,CODES_KEYS_ITERATOR_ALL_KEYS,NULL);
    // if(!kiter){
    //     std::cerr<<"TODO errormessage"<<std::endl;
    // }
    // int type;
    // while(codes_keys_iterator_next(kiter))
    // {
    //     //char value[1024];//make it better
    //     //size_t vlen=1024;
    //     auto name = codes_keys_iterator_get_name(kiter);
    //     codes_get_native_type(hRef,name,&type);
    //     std::cerr<<name<<" "<< type<<std::endl;
    // }

    // // //// end key iterator investigation

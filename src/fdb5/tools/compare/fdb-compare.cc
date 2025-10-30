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
#include <typeinfo>
#include <typeindex>

#include "eckit/option/CmdArgs.h"
#include "eckit/config/Resource.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/log/JSON.h"

#include "metkit/hypercube/HyperCube.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
//#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBVisitTool.h"

////// new
#include "datautil/comparator.h"
#include "datautil/common/scope.h"
#include "datautil/common/comparison_map.h"

// FDB includes
// #include "fdb5/api/FDB.h"
// #include "eckit/config/Resource.h"
#include "eckit/filesystem/PathName.h"

#include <memory>
#include <vector>

////


#include "fdb5/tools/compare/datautil/grib/mars-to-grib.h"
#include "fdb5/tools/compare/datautil/grib/data-section-util.h"
#include "fdb5/tools/compare/datautil/common/data_map.h"


#include "eccodes.h"
//#include "grib_api_internal.h"


using namespace eckit;
using namespace eckit::option;
using compare::Options;
using compare::Scope;
using namespace compare::common;

namespace fdb5 {
namespace tools {

extern "C"
{
    int grib_get_message_headers(const grib_handle* h, const void** msg, size_t* size);
}
//----------------------------------------------------------------------------------------------------------------------
//Reorganise. Ideally this should be encapsulated in a class or somewhere and not just here
// typedef std::tuple<std::string,long long int, long long int> GribLocation; //{Location,Offset,Length}
// Utility function to append key-value pairs to std::map
void appendKeys(std::map<std::string, std::string>& container, const std::string& keyValueStr) {
    std::istringstream stream(keyValueStr);
    std::string entry;

    while (std::getline(stream, entry, ',')) {
        std::istringstream pairStream(entry);
        std::string key, value;
        if (std::getline(pairStream, key, '=') && std::getline(pairStream, value)) {
            container[key] = value;  // Update or insert key-value pair
        } else {
            std::cerr << "Invalid key-value pair: " << entry << std::endl;
        }
    }
}

// Utility function to append entries to std::unordered_set
void appendKeys(std::unordered_set<std::string>& container, const std::string& keyValueStr) {
    std::istringstream stream(keyValueStr);
    std::string entry;

    while (std::getline(stream, entry, ',')) {
        container.insert(entry);  // Insert the whole entry as a single string
    }
}

// Utility function to append vector of strings to std::unordered_set
void appendKeys(std::unordered_set<std::string>& container, std::vector<std::string> v) {
    for (const auto& entry : v) {
        container.insert(entry);
    }
}

void request_diff(std::string ref_req, std::map<std::string,std::string> overwrite_test, std::map<std::string,std::pair<std::string,std::string>>& mars_req_diff){
    std::map<std::string,std::string> ref_req_map;
    appendKeys(ref_req_map,ref_req);
    for (auto& [key, value]:overwrite_test){
        mars_req_diff[key] = {ref_req_map[key], value};
    }
}


template <typename T>
std::string createStringFromArray(const std::vector<T>& array, size_t length) {
    std::ostringstream oss;
    size_t numElements = std::min(length, array.size());

    for (size_t i = 0; i < numElements; ++i) {
        oss << array[i];
        if (i != numElements - 1) {
            oss << ", ";
        }
    }

    return oss.str();
}
class FDBCompare : public FDBVisitTool {

  public: // methods

    FDBCompare(int argc, char **argv) :
        FDBVisitTool(argc, argv, "class,expver") {

        options_.push_back(new SimpleOption<std::string>("test-config", "Path to a FDB config"));
        options_.push_back(new SimpleOption<std::string>("reference-config", "Path to a second FDB config"));
        options_.push_back(new SimpleOption<std::string>("reference-request", "Mars key request for reference FDB entry (e.g reference experiment)"));
        options_.push_back(new SimpleOption<std::string>("test-request","Mars key request for test FDB entry (e.g. test experiment)"));
        options_.push_back(new SimpleOption<std::string>("test-request-overwrite","Format: \"Key1=Value1,Key2=Value2,...KeyN=ValueN\", these values will be changed in the test request")); // TODO potentially remove again
        options_.push_back(new SimpleOption<std::string>("scope", "[mars (default)|header-only|all] The FDBs can be compared in different scopes, 1) [mars] Mars Metadata only (default), 2) [header-only] includes Mars Key comparison and the comparison of the data headers (e.g. grib headers) 3) [all] includes Mars key and data header comparison but also the data sections up to a defined floating point tolerance "));
        options_.push_back(new SimpleOption<std::string>("grib-comparison-type", " [hashkeys|grib-keys(default)|bitexact] Comparing two Grib messages can be done via either (grib-comparison-type=hashkeys) in a bitexact way with hashkeys for each sections, via grib keys in the reference FDB (grib-comparison-type=grib-keys (default))  or by directly comparing bitexact memory segments(grib-comparison-type=bitexact)" ));
        options_.push_back(new SimpleOption<double>("fp-tolerance", "Floatinng point tolerance for comparison default=machine tolerance epsilon. Tolerance will only be used if level=2 is set otherwise tolerance will have no effect"));
        options_.push_back(new SimpleOption<std::string>("mars-keys-ignore", "Format: \"Key1=Value1,Key2=Value2,...KeyN=ValueN\" All Messages that contain any of the defined key value pairs will be omitted"));
        options_.push_back(new SimpleOption<std::string>("grib-keys-select", "Format: \"Key1,Key2,Key3...KeyN\" Only the specified grib keys will be compared (Only effective with grib-comparison-type=grib-keys)" ));
        options_.push_back(new SimpleOption<std::string>("grib-keys-ignore", "Format: \" \" The specified key words will be ignored (only effective with grib-comparison-type=grib-keys)"));
        options_.push_back(new SimpleOption<bool>("verbose","Print additional output, including `|' for every compared grib message to see status"));
    }

  private: // methods

    virtual void execute(const CmdArgs& args);
    virtual void init(const CmdArgs &args);
    virtual bool gribCompare(const DataLocation& gribLocRef,const DataLocation& gribLocTest, double relativeError, double absoluteError);
    virtual void usage(const std::string& tool) const override;

    std::string testConfig_;
    std::string referenceConfig_;
    std::string gribcomparison_;
    std::string scope_;
    double tolerance_;
    bool detail_; // this parameter should be used to determine if an additional information is required why a two FDBs don't match. 
    std::map<std::string,std::string> mars_keys_ignore_;
    std::unordered_set<std::string> grib_keys_select_;
    std::unordered_set<std::string> grib_keys_ignore_;
    std::map<std::string,std::string> mars_req_test_overwrite_;
    std::map<std::string,std::pair<std::string,std::string>> req_diff_;
    std::string testRequest_;
    std::string referenceRequest_;
    bool verbose_;
    bool singleFDB_=false;
};


void FDBCompare::init(const CmdArgs& args) {

    FDBVisitTool::init(args);


    // ASSERT(args.has("test-config"));
    // ASSERT(args.has("reference-config"));
    
    testConfig_ = args.getString("test-config","");
    // if(testConfig_.empty()){
    //     throw UserError("No path for FDB 1 specified",Here());
    // }
    referenceConfig_ = args.getString("reference-config","");
    // if(referenceConfig_.empty()){
    //     throw UserError("No path for FDB 2 specified",Here());
    // }
    if(testConfig_.empty() && referenceConfig_.empty()){
        singleFDB_=true;
    }

    scope_ = args.getString("scope","mars");
    if((scope_ != "mars") && (scope_ != "header-only") && (scope_ != "all")){
        throw UserError("Unknown comparison scope "+scope_,Here());
    }
    if(scope_ == "header-only") DataKeys::append_data_relevant_keys(grib_keys_ignore_);

    gribcomparison_ = args.getString("grib-comparison-type","grib-keys");
    if(gribcomparison_ != "hashkeys" && gribcomparison_ != "bitexact" && gribcomparison_ != "grib-keys"){
        throw UserError("Unknown Grib comparison method "+gribcomparison_, Here());
    }

    tolerance_ = args.getDouble("fp-tolerance", std::numeric_limits<double>::epsilon());
    if(tolerance_<std::numeric_limits<double>::epsilon()){
        throw UserError("The tolerance should be at least machine epsilon to compare floating point values.",Here());
    }
    std::string tmp = args.getString("mars-keys-ignore","");
    if(!tmp.empty()){
        appendKeys(mars_keys_ignore_, tmp);
    }

    referenceRequest_ = args.getString("reference-request","");
    testRequest_ = args.getString("test-request","");
    //TODO assert that if one is set the other one is set as well. 
    tmp = args.getString("grib-keys-select","");
    if(!tmp.empty()){
        if(gribcomparison_ != "grib-keys"){
            std::cout<<"The specified eccode select keys will have no effect because grib-comparison-type = "<<gribcomparison_<<" and would need to be grib-keys"<<std::endl;
        }
        else{
            appendKeys(grib_keys_select_,tmp);
        }    }
    tmp = args.getString("grib-keys-ignore","");
    if(!tmp.empty()){
        if(gribcomparison_ != "grib-keys"){
            std::cout<<"The specified eccode ignore keys will have no effect because grib-comparison-type = "<<gribcomparison_<<" and would need to be grib-keys"<<std::endl;
        }
        else{
            appendKeys(grib_keys_ignore_,tmp);
        }
    }
    tmp = args.getString("test-request-overwrite","");
    if(!tmp.empty()) appendKeys(mars_req_test_overwrite_,tmp);
    request_diff(referenceRequest_, mars_req_test_overwrite_, req_diff_);
    std::cout<<"req diff" << req_diff_<<std::endl;
    for (auto const& [key, _] : req_diff_) {
        appendKeys(grib_keys_ignore_, MarsGribMap::translate(key));
    }
    
    //TODO CHECK WHY TEST AND REF MATRIX CONTAIN TOO MANY ELEMENTS
    verbose_ = args.getBool("verbose",false);
    
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
        std::cerr<<"[GRIB COMPARE ERROR] Message is not a Grib message: Comparing data formats other than Grib messages is currently not implemented"<<std::endl;
        return 0;
    }

    //EditionNumber will is always the 8th Byte in a Grib message
    int editionnumberRef = (int)bufferRef[7];
    int editionnumberTest = (int)bufferRef[7];

    if(editionnumberRef != editionnumberTest){
        std::cerr<<"[GRIB COMPARE ERROR] Editionnumbers don't match -> cannot compare Grib message"<<std::endl;
        return 0;
    }
    if(editionnumberRef == 2){
        //Total Length Value starts at position 8 in the Grid2 header
        uint64_t totalLength = readValue<uint64_t>(bufferRef,8);
        if(length != totalLength){
            std::cerr<<"[GRIB COMPARE ERROR] Message length in Mars Key location and total message length specified in Grib message don't match"<<std::endl;
            return 0;
        }
        //GRIB2 Section 0 is always 16 Bytes long:
        offset += 16;
    }
    else if(editionnumberRef == 1){
        uint32_t totalLength = readValue3Byte(bufferRef,4);
        if(length != totalLength){
            std::cerr<<"[GRIB COMPARE ERROR] Message length in Mars Key location and total message length specified in Grib message don't match"<<std::endl;
            return 0;
        }
        //Grib1 Section 0 is always 8 Bytes long:
        offset += 8;
    }else{
        std::cerr<<"[GRIB COMPARE ERROR] Unknown Grib edition number "<< editionnumberRef<<std::endl;
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
                std::cerr <<"[GRIB COMPARE ERROR] Unexpected end of buffer while reading section length for section "<< i+1<<std::endl;
                return ((i+1)==numSections) ? 9:i+1;
            }
            sectionLengthRef = readValue3Byte(bufferRef,offset);
            sectionLengthTest = readValue3Byte(bufferTest,offset);
        }
        else {
            //Grib 2 stores a 4 byte section length value at the beginning of each section
            if (offset + 4 > length){
                std::cerr <<"[GRIB COMPARE ERROR] Unexpected end of buffer while reading section length for section "<< i+1<<std::endl;
                return ((i+1)==numSections) ? 9:i+1;
            }
            sectionLengthRef = readValue<uint32_t>(bufferRef,offset);
            sectionLengthTest = readValue<uint32_t>(bufferTest,offset);
        }
        if(sectionLengthRef != sectionLengthTest){
            std::cout<<"[GRIB COMPARE MISMATCH] The section "<<i+1<<" length parameter differs between the grib messages "<<std::endl;
            return ((i+1)==numSections) ? 9:i+1;
        }
        //Memory comparison for each section 
        if(! memComparison(bufferRef, bufferTest,offset,sectionLengthRef)){
            std::cout<<"[GRIB COMPARE MISMATCH] Memorycomparison failed in section "<<i+1<<std::endl;
            return ((i+1)==numSections) ? 9:i+1;
        }
        offset += sectionLengthRef;
    }
    //Test that the final bit of the message 7777 is present in both
    while(offset<length){
        if((static_cast<char>(bufferRef[offset]) != '7')||(static_cast<char>(bufferTest[offset]) != '7')){
            std::cerr<<"[GRIB COMPARE ERROR] Message does not contain the correct section"<<std::endl;
        }
        ++offset;      
    }

    return 10;
}
//void extractGribMessage(GribLocation GribLocTuple ,std::unique_ptr<codes_handle,decltype(&codes_handle_delete)>& h, std::unique_ptr<char[]>& buffer,std::unique_ptr<codes_context,decltype(&codes_context_delete)>& context){
void extractGribMessage(DataLocation GribLocTuple ,char* buffer){

    const auto [location,offset,length] = GribLocTuple;
    std::ifstream refFile(location,std::ios::binary);
    //get first message handle
    if(refFile.is_open())
    {
        refFile.seekg(offset,std::ios::beg);
        //For a tco79 this seems to be the bottleneck: Optimisation suggestions: would be to keep the files open in a  Filestream cache. Potentially with an option to only store a certain amount of filestreams to avoid memory of open file distriptor issues.
        if(!refFile.read(buffer,length)) {
            refFile.close();
            throw Abort("Could not read the data from file",Here());
        }
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
    // std::cout<<"********************* Reference Grib Message Header **********************************"<<std::endl;
    // int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    // codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
    // std::cout<<"********************* Test Grib Message Header **********************************"<<std::endl;
    // dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    // codes_dump_content(hTest, stdout, "wmo", dump_flags, NULL);
    // std::cout<<"********************** END DEBUG SUMMARY *****************************************"<<std::endl;
    return false;

}
bool compare_header_keys(codes_handle * hRef, codes_handle * hTest)
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
    // std::cout<<"********************* Reference Grib Message Header **********************************"<<std::endl;
    // int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    // codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
    // std::cout<<"********************* Test Grib Message Header **********************************"<<std::endl;
    // dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    // codes_dump_content(hTest, stdout, "wmo", dump_flags, NULL);
    // std::cout<<"********************** END DEBUG SUMMARY *****************************************"<<std::endl;
    return false;

}
//Return 2 if Data section fails, Return 1 if all sections match return 0 if any other section does not match
int compare_md5sums(codes_handle* hRef, codes_handle *hTest)
{
    int err1,err2;
    int hashkeylength = 33;

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

    //Create a vector with all relevant sections
    std::vector<std::string> grib_sections;
    //"md5Headers"; This is possible to add but is already covered in memcmp the header message (But the header message does not contain section data 7 (or 4 in Grib1) bitmap 6 (or 3 in Grib1) and section DRS (only present in Grib2))
    if(gribEditionRef == 2)
    {
        //check MD5sums for Grib2 section 5,6 and 7
        grib_sections.push_back("md5Section5");
        grib_sections.push_back("md5Section6");
        grib_sections.push_back("md5Section7");
    }
    if(gribEditionRef ==1)
    {   
        //compare MD5 Keys for Grib 1 bitmap and datasection 3 and 4
        grib_sections.push_back("md5Section3");
        grib_sections.push_back("md5Section4");
    }

    //iterate over map and extract the string hash key 
    for (const auto value:grib_sections)
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
            // std::cout<<"Hash keys don't match for section "<<value<<std::endl;
            // std::cout<<"********************* Reference Grib Message Header **********************************"<<std::endl;
            // int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
            // codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
            // std::cout<<"********************* Test Grib Message Header **********************************"<<std::endl;
            // dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
            // codes_dump_content(hTest, stdout, "wmo", dump_flags, NULL);
            // std::cout<<"********************** END DEBUG SUMMARY *****************************************"<<std::endl;
            //Check if it's a data section that doesn't match
            if((gribEditionRef==1 && value=="md5Section4") || (gribEditionRef==2 && value=="md5Section7")) return 2;
            return 0;
        }
        
    }
    return 1;
}
//std::tuple<relativeError,absoluteError>
void compare_DataSection(codes_handle* hRef, codes_handle* hTest,double relativeError, double absoluteError,double tolerance)
{
    size_t valuesLenRef = 0;
    size_t valuesLenTest = 0;
    double maxError = 0.;

    std::cout<<"STEBA DEBUG STARTING DATA COMPARISON"<<std::endl;
    //make sure that this is set to zero 
    ASSERT(relativeError<=tolerance);
    ASSERT(absoluteError<=tolerance);
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
        if(error<=tolerance) continue;
        absoluteError += error;
        denominator = std::fabs(valuesRef[i]) > tolerance ? std::fabs(valuesRef[i]) : tolerance;
        relativeError += error/denominator;
    }


    free(valuesTest);
    free(valuesRef);
    return;
}
std::string value_to_string(codes_handle* h, const char* name, int codes_data_type, size_t length){
       
    std::ostringstream oss;
    switch(codes_data_type){
        case CODES_TYPE_STRING:{
            std::vector<char> val(length);
            CODES_CHECK(codes_get_string(h,name,val.data(),&length),CODES_SUCCESS);
            oss.write(val.data(),length);
            break;
        }
        case CODES_TYPE_LONG:{
            std::vector<long> val(length);
            CODES_CHECK(codes_get_long_array(h,name,val.data(),&length),CODES_SUCCESS);
            oss<<createStringFromArray(val,100);
            break;
        }
        case CODES_TYPE_DOUBLE:{
            std::vector<double> val(length);
            CODES_CHECK(codes_get_double_array(h,name,val.data(),&length),CODES_SUCCESS);
            oss<<createStringFromArray(val,100);
            break;
        }
        case CODES_TYPE_BYTES:{
            std::vector<unsigned char> val(length);
            CODES_CHECK(codes_get_bytes(h,name,val.data(),&length),CODES_SUCCESS);
            for(auto c: val)
                oss<<static_cast<int>(c)<<" ";
            break;
        }
        default:
            throw Abort("ECCODES unsupported type "+codes_data_type);
    }
    return oss.str();
}



// only compare the key of each map. the tupel is expected to diverge. first as bytestreams and only if they don't match get more details
//swapped means that the codes handles were swapped so that 
std::tuple<bool,bool> compare_values(codes_handle* hRef, codes_handle* hTest, const char* name){
    size_t lenRef;
    size_t lenTest;
    size_t lengthT=0; //number of bytes (lenRef *sizeof(Datatype))
    size_t lengthR=0; //number of bytes (lenRef * sizeof(Datatype))
    int err;
    std::string ref = "Reference FDB";
    std::string test=  "Test FDB";
    bool isMissingRef,isMissingTest;
    int typeRef,typeTest;
    unsigned char* uvalRef;
    unsigned char* uvalTest;
    bool dataSectionMatch = true;
    // size_t lengthT;
    // int typeT;
    // codes_get_native_type(hRef,"bitmapPresent",&typeT);
    // codes_get_size(hRef,"bitmapPresent",&lengthT);
    // std::cout<<"Values: "<<typeT<<" "<<lengthT<<std::endl;
    // codes_get_native_type(hRef,"codedValues",&typeT);
    // codes_get_size(hRef,"codedValues",&lengthT);
    // std::cout<<"codedValues: "<<typeT<<" "<<lengthT<<std::endl;

    
    CODES_CHECK(codes_get_native_type(hRef,name,&typeRef),CODES_SUCCESS);
    CODES_CHECK(codes_get_native_type(hTest,name,&typeTest),CODES_SUCCESS);
    if(typeRef != typeTest) {
        std::cout<<"[GRIB COMPARISON MISMATCH]"<< std::string(name)<<"Grib comparison Type mismatch: Reference Value "<<value_to_string(hRef,name, typeRef,lenRef)<<" CODES_TYPE " << typeRef<<"\nTest value: "<<value_to_string(hTest,name, typeTest,lenTest)<<" CODES_TYPE " << typeTest<<std::endl;
        return {false,dataSectionMatch}; //DataSectionMismatch is always false unless the difference is cause by the actual values
    }
    if((err=codes_get_size(hRef,name,&lenRef))!=CODES_SUCCESS){
        throw Abort("Error: Cannot get size of "+std::string(name)+"Error message "+std::string(codes_get_error_message(err)));
    }
    if((err=codes_get_size(hTest,name,&lenTest))!=CODES_SUCCESS){
        if(err==CODES_NOT_FOUND){
                 
            std::cout<<"[GRIB COMPARISON MISMATCH]"<<name<<" not found in test grib message but found in reference grib message"<<std::endl;
        }
        throw Abort("Error: Cannot get size of "+std::string(name)+"Error message "+std::string(codes_get_error_message(err)));
    }
    if(lenRef != lenTest) {
        std::cout<<"[GRIB COMPARISON MISMATCH]"<< std::string(name)<<" Reference Value "<<value_to_string(hRef,name, typeRef,lenRef)<<" Test value: "<<value_to_string(hTest,name, typeTest,lenTest)<<std::endl;
        return {false,dataSectionMatch};
    }
    //Only using lenRef and typeRef from here on because they are the same
    switch(typeRef){
        case CODES_TYPE_STRING:
            CODES_CHECK(codes_get_length(hRef,name,&lenRef),CODES_SUCCESS);
            CODES_CHECK(codes_get_length(hTest,name,&lenTest),CODES_SUCCESS);
            lengthR = lenRef*sizeof(char);
            lengthT = lenTest*sizeof(char);
            break;
        case CODES_TYPE_LONG:
            lengthR = lenRef*sizeof(long);
            lengthT = lenTest*sizeof(long);
            break;
        case CODES_TYPE_DOUBLE:
            lengthR = lenRef*sizeof(double);
            lengthT = lenTest*sizeof(double);
            break;
        case CODES_TYPE_BYTES:
            CODES_CHECK(codes_get_length(hRef,name,&lenRef),CODES_SUCCESS);
            CODES_CHECK(codes_get_length(hTest,name,&lenTest),CODES_SUCCESS);
            // if(lenRef == 1){
            //     lenRef = 512;
            // } //HACK USED IN ECCODES GRIB_COMPARE. For some messages it seems to be necessary.
            lengthR = lenRef*sizeof(unsigned char);
            lengthT = lenTest*sizeof(unsigned char);
            break;
        default:
            throw Abort("ECCODES unsupported type "+typeRef);
    }
   // std::cout<<"name = "<<name<<" lenRef "<<lenRef<<" type "<<typeRef<<" lenTest "<<lenTest<<" typeTest " << typeTest<<" lenghth "<<length<<std::endl;

    isMissingRef = ((codes_is_missing(hRef, name, &err) == 1) && (err == 0)) ? true : false;
    isMissingTest = ((codes_is_missing(hTest, name, &err) == 1) && (err == 0)) ? true : false;

    if (isMissingRef && isMissingTest) return {true,dataSectionMatch};
    
    if (isMissingRef) {
        std::cout<<"[GRIB COMPARISON MISMATCH]"<<name<<" is set to missing in reference FDB but is not missing in test FDB"<<std::endl;
        return {false,dataSectionMatch};
    }

    if (isMissingTest) {
        std::cout<<"[GRIB COMPARISON MISMATCH]"<<name<<" is set to missing in test FDB but is not missing in reference FDB"<<std::endl;
        return {false,dataSectionMatch};
    }

    uvalRef = (unsigned char*)malloc(lengthR);
    uvalTest = (unsigned char*)malloc(lengthT);

    if ((err = grib_get_bytes(hRef, name, uvalRef, &lengthR)) != GRIB_SUCCESS) {
        free(uvalRef);
        free(uvalTest);
        // std::cout<<"********************* Reference Grib Message Header **********************************"<<std::endl;
        // int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
        // codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
        // std::cout<<"********************* Test Grib Message Header **********************************"<<std::endl;
        // dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
        // codes_dump_content(hTest, stdout, "wmo", dump_flags, NULL);
        // std::cout<<"********************** END DEBUG SUMMARY *****************************************"<<std::endl;
   
        // std::cout<<"Name "<<name<<" type "<< typeRef<<" lenRef "<<lenRef<<" length "<<length<<std::endl;
        throw Abort("Error Cannot get bytes value of "+std::string(name)+" in reference FDB");
    }
    //std::cout<<"ireturn length = "<<length<<std::endl;
    if ((err = grib_get_bytes(hTest, name, uvalTest, &lengthT)) != GRIB_SUCCESS) {
        free(uvalRef);
        free(uvalTest);
        throw Abort("Error Cannot get bytes value of "+std::string(name)+" in test FDB");
    }

    if(lengthT != lengthR){
        std::cout<<"[GRIB COMPARISON MISMATCH]"<<" Key" << std::string(name)<<"can't compare retrieved data bitexact because of a bytelength mismatch "<<" lengthR= "<<lengthR<<"lengthT= "<<lengthT<<std::endl;
        std::cout<<"this could be because of encoding " <<std::endl;
        return {false,false};
    }
    bool match = true;
    if (memcmp(uvalRef, uvalTest, lengthR) != 0) {
        if(!((std::string(name)=="values")||(std::string(name)=="packedValues")||(std::string(name)=="codedValues")))
        {    
            std::cout<<"[GRIB COMPARISON MISMATCH]"<<" Key="<<std::string(name)<<" Reference Value="<<value_to_string(hRef,name, typeRef,lenRef)<<" Test value:="<<value_to_string(hTest,name, typeTest,lenTest)<<std::endl;
            dataSectionMatch=false;
            match=false;
            
        }
        else{
            dataSectionMatch = true;
            match=true; //set to true so that the Data Values can be checked numerically
        }
    }
    free(uvalRef);
    free(uvalTest);
    return {match,dataSectionMatch};

}
template<typename T>
std::tuple<bool,bool> compare_eccodes(codes_handle* hRef, codes_handle* hTest,T& ignore_list,T& match_list, std::string scope){
    
    bool matchreturn = true;
    bool dataSectionreturn = true;
    if(match_list.size() != 0){
        for(auto key: match_list){
            auto [match, dataValuesMatch]  = compare_values(hRef,hTest,key.c_str());
            if(!match) matchreturn = match;

            if(!dataValuesMatch) dataSectionreturn=dataValuesMatch;
        }

    }
    else{
        const char * name;
        bool dataValuesMismatch=false;
        int flags = CODES_KEYS_ITERATOR_SKIP_COMPUTED; // always skip computed keys

        codes_keys_iterator* key_iter = codes_keys_iterator_new(hRef,flags,NULL);
        if(!key_iter){
            throw Abort("Error: Eccodes Could not create Iterator",Here());
        }

        while(codes_keys_iterator_next(key_iter)){
            name = codes_keys_iterator_get_name(key_iter);
            if(!name){
                throw Abort("Error: Eccodes could not retrieve name from Key iterator",Here());
            }
            if(ignore_list.find(std::string(name))!=ignore_list.end() ) continue;
            auto [match, dataValuesMatch]  = compare_values(hRef,hTest,name);
            if(!match) matchreturn = match;

            if(!dataValuesMatch) dataSectionreturn=dataValuesMatch;
        }

    }
    return {matchreturn,dataSectionreturn};
    
    

}



bool FDBCompare::gribCompare(const DataLocation& gribLocRef,const DataLocation& gribLocTest, double relativeError, double absoluteError)
{
    time_t start,stop;
    codes_handle* hRef;
    codes_handle* hTest;
    ASSERT(relativeError==0.0);
    ASSERT(absoluteError==0.0);

    bool datamatch = true;
    char* bufferRef = (char*)malloc((gribLocRef.length)*sizeof(char));
    if(!bufferRef)
    {
        std::cerr<<"Here()"<<std::endl;
        throw std::bad_alloc();
    }
    char* bufferTest = (char*)malloc((gribLocTest.length)*sizeof(char));
    if(!bufferTest)
    {
        std::cerr<<Here()<<std::endl;
        throw std::bad_alloc();
    }
    try{
        extractGribMessage(gribLocRef,bufferRef);
        if(bufferRef==NULL)
        {
            std::ostringstream oss;
            oss << "Extraction of Grib Message failed {Location,offset,length} = " << gribLocRef;
            throw Abort(oss.str());
        }
        extractGribMessage(gribLocTest,bufferTest);
        if(bufferTest==NULL)
        {
            std::ostringstream oss;
            oss << "Extraction of Grib Message failed {Location,offset,length} = " << gribLocTest;
            throw Abort(oss.str());
        }
        if(gribcomparison_ == "bitexact"){
            if( verbose_ ) std::cout<<"[LOG] Memory comparison (Bytestream)"<<std::endl;
            //Bitexact comparison directly on the Bytestream begin
            int i =  bit_comparison(bufferRef, bufferTest,(gribLocTest.length));
            if(i==10){
                free(bufferRef);
                free(bufferTest);
                return true;
            } //Message is bitexact
            if(i<9 && i>=0){
                std::ostringstream oss;
                oss << "[GRIB COMPARISON MISMATCH: BITEXACT: HEADER ] {Location,offset,length} = " << gribLocRef;
                std::cout<<oss.str()<<std::endl;
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
            ASSERT(i==9); // Numerical check of data section IF only the data section failed it is possible to do a floating point check if data check is specified
        }
        hRef = codes_handle_new_from_message(NULL,bufferRef,(gribLocRef.length));
        if (!hRef)
        {
            std::ostringstream oss;
            oss << "ECCODES Handle could not be created {Location,offset,length} = " << gribLocRef;
            throw FailedLibraryCall("eccodes","codes_handle_new_from_message",oss.str(),Here());   
        }
        hTest = codes_handle_new_from_message(NULL,bufferTest,(gribLocTest.length));
        if (!hTest)
        {

            std::ostringstream oss;
            oss << "ECCODES Handle could not be created {Location,offset,length} = " << gribLocTest;
            throw FailedLibraryCall("eccodes","codes_handle_new_from_message",oss.str(),Here());  
        }
        if(gribcomparison_ == "hashkeys"){
            // BitExactComparison with hashkeys begin
            //std::cout<<"hashkeys comparison"<<std::endl;
            auto match = compare_header(hRef,hTest);
            if(!match){
                //just called to give a more specific output if the comparison failed
                // compare_eccodes(hRef,hTest,grib_keys_ignore_,grib_keys_select_);
                free(bufferRef);
                free(bufferTest);
            
                codes_handle_delete(hRef);
                codes_handle_delete(hTest);

                std::ostringstream oss;
                oss << "[GRIB COMPARISON MISMATCH] HEADER SECTION MD5SUMS ] Headers don't match Reference = " << gribLocRef << " Test = " << gribLocTest <<"\n";
                std::cout<<oss.str()<<std::endl;
                
                return false;
            }
        
            //Return 2 if Data section fails, Return 1 if all sections match, return 0 if any other section does not match
            int match_md5sums = compare_md5sums(hRef,hTest);
            if(match_md5sums == 1){
                free(bufferRef);
                free(bufferTest);
            
                codes_handle_delete(hRef);
                codes_handle_delete(hTest);

                absoluteError = 0.0;
                relativeError = 0.0;
                return true;
            }
            if(match_md5sums != 2) // stop if it's not the data section that didn't match
            {
                free(bufferRef);
                free(bufferTest);
            
                codes_handle_delete(hRef);
                codes_handle_delete(hTest);

                absoluteError = 0.0;
                relativeError = 0.0;
                return false;
            }
            ASSERT(match_md5sums == 2); // HEADER CHECK WAS SUCCESSFUL but data needs to be checked with a fp comparison
        }
        if(gribcomparison_ == "grib-keys"){
            auto [match,datasectionMatch] = compare_eccodes(hRef, hTest,grib_keys_ignore_,grib_keys_select_, scope_);
            if(!match){
                std::ostringstream oss;
                oss << "[ GRIB COMPARISON MISMATCH KEY-BY-KEY ] Grib Message REF: " << gribLocRef << "  TEST at {Location,offset,length} = "<< gribLocTest<<"\n";
                std::cout<<oss.str()<<std::endl;

                free(bufferRef);
                free(bufferTest);
            
                codes_handle_delete(hRef);
                codes_handle_delete(hTest);

                absoluteError = 0.0;
                relativeError = 0.0;
                return false;
            }
            else if(datasectionMatch && match){
                free(bufferRef);
                free(bufferTest);
            
                codes_handle_delete(hRef);
                codes_handle_delete(hTest);

                absoluteError = 0.0;
                relativeError = 0.0;
                return true;
            };
            // ASSERT((datasectionMatch==false) && (match==true));
            //else continue as we have a Mismatch in the data section but all other sections had a match compare the data section numerically 
        }
        if(scope_ == "all") {
            compare_DataSection(hRef,hTest,relativeError,absoluteError,tolerance_);
        }

        if( relativeError>tolerance_ || absoluteError>tolerance_)
        {
            std::ostringstream oss;
            oss << "[GRIB COMPARISON FAILED FP-DATA CHECK] Differenene in Grib message Grib Message failed  Ref {Location,offset,length} = " << gribLocRef << "  TEST at {Location,offset,length} = " <<gribLocTest <<"\n";
            oss << "Difference in Grib message: Absolute Error = "<<absoluteError<<" relative Error = "<<relativeError<<"\n";
            std::cout<<oss.str()<<std::endl;
            datamatch=false;
        }

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
    FDB fdbref;
    FDB fdbtest;


    if(!singleFDB_) {
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
        
        fdbref = FDB( Config::make(configPathref));

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
    
        fdbtest = FDB( Config::make(configPathtest));
    }else{
        fdbtest = FDB(config(args));
        fdbref = FDB(config(args));
    }

    compare::Options opts;
    opts.scope = compare::parseScope(scope_);
    opts.referenceRequest = referenceRequest_;
    opts.testRequest = testRequest_;
    opts.marsReqDiff = req_diff_;

    DataIndex refIdx, testIdx;

    const auto pickReq = [&](const std::string& s) -> fdb5::FDBToolRequest {
        if (!s.empty()) return FDBToolRequest::requestsFromString(s)[0];
        return requests()[0];
    };

    auto reqRef  = pickReq(opts.referenceRequest);
    auto reqTest = pickReq(opts.testRequest);

    assemble_compare_map(fdbref, refIdx, reqRef,  mars_keys_ignore_);
    assemble_compare_map(fdbtest, testIdx, reqTest, mars_keys_ignore_);



    
    //FDBToolRequest is possible by using the known syntax on the command line and restrict the amount of data that is 
    //retrieved by fdb.list. But in develop we might also want to request individual param ID. that currently results in a
    //Serious bug exception. So even tough the filter is possible we will be able to reduce it further.  
    // for (const FDBToolRequest& request : requests()) {
    auto marsCmp = compare::ComparatorFactory::create("mars", refIdx, testIdx);
    auto marsRes = marsCmp->compare(opts);
    if (!marsRes.ok) {
        std::cerr << "[MARS KEYS COMPARE] MISMATCH\n";
        return; 
    }
    


    // Return if only a comparison of Mars metadata messages was specified as Command Line option
    if(scope_ == "mars"){
        return;
    }
    std::cout<<"[GRIB COMPARISON LOG]"<<"Compare Grib messages"<<std::endl;

        //Compare the individual Grib messages
        double absoluteErrorSum = 0.0;
        double absoluteErrorMin = 0.0;
        double absoluteErrorMax = 0.0;
        size_t countValidErrorAbs = 0;
    
        double relativeErrorSum = 0.0;
        double relativeErrorMax = 0.0;
        double relativeErrorMin = 0.0;
        size_t countValidErrorRelative = 0;
        double absoluteError=0.0;
        double relativeError=0.0;
        bool datamatch=false;

        // Lambda: modifies key inline and returns reference to it
        auto applyDiffAndReturn = [](std::map<std::string,std::string> k,
                                    const std::map<std::string,std::pair<std::string,std::string>>& diff) -> std::map<std::string,std::string> {
            for (const auto& [field, val_pair] : diff) {
                auto it = k.find(field);
                if (it != k.end()) {
                    it->second = val_pair.second; // replace value
                }
            }
            return k; // return modified copy
        };

        // std::cout<<"REF "<<ref_map<<std::endl;
        // std::cout<<"TEST "<<test_map<<std::endl;
     
        for (auto& [key, value]:refIdx)
        {
            std::cerr<<"|";

            absoluteError=0.0;
            relativeError=0.0;
            try{
                // std::cout<<"key = "<<key<<"changed key = "<<applyDiffAndReturn(key,req_diff_)<<std::endl;
                // std::cout<<"test_map[key]"<<test_map[applyDiffAndReturn(key,req_diff_)]<<std::endl;

                datamatch = gribCompare(value,testIdx[applyDiffAndReturn(key,req_diff_)],relativeError,absoluteError);
            }
            catch(...){
                std::cout<<"[GRIB COMPARE MISMATCH] Grib Comparison failed Mars Key"<<key<<std::endl;
                throw;
            }
            if(!datamatch){
                std::cout<<"[GRIB COMPARE MISMATCH] Grib Comaprison failed! Mars Key\n"<<key<<std::endl;
                // return;
            } 
            //std::cout<<"Numerical differences in the data values of Grib message: "<<key<<std::endl;
           
            if(absoluteError > tolerance_)
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
            if(relativeError > tolerance_)
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
                        
        }
        if(scope_ == "all") {
            //codes_context_delete(0); // continue to monitor memory usage. Potentially it might make sense to condes_context_delete(0); at some point to flush all the cached eccodes pages
            double relativeErrorAvg = (countValidErrorRelative > 0) ? (relativeErrorSum/countValidErrorRelative) : 0.0;
            double absoluteErrorAvg = (countValidErrorAbs > 0) ? (absoluteErrorSum/countValidErrorAbs) : 0.0;

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
    // }
 
}

void FDBCompare::usage(const std::string &tool) const {

    // derived classes should provide this type of usage information ...

    Log::info() << "FDB Comparison" << std::endl
                << std::endl;

    Log::info() << "Examples:" << std::endl
                << "=========" << std::endl << std::endl
                << tool << " --reference-config=<path/to/reference/config.yaml> --test-config=<path/to/test/config.yaml --minimum-keys=\"\" --all"
                << std::endl
                << std::endl;

    FDBTool::usage(tool);
}
//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBCompare app(argc, argv);
    return app.start();
}


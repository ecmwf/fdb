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
// Custom hash function for std::map<std::string, std::string>
struct MapHash {
    std::size_t operator()(const std::map<std::string, std::string>& m) const {
        std::size_t seed = 0; // Initial seed value
        for (const auto& pair : m) {
            // Combine the hash of each element pair into the seed using a simple hash combine algorithm
            seed ^= std::hash<std::string>()(pair.first) // Hash the key
                   + 0x9e3779b9                          // Add the golden ratio prime constant
                   + (seed << 6)                         // Mix the bits of the seed (left shift)
                   + (seed >> 2);                        // Mix the bits of the seed (right shift)
            seed ^= std::hash<std::string>()(pair.second) // Hash the value
                   + 0x9e3779b9                           // Add the golden ratio prime constant
                   + (seed << 6)                          // Mix the bits of the seed (left shift)
                   + (seed >> 2);                         // Mix the bits of the seed (right shift)
        }
        return seed;
    }
};
// Custom equality operator for std::map<std::string, std::string>
struct MapEqual {
    bool operator()(const std::map<std::string, std::string>& lhs, const std::map<std::string, std::string>& rhs) const {
        return lhs == rhs;
    }
};
std::ostream& operator<<(std::ostream& os, const std::unordered_map<
    std::map<std::string, std::string>,
    GribLocation,
    MapHash,
    MapEqual>& umap) {
    for (const auto& entry : umap) {
        const auto& keyMap = entry.first;
        const auto& valueTuple = entry.second;

        os << "Key: {";
        for (const auto& pair : keyMap) {
            os << pair.first << "=" << pair.second << ", ";
        }
        os << "} -> Value: ("
           << std::get<0>(valueTuple) << ", "
           << std::get<1>(valueTuple) << ", "
           << std::get<2>(valueTuple) << ")\n";
    }
    return os;
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

        options_.push_back(new SimpleOption<std::string>("testConfig", "Path to a FDB config"));
        options_.push_back(new SimpleOption<std::string>("referenceConfig", "Path to a second FDB config"));
        options_.push_back(new SimpleOption<std::size_t>("level", "The difference can be evaluated at different levels, 1) Mars Metadata , 2) Mars and Grib Metadata and data up to a defined tolerance (default) "));
        options_.push_back(new SimpleOption<std::string>("gribcomparison", " [eccodes|eccodes_detail|direct] Comparing two Grib messages can be done via eccode (gribcomparison=eccodes (default)) in a bitexact(hashkeys for sections) way, via eccodes keys in the reference FDB (gribcomparison=eccodes_detail)  or directly by directly by comparing for bitexact memory (gribcomparison=direct)" ));
        options_.push_back(new SimpleOption<double>("tolerance", "Floatinng point tolerance for comparison default=machine tolerance epsilon. Tolerance will only be used if level=2 is set otherwise tolerance will have no effect"));
        options_.push_back(new SimpleOption<std::string>("mars_keys_ignore", "Format: \"Key1=Value1,Key2=Value2,...KeyN=ValueN\" All Messages that contain any of the defined key value pairs will be omitted"));
        options_.push_back(new SimpleOption<std::string>("eccodes_keys_select", "Format: \"Key1,Key2,Key3...KeyN\" Only the specified eccodes/grib keys will be compared (Only effective with gribcompare=eccodes_detail)" ));
        options_.push_back(new SimpleOption<std::string>("eccodes_keys_ignore", "Format: \" \" The specified key words will be ignored (only effective with gribcomparison=eccodes_detail)"));
    }

  private: // methods

    virtual void execute(const CmdArgs& args);
    virtual void init(const CmdArgs &args);
    virtual bool gribCompare(const GribLocation& gribLocRef,const GribLocation& gribLocTest, double relativeError, double absoluteError);
    virtual void usage(const std::string& tool) const override;

    std::string testConfig_;
    std::string referenceConfig_;
    std::string gribcomparison_;
    int level_;
    double tolerance_;
    bool detail_; // this parameter should be used to determine if an additional information is required why a two FDBs don't match. 
    std::map<std::string,std::string> mars_keys_ignore_;
    std::unordered_set<std::string> eccodes_keys_select_;
    std::unordered_set<std::string> eccodes_keys_ignore_;
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

    level_ = args.getInt("level",2);
    if(level_<=0 || level_ >3){
        throw UserError("Unknown comparison level "+level_,Here());
    }

    gribcomparison_ = args.getString("gribcomparison","eccodes");
    if(gribcomparison_ != "eccodes" && gribcomparison_ != "direct" && gribcomparison_ != "eccodes_detail"){
        throw UserError("Unknown Grib comparison method "+gribcomparison_, Here());
    }

    tolerance_ = args.getDouble("tolerance", std::numeric_limits<double>::epsilon());
    if(tolerance_<std::numeric_limits<double>::epsilon()){
        throw UserError("The tolerance should be at least machine epsilon to compare floating point values.",Here());
    }
    std::string tmp = args.getString("mars_keys_ignore","");
    if(!tmp.empty()){
        appendKeys(mars_keys_ignore_, tmp);
    }
    tmp = args.getString("eccodes_keys_select","");
    if(!tmp.empty()){
        if(gribcomparison_ != "eccodes_detail"){
            std::cout<<"The specified eccode select keys will have no effect because gribcomparison = "<<gribcomparison_<<" and would need to be eccodes_detail"<<std::endl;
        }
        else{
            appendKeys(eccodes_keys_select_,tmp);
        }    }
    tmp = args.getString("eccodes_keys_ignore","");
    if(!tmp.empty()){
        if(gribcomparison_ != "eccodes_detail"){
            std::cout<<"The specified eccode ignore keys will have no effect because gribcomparison = "<<gribcomparison_<<" and would need to be eccodes_detail"<<std::endl;
        }
        else{
            appendKeys(eccodes_keys_ignore_,tmp);
        }
    }
    
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
    std::cout<<"********************* Reference Grib Message Header **********************************"<<std::endl;
    int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
    std::cout<<"********************* Test Grib Message Header **********************************"<<std::endl;
    dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    codes_dump_content(hTest, stdout, "wmo", dump_flags, NULL);
    std::cout<<"********************** END DEBUG SUMMARY *****************************************"<<std::endl;
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
    std::cout<<"********************* Reference Grib Message Header **********************************"<<std::endl;
    int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
    std::cout<<"********************* Test Grib Message Header **********************************"<<std::endl;
    dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
    codes_dump_content(hTest, stdout, "wmo", dump_flags, NULL);
    std::cout<<"********************** END DEBUG SUMMARY *****************************************"<<std::endl;
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
            std::cout<<"Hash keys don't match for section "<<value<<std::endl;
            std::cout<<"********************* Reference Grib Message Header **********************************"<<std::endl;
            int dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
            codes_dump_content(hRef, stdout, "wmo", dump_flags, NULL);
            std::cout<<"********************* Test Grib Message Header **********************************"<<std::endl;
            dump_flags = CODES_DUMP_FLAG_CODED| CODES_DUMP_FLAG_OCTET | CODES_DUMP_FLAG_VALUES | CODES_DUMP_FLAG_READ_ONLY;
            codes_dump_content(hTest, stdout, "wmo", dump_flags, NULL);
            std::cout<<"********************** END DEBUG SUMMARY *****************************************"<<std::endl;
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
        std::cout<<error<<std::endl;
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
    size_t length=0;
    int err;
    std::string ref = "Reference FDB";
    std::string test=  "Test FDBf";
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

    //std::cout<<name<<std::endl;
    CODES_CHECK(codes_get_native_type(hRef,name,&typeRef),CODES_SUCCESS);
    CODES_CHECK(codes_get_native_type(hTest,name,&typeTest),CODES_SUCCESS);
    if(typeRef != typeTest) {
        std::cout<< std::string(name)<<"Type mismatch: Reference Value"<<value_to_string(hRef,name, typeRef,lenRef)<<" CODES_TYPE " << typeRef<<"\nTest value: "<<value_to_string(hTest,name, typeTest,lenTest)<<" CODES_TYPE " << typeTest<<std::endl;
        return {false,dataSectionMatch}; //DataSectionMismatch is always false unless the difference is cause by the actual values
    }
    if((err=codes_get_size(hRef,name,&lenRef))!=CODES_SUCCESS){
        throw Abort("Error: Cannot get size of "+std::string(name)+"Error message "+std::string(codes_get_error_message(err)));
    }
    if((err=codes_get_size(hTest,name,&lenTest))!=CODES_SUCCESS){
        if(err==CODES_NOT_FOUND){
                 
            std::cout<<name<<" not found in test FDB but found in reference FDB"<<std::endl;
        }
        throw Abort("Error: Cannot get size of "+std::string(name)+"Error message "+std::string(codes_get_error_message(err)));
    }
    if(lenRef != lenTest) {
        std::cout<< std::string(name)<<" Reference Value"<<value_to_string(hRef,name, typeRef,lenRef)<<" Test value: "<<value_to_string(hTest,name, typeTest,lenTest)<<std::endl;
        return {false,dataSectionMatch};
    }

    switch(typeRef){
        case CODES_TYPE_STRING:
            CODES_CHECK(codes_get_length(hRef,name,&lenRef),CODES_SUCCESS);
            CODES_CHECK(codes_get_length(hTest,name,&lenTest),CODES_SUCCESS);
            ASSERT(lenRef==lenTest);
            length = lenRef*sizeof(char);
            break;
        case CODES_TYPE_LONG:
            length = lenRef*sizeof(long);
            break;
        case CODES_TYPE_DOUBLE:
            length = lenRef*sizeof(double);
            break;
        case CODES_TYPE_BYTES:
            length = lenRef*sizeof(unsigned char);
            break;
        default:
            length = 0;
            throw Abort("ECCODES unsupported type "+typeRef);
    }
        
    isMissingRef = ((codes_is_missing(hRef, name, &err) == 1) && (err == 0)) ? true : false;
    isMissingTest = ((codes_is_missing(hTest, name, &err) == 1) && (err == 0)) ? true : false;

    if (isMissingRef && isMissingTest) return {true,dataSectionMatch};
    
    if (isMissingRef) {
        std::cout<<name<<" is set to missing in reference FDB but is not missing in test FDB"<<std::endl;
        return {false,dataSectionMatch};
    }

    if (isMissingTest) {
        std::cout<<name<<" is set to missing in test FDB but is not missing in reference FDB"<<std::endl;
        return {false,dataSectionMatch};
    }

    uvalRef = (unsigned char*)malloc(length);
    uvalTest = (unsigned char*)malloc(length);

    if ((err = grib_get_bytes(hRef, name, uvalRef, &length)) != GRIB_SUCCESS) {
        free(uvalRef);
        free(uvalTest);
        throw Abort("Error Cannot get bytes value of "+std::string(name)+" in reference FDB");
    }

    if ((err = grib_get_bytes(hTest, name, uvalTest, &length)) != GRIB_SUCCESS) {
        free(uvalRef);
        free(uvalTest);
        throw Abort("Error Cannot get bytes value of "+std::string(name)+" in test FDB");
    }
    bool match = true;
    if (memcmp(uvalRef, uvalTest, length) != 0) {
        if(!((std::string(name)=="values")||(std::string(name)=="packedValues")||(std::string(name)=="codedValues")))
        {    
            std::cout<< std::string(name)<<" Reference Value"<<value_to_string(hRef,name, typeRef,lenRef)<<" Test value: "<<value_to_string(hTest,name, typeTest,lenTest)<<std::endl;
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
std::tuple<bool,bool> compare_eccodes(codes_handle* hRef, codes_handle* hTest,T& ignore_list,T& match_list){
    
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
        codes_keys_iterator* key_iter = codes_keys_iterator_new(hRef,CODES_KEYS_ITERATOR_SKIP_COMPUTED,NULL);
        if(!key_iter){
            throw Abort("Error: Eccodes Could not create Iterator",Here());
        }

        while(codes_keys_iterator_next(key_iter)){
            name = codes_keys_iterator_get_name(key_iter);
            if(!name){
                throw Abort("Error: Eccodes could not retrieve name from Key iterator",Here());
            }
            if(ignore_list.find(std::string(name))!=ignore_list.end()) continue;
            auto [match, dataValuesMatch]  = compare_values(hRef,hTest,name);
            if(!match) matchreturn = match;

            if(!dataValuesMatch) dataSectionreturn=dataValuesMatch;
        }

    }
    return {matchreturn,dataSectionreturn};
    
    

}


bool FDBCompare::gribCompare(const GribLocation& gribLocRef,const GribLocation& gribLocTest, double relativeError, double absoluteError)
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
        if(gribcomparison_ == "direct"){
            std::cout<<"direct comparison"<<std::endl;
            //Bitexact comparison directly on the Bytestream begin
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
        }
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
        if(gribcomparison_ == "eccodes"){
            // BitExactComparison with ECCODES begin
            //std::cout<<"eccodes comparison"<<std::endl;
            auto match = compare_header(hRef,hTest);
            if(!match){
                //just called to give a more specific output if the comparison failed
                compare_eccodes(hRef,hTest,eccodes_keys_ignore_,eccodes_keys_select_);
                free(bufferRef);
                free(bufferTest);
            
                codes_handle_delete(hRef);
                codes_handle_delete(hTest);
                std::string msg = "Headers don't match Location reference = " + std::get<0>(gribLocRef) + " offset reference = " + std::to_string(std::get<1>(gribLocRef)) + " length reference = " +std::to_string(std::get<2>(gribLocRef))+" \nLocation test = " + std::get<0>(gribLocTest) + " offset test = " + std::to_string(std::get<1>(gribLocTest)) + " length test = " +std::to_string(std::get<2>(gribLocTest));
                std::cout<<msg<<std::endl;
                
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
            ASSERT(match_md5sums == 2);
        }
        if(gribcomparison_ == "eccodes_detail"){
            auto [match,datasectionMatch] = compare_eccodes(hRef, hTest,eccodes_keys_ignore_,eccodes_keys_select_);
            if(!match) return false;
            else if(datasectionMatch && match) return true;
            ASSERT((datasectionMatch==false) && (match==true));
            //else continue as we have a Mismatch in the data section but all other sections had a match compare the data section numerically 
        }

   
        compare_DataSection(hRef,hTest,relativeError,absoluteError,tolerance_);

        if( relativeError>tolerance_ || absoluteError>tolerance_)
        {
            std::cerr<<"Differenene in Grib message Grib Message failed {Location,offset,length} = " << std::get<0>(gribLocTest) << " , " << std::to_string(std::get<1>(gribLocTest)) << " , " << std::to_string(std::get<2>(gribLocTest))<<std::endl;
            std::cerr<<"Difference in Grib message: Absolute Error = "<<absoluteError<<" relative Error = "<<relativeError<<std::endl;
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
// Function to check if map1 is a subset of map2
template <typename Map>
bool isSubset(const Map& map1, const Map& map2) {
    return std::any_of(map1.begin(), map1.end(), [&map2](const auto& pair) {
        auto it = map2.find(pair.first);
        return it != map2.end() && it->second == pair.second;
    });
}

void assemble_compare_map(FDB& localFDB, std::unordered_map<std::map<std::string,std::string>, GribLocation,MapHash,MapEqual>& umap,const FDBToolRequest& request,const std::map<std::string,std::string>& ignore_keys){
        auto listObject = localFDB.list(request);
        ListElement elem;    
        while (listObject.next(elem)) {
            std::map<std::string,std::string> tmp;
            for(const auto & bit : elem.key()) {
                //bit comes in format "{key1=value1,key2=value2,....,keyN=valueN}
               // std::cout<<bit<<std::endl;
                auto keydict = bit.keyDict();
                tmp.insert(keydict.begin(),keydict.end());
            }
            // if the ignore_keys are a subset of the Mars keys then the entry is ignored and not furhter tested.
            if(ignore_keys.size()==0|| !isSubset(ignore_keys,tmp))
            {
                umap.insert({tmp,{elem.location().uri().path(),static_cast<long long int>(elem.location().offset()),static_cast<long long int>(elem.location().length())}});
            }
            //else{
            //    std::cout<<"Entry: "<<tmp<<std::endl;
            //    std::cout<<"Was ignored because it matched "<<ignore_keys<<std::endl;
           // }
        }
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


    //FDBToolRequest is possible by using the known syntax on the command line and restrict the amount of data that is 
    //retrieved by fdb.list. But in develop we might also want to request individual param ID. that currently results in a
    //Serious bug exception. So even tough the filter is possible we will be able to reduce it further.  
    for (const FDBToolRequest& request : requests()) {
        //Probalby a little bit faster but more error prone with the string assembling
        // std::unordered_map<std::string, std::tuple<std::string,long long int,long long int>> ref_map;
        // std::unordered_map<std::string, std::tuple<std::string,long long int,long long int>> test_map;

        // auto listObject = fdbref.list(request);
        // ListElement elem;    
        // while (listObject.next(elem)) {
        //     std::string str;
        //     for(const auto & bit : elem.key()) {
        //         //std::cout<<bit<<" "<<std::endl;
        //         str.append(bit);
        //         str.append(",");
        //     }
        //     //ßstd::cout<<"-----------------"<<std::endl;
        //     ref_map.insert({str,{elem.location().uri().path(),static_cast<long long int>(elem.location().offset()),static_cast<long long int>(elem.location().length())}});

        // }
        // listObject = fdbtest.list(request);
        // while (listObject.next(elem)) {
        //     std::string str;
        //     for(const auto & bit : elem.key()) {
        //         str.append(bit);
        //         str.append(",");
        //     }
        //     test_map.insert({str,{elem.location().uri().path(),static_cast<long long int>(elem.location().offset()),static_cast<long long int>(elem.location().length())}});
        
        // }


        std::unordered_map<std::map<std::string,std::string>, GribLocation,MapHash,MapEqual> ref_map;
        std::unordered_map<std::map<std::string,std::string>, GribLocation,MapHash,MapEqual> test_map;

        assemble_compare_map(fdbref,ref_map,request,mars_keys_ignore_);
        assemble_compare_map(fdbtest,test_map,request,mars_keys_ignore_);
        // std::cout<<"ref map"<<std::endl;
        // std::cout<<ref_map<<std::endl;
        // std::cout<<ref_map.size()<<std::endl;
        // std::cout<<"TEst map"<<std::endl;
        // std::cout<<test_map<<std::endl;
        // std::cout<<test_map.size()<<std::endl;


        //Check that the keys match only continue with next comparison is this is the case
        if(!(compare_keys(ref_map,test_map))){
            std::cerr<<"Mars Metadata Keys don't match"<<std::endl;
            return;
        }
        // Return if only a comparison of Mars metadata messages was specified as Command Line option
        if(level_ != 2){
            return;
        }
        std::cout<<"Compare Grib messages"<<std::endl;
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
        for (auto& [key, value]:ref_map)
        {
            std::cerr<<"|";

            absoluteError=0.0;
            relativeError=0.0;
            try{
                datamatch = gribCompare(value,test_map[key],relativeError,absoluteError);
            }
            catch(...){
                std::cout<<"Grib Comparison failed Mars Key"<<key<<std::endl;
                throw;
            }
            if(!datamatch){
                std::cout<<"Grib Comaprison failed! Mars Key"<<key<<std::endl;
                return;
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
 
}

void FDBCompare::usage(const std::string &tool) const {

    // derived classes should provide this type of usage information ...

    Log::info() << "FDB Comparison" << std::endl
                << std::endl;

    Log::info() << "Examples:" << std::endl
                << "=========" << std::endl << std::endl
                << tool << " --referenceConfig=<path/to/reference/config.yaml> --testConfig=<path/to/test/config.yaml --minimum-keys=\"\" --all"
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


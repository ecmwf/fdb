#include <string>
#include <unordered_set>

class DataKeys {
public:
    // Check if a given key is a data key
    static bool is_data_key(const std::string& key) {
        return keyset().find(key) != keyset().end();
    }

    // Append all data keys to the provided set
    static void append_data_relevant_keys(std::unordered_set<std::string>& set) {
        set.insert(keyset().begin(), keyset().end());
    }

private:
    static const std::unordered_set<std::string>& keyset() {
        //Set of keys that are changing if the data values change.
        //Depending on the packing type different key words are relevant. 
        static const std::unordered_set<std::string> s = {
            "values",
            "codedValues",
            "packedValues",
            "referenceValue", // simple packing, ccsds
            "binaryScaleFactor", // simple packing, ccsds 
            "decimalScaleFactor", // simple packing, ccsds
            "bitsPerValue", // simple packing, ccsds
            "numberOfValues", // simple packing 
            "numberOfDatapoints", // simple packing
            "ccsdsFlags", //ccsds
            "ccsdsBlockSize", //ccsds
            "ccsdsRsi", // ccsds
            "bitMapIndicator", // bitmap
            "bitmap" // bitmap 
        };
        return s;
    }
};
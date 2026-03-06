/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "CompareBitwise.h"

#include "fdb5/tools/compare/common/Util.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "metkit/codes/api/CodesAPI.h"

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>


namespace compare::grib {

//---------------------------------------------------------------------------------------------------------------------

namespace {


/// Replacement for grib_get_message_headers
struct GribHeaderInfo {
    std::size_t header_size;   // offset to data section
    std::size_t message_size;  // total GRIB message length
    int edition;               // 1 or 2
};


GribHeaderInfo parseGribHeader(const uint8_t* buf, std::size_t size) {
    if (size < 8) {
        throw std::runtime_error("Buffer too small for GRIB");
    }

    if (buf[0] != 'G' || buf[1] != 'R' || buf[2] != 'I' || buf[3] != 'B') {
        throw std::runtime_error("Not a GRIB message");
    }

    int edition = buf[7];

    std::size_t total_len = 0;
    std::size_t offset = 0;

    if (edition == 1) {
        // --- GRIB1 ---
        total_len = readU24Be(buf + 4);
        offset = 8;  // Section 0 length

        // Sections 1–3 (walk until section 4)
        while (offset + 3 <= total_len) {
            uint32_t sec_len = readU24Be(buf + offset);
            uint8_t sec_num = buf[offset + 3];

            if (sec_len == 0) {
                throw std::runtime_error("Invalid GRIB1 section length");
            }

            if (sec_num == 4) {
                // Binary Data Section → header ends here
                return {offset, total_len, edition};
            }

            offset += sec_len;
        }

        throw std::runtime_error("GRIB1 data section not found");
    }
    else if (edition == 2) {
        // --- GRIB2 ---
        if (size < 16) {
            throw std::runtime_error("Buffer too small for GRIB2");
        }

        total_len = readU32Be(buf + 12);
        offset = 16;  // Section 0 length

        // Sections 1–6 (stop at section 7)
        while (offset + 5 <= total_len) {
            uint32_t sec_len = readU32Be(buf + offset);
            uint8_t sec_num = buf[offset + 4];

            if (sec_len == 0) {
                throw std::runtime_error("Invalid GRIB2 section length");
            }

            if (sec_num == 7) {
                // Data Section → header ends here
                return {offset, total_len, edition};
            }

            offset += sec_len;
        }

        throw std::runtime_error("GRIB2 data section not found");
    }
    else {
        throw std::runtime_error("Unknown GRIB edition");
    }
}


}  // namespace

//---------------------------------------------------------------------------------------------------------------------

using namespace eckit;
bool compareHeader(const metkit::codes::CodesHandle& h1, const metkit::codes::CodesHandle& h2) {
    auto data1 = h1.messageData();
    auto data2 = h2.messageData();

    auto refInfo = parseGribHeader(data1.data(), data1.size());
    auto testInfo = parseGribHeader(data2.data(), data2.size());

    if (refInfo.header_size == testInfo.header_size && (0 == memcmp(data1.data(), data2.data(), refInfo.header_size))) {
        return true;
    }
    return false;
}


CompareResult bitComparison(const uint8_t* buffer1, const uint8_t* buffer2, size_t length) {

    std::size_t offset = 0;
    if (!buffer1 || !buffer2) {
        throw BadValue("Buffer corrupted");
    }
    ASSERT(length > 16);  // Needs to be true because those bytes are read without further checking.
    if ((static_cast<char>(buffer1[0]) != 'G') || (static_cast<char>(buffer2[0]) != 'G') ||
        (static_cast<char>(buffer1[1]) != 'R') || (static_cast<char>(buffer2[1]) != 'R') ||
        (static_cast<char>(buffer1[2]) != 'I') || (static_cast<char>(buffer2[2]) != 'I') ||
        (static_cast<char>(buffer1[3]) != 'B') || (static_cast<char>(buffer2[3]) != 'B')) {
        eckit::Log::error()
            << "[GRIB COMPARISON MISMATCH] Message is not a Grib message: Comparing data formats other than Grib "
               "messages is currently not implemented"
            << std::endl;
        return CompareResult::OtherMismatch;
    }

    // EditionNumber is always the 8th Byte in a Grib message
    int editionnumber1 = (int)buffer1[7];
    int editionnumber2 = (int)buffer2[7];

    if (editionnumber1 != editionnumber2) {
        eckit::Log::error() << "[GRIB COMPARISON MISMATCH] Editionnumbers don't match -> cannot compare Grib message"
                            << std::endl;
        return CompareResult::OtherMismatch;
    }
    if (editionnumber1 == 2) {
        // Total Length Value starts at position 8 in the GRIB2 header
        uint64_t totalLength = readU64Be(buffer1 + 8);
        if (length != totalLength) {
            eckit::Log::error() << "[GRIB COMPARISON MISMATCH] Message length (" << length
                                << ") in Mars Key location and total message length (" << totalLength
                                << ") specified "
                                   "in Grib message don't match"
                                << std::endl;
            return CompareResult::OtherMismatch;
        }
        // GRIB2 Section 0 is always 16 Bytes long:
        offset += 16;
    }
    else if (editionnumber1 == 1) {
        uint32_t totalLength = readU24Be(buffer1 + 4);
        if (length != totalLength) {
            eckit::Log::error()
                << "[GRIB COMPARISON MISMATCH] Message length in Mars Key location and total message length specified "
                   "in Grib message don't match"
                << std::endl;
            return CompareResult::OtherMismatch;
        }
        // Grib1 Section 0 is always 8 Bytes long:
        offset += 8;
    }
    else {
        eckit::Log::error() << "[GRIB COMPARISON MISMATCH] Unknown Grib edition number " << editionnumber1 << std::endl;
        return CompareResult::OtherMismatch;
    }
    // Determine number of sections based on the edition number 1 or 2
    int numSections = (editionnumber1 == 1) ? 4 : 7;

    // Process sections based on edition
    for (int i = 0; i < numSections; ++i) {
        // std::cout<<"offset " << offset<<std::endl;
        // Read section length
        uint32_t sectionLength1 = 0;
        uint32_t sectionLength2 = 0;

        const auto errCode = i < (numSections - 1) ? CompareResult::OtherMismatch : CompareResult::DataSectionMismatch;
        if (editionnumber1 == 1) {
            // Grib 1 Read 3 byte section length at the beginning of each section
            if (offset + 3 > length) {
                eckit::Log::error()
                    << "[GRIB COMPARISON MISMATCH] Unexpected end of buffer while reading section length for section "
                    << i + 1 << std::endl;
                return errCode;
            }
            sectionLength1 = readU24Be(buffer1 + offset);
            sectionLength2 = readU24Be(buffer2 + offset);
        }
        else {
            // Grib 2 stores a 4 byte section length value at the beginning of each section
            if (offset + 4 > length) {
                eckit::Log::error()
                    << "[GRIB COMPARISON MISMATCH] Unexpected end of buffer while reading section length for section "
                    << i + 1 << std::endl;
                return errCode;
            }
            sectionLength1 = readU32Be(buffer1 + offset);
            sectionLength2 = readU32Be(buffer2 + offset);
        }
        if (sectionLength1 != sectionLength2) {
            eckit::Log::info() << "[GRIB COMPARISON MISMATCH] The section " << i + 1
                               << " length parameter differs between the grib messages " << std::endl;
            return errCode;
        }
        if (offset + sectionLength1 >= length) {
            eckit::Log::info()
                << "[GRIB COMPARISON MISMATCH] Corrupted grib - sections lengths exceed total length at section "
                << i + 1 << std::endl;
            return errCode;
        }
        // Memory comparison for each section
        if (std::memcmp(buffer1 + offset, buffer2 + offset, sectionLength1) != 0) {
            eckit::Log::info() << "[GRIB COMPARISON MISMATCH] Memory comparison failed in section " << i + 1
                               << std::endl;
            return errCode;
        }
        offset += sectionLength1;
    }

    // Test that the final bit of the message 7777 is present in both
    if (length - offset != 4) {
        eckit::Log::error() << "[GRIB COMPARISON MISMATCH] Message is expected to end with 7777 - remaining length is "
                               "different than 4: "
                            << (length - offset) << std::endl;
        return CompareResult::OtherMismatch;
    }
    for (; offset < length; ++offset) {
        if (static_cast<char>(buffer1[offset]) != '7') {
            eckit::Log::error() << "[GRIB COMPARISON MISMATCH] FDB1 message does not end with 7777" << std::endl;
            return CompareResult::OtherMismatch;
        }
        if (static_cast<char>(buffer2[offset]) != '7') {
            eckit::Log::error() << "[GRIB COMPARISON MISMATCH] FDB2 message does not end with 7777" << std::endl;
            return CompareResult::OtherMismatch;
        }
    }

    return CompareResult::Match;
}

//---------------------------------------------------------------------------------------------------------------------

}  // namespace compare::grib

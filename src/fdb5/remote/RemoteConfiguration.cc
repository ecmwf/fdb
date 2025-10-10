#include "fdb5/remote/RemoteConfiguration.h"

#include <algorithm>
// #include <cstdint>
// #include <mutex>
// #include <string_view>

#include "eckit/config/Configuration.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/Stream.h"
#include "eckit/value/Value.h"

#include "fdb5/LibFdb5.h"
// #include "fdb5/remote/Connection.h"
// #include "fdb5/remote/Messages.h"

namespace {

std::vector<int> intersection(std::vector<int>& v1, std::vector<int>& v2) {

    std::vector<int> v3;

    std::sort(v1.begin(), v1.end());
    std::sort(v2.begin(), v2.end());

    std::set_intersection(v1.begin(), v1.end(), v2.begin(), v2.end(), back_inserter(v3));
    return v3;
}

}  // namespace

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

RemoteConfiguration::RemoteConfiguration(const eckit::Configuration& config) {

    remoteFieldLocationVersions = {1};

    numberOfConnections = config.getIntVector("supportedConnections", {1, 2});
    ASSERT(0 < numberOfConnections.size());
    ASSERT(numberOfConnections[0] == 1 || numberOfConnections[0] == 2);

    ASSERT(numberOfConnections.size() <= 2);
    if (numberOfConnections.size() > 1) {
        ASSERT(numberOfConnections[0] == 1);
        ASSERT(numberOfConnections[1] == 2);
    }

    if (config.has("preferSingleConnection")) {
        preferSingleConnection = config.getBool("preferSingleConnection");
    }
    else {
        preferSingleConnection = std::nullopt;
    }
}

RemoteConfiguration::RemoteConfiguration(eckit::Stream& s) {
    eckit::Value v(s);
    ASSERT(v.isMap());

    if (v.contains("RemoteFieldLocation")) {
        eckit::Value rfl = v["RemoteFieldLocation"];
        ASSERT(rfl.isList());
        remoteFieldLocationVersions.reserve(rfl.size());
        for (size_t i = 0; i < rfl.size(); ++i) {
            remoteFieldLocationVersions.push_back(rfl[i]);
        }
    }
    else {
        remoteFieldLocationVersions = {1};
    }

    if (v.contains("NumberOfConnections")) {
        eckit::Value nc = v["NumberOfConnections"];
        if (nc.isList()) {
            numberOfConnections.reserve(nc.size());
            for (size_t i = 0; i < nc.size(); ++i) {
                numberOfConnections.push_back(nc[i]);
            }
        }
        else {
            ASSERT(nc.isNumber());
            numberOfConnections = std::vector<int>{nc};
        }
    }
    else {
        numberOfConnections = {1, 2};
    }

    if (v.contains("PreferSingleConnection")) {
        eckit::Value psc = v["PreferSingleConnection"];
        ASSERT(psc.isBool());
        preferSingleConnection.emplace(psc);
    }
    else {
        preferSingleConnection = std::nullopt;
    }
}

bool RemoteConfiguration::singleConnection() const {
    return singleConnection_;
}

eckit::Value RemoteConfiguration::get() const {
    eckit::Value val           = eckit::Value::makeOrderedMap();
    val["RemoteFieldLocation"] = eckit::toValue(remoteFieldLocationVersions);
    val["NumberOfConnections"] = eckit::toValue(numberOfConnections);
    if (preferSingleConnection) {
        val["PreferSingleConnection"] = eckit::toValue(preferSingleConnection.value());
    }
    return val;
}

RemoteConfiguration RemoteConfiguration::common(RemoteConfiguration& clientConf, RemoteConfiguration& serverConf) {

    RemoteConfiguration agreedConf{};

    std::vector<int> rflCommon =
        intersection(clientConf.remoteFieldLocationVersions, serverConf.remoteFieldLocationVersions);
    if (rflCommon.empty()) {
        std::stringstream ss;
        ss << "RemoteFieldLocation version not matching." << std::endl
           << "Client supports " << clientConf.remoteFieldLocationVersions << ", server supports "
           << serverConf.remoteFieldLocationVersions;
        throw RemoteConnectionNegotiationException(ss.str(), Here());
    }

    LOG_DEBUG_LIB(LibFdb5) << "Protocol negotiation - RemoteFieldLocation version " << rflCommon.back() << std::endl;
    agreedConf.remoteFieldLocationVersions = {rflCommon.back()};

    // initial implementation was not sending the supported number of connections; set the default {2} for both client
    // and server
    if (clientConf.numberOfConnections.empty()) {
        clientConf.numberOfConnections.push_back(2);
    }
    if (serverConf.numberOfConnections.empty()) {
        serverConf.numberOfConnections.push_back(2);
    }

    // agree on a common functionality by intersecting server and client version numbers
    std::vector<int> ncCommon = intersection(clientConf.numberOfConnections, serverConf.numberOfConnections);
    if (ncCommon.empty()) {
        std::stringstream ss;
        ss << "Number of remote connections not matching." << std::endl
           << "Client supports " << clientConf.numberOfConnections << ", server supports "
           << serverConf.numberOfConnections;
        throw RemoteConnectionNegotiationException(ss.str(), Here());
    }

    int ncSelected = 2;

    if (ncCommon.size() == 1) {
        ncSelected = ncCommon.at(0);
    }
    else {
        ncSelected = ncCommon.back();
        if (clientConf.preferSingleConnection.has_value()) {
            int preferredMode = clientConf.preferSingleConnection.value() ? 1 : 2;
            if (std::find(ncCommon.begin(), ncCommon.end(), preferredMode) != ncCommon.end()) {
                ncSelected = preferredMode;
            }
        }
    }

    LOG_DEBUG_LIB(LibFdb5) << "Protocol negotiation - NumberOfConnections " << ncSelected << std::endl;
    agreedConf.numberOfConnections = {ncSelected};
    agreedConf.singleConnection_   = (ncSelected == 1);

    return agreedConf;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote

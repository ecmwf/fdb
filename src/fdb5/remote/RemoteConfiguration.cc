#include "fdb5/remote/RemoteConfiguration.h"

#include <algorithm>

#include "eckit/config/Configuration.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/Stream.h"
#include "eckit/value/Value.h"

#include "fdb5/LibFdb5.h"

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

    remoteFieldLocationVersions_ = {1};

    numberOfConnections_ = config.getIntVector("supportedConnections", {1, 2});
    ASSERT(0 < numberOfConnections_.size());
    ASSERT(numberOfConnections_[0] == 1 || numberOfConnections_[0] == 2);

    ASSERT(numberOfConnections_.size() <= 2);
    if (numberOfConnections_.size() > 1) {
        ASSERT(numberOfConnections_[0] == 1);
        ASSERT(numberOfConnections_[1] == 2);
    }

    if (config.has("preferSingleConnection")) {
        preferSingleConnection_ = config.getBool("preferSingleConnection");
    }
    else {
        preferSingleConnection_ = std::nullopt;
    }
}

RemoteConfiguration::RemoteConfiguration(eckit::Stream& s) {
    eckit::Value v(s);
    ASSERT(v.isMap());

    if (v.contains("RemoteFieldLocation")) {
        eckit::Value rfl = v["RemoteFieldLocation"];
        ASSERT(rfl.isList());
        remoteFieldLocationVersions_.reserve(rfl.size());
        for (size_t i = 0; i < rfl.size(); ++i) {
            remoteFieldLocationVersions_.push_back(rfl[i]);
        }
    }
    else {
        remoteFieldLocationVersions_ = {1};
    }

    if (v.contains("NumberOfConnections")) {
        eckit::Value nc = v["NumberOfConnections"];
        if (nc.isList()) {
            numberOfConnections_.reserve(nc.size());
            for (size_t i = 0; i < nc.size(); ++i) {
                numberOfConnections_.push_back(nc[i]);
            }
        }
        else {
            ASSERT(nc.isNumber());
            numberOfConnections_ = std::vector<int>{nc};
        }
    }
    else {
        numberOfConnections_ = {1, 2};
    }

    if (v.contains("PreferSingleConnection")) {
        eckit::Value psc = v["PreferSingleConnection"];
        ASSERT(psc.isBool());
        preferSingleConnection_.emplace(psc);
    }
    else {
        preferSingleConnection_ = std::nullopt;
    }
}

bool RemoteConfiguration::singleConnection() const {
    return singleConnection_;
}

eckit::Stream& operator<<(eckit::Stream& s, const RemoteConfiguration& r) {
    eckit::Value val = eckit::Value::makeOrderedMap();
    val["RemoteFieldLocation"] = eckit::toValue(r.remoteFieldLocationVersions_);
    val["NumberOfConnections"] = eckit::toValue(r.numberOfConnections_);
    if (r.preferSingleConnection_) {
        val["PreferSingleConnection"] = eckit::toValue(r.preferSingleConnection_.value());
    }
    s << val;
    return s;
}

RemoteConfiguration RemoteConfiguration::common(RemoteConfiguration& clientConf, RemoteConfiguration& serverConf) {

    RemoteConfiguration agreedConf{};

    std::vector<int> rflCommon =
        intersection(clientConf.remoteFieldLocationVersions_, serverConf.remoteFieldLocationVersions_);
    if (rflCommon.empty()) {
        std::stringstream ss;
        ss << "RemoteFieldLocation version not matching." << std::endl
           << "Client supports " << clientConf.remoteFieldLocationVersions_ << ", server supports "
           << serverConf.remoteFieldLocationVersions_;
        throw RemoteConnectionNegotiationException(ss.str(), Here());
    }

    LOG_DEBUG_LIB(LibFdb5) << "Protocol negotiation - RemoteFieldLocation version " << rflCommon.back() << std::endl;
    agreedConf.remoteFieldLocationVersions_ = {rflCommon.back()};

    // initial implementation was not sending the supported number of connections; set the default {2} for both client
    // and server
    if (clientConf.numberOfConnections_.empty()) {
        clientConf.numberOfConnections_.push_back(2);
    }
    if (serverConf.numberOfConnections_.empty()) {
        serverConf.numberOfConnections_.push_back(2);
    }

    // agree on a common functionality by intersecting server and client version numbers
    std::vector<int> ncCommon = intersection(clientConf.numberOfConnections_, serverConf.numberOfConnections_);
    if (ncCommon.empty()) {
        std::stringstream ss;
        ss << "Number of remote connections not matching." << std::endl
           << "Client supports " << clientConf.numberOfConnections_ << ", server supports "
           << serverConf.numberOfConnections_;
        throw RemoteConnectionNegotiationException(ss.str(), Here());
    }

    int ncSelected = 2;

    if (ncCommon.size() == 1) {
        ncSelected = ncCommon.at(0);
    }
    else {
        ncSelected = ncCommon.back();
        if (clientConf.preferSingleConnection_.has_value()) {
            int preferredMode = clientConf.preferSingleConnection_.value() ? 1 : 2;
            if (std::find(ncCommon.begin(), ncCommon.end(), preferredMode) != ncCommon.end()) {
                ncSelected = preferredMode;
            }
        }
    }

    LOG_DEBUG_LIB(LibFdb5) << "Protocol negotiation - NumberOfConnections " << ncSelected << std::endl;
    agreedConf.numberOfConnections_ = {ncSelected};
    agreedConf.singleConnection_ = (ncSelected == 1);

    return agreedConf;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote

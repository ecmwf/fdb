/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/remote/server/AvailablePortList.h"

#include <netdb.h>
#include <unistd.h>
#include <fstream>
#include <mutex>

#include "eckit/exception/Exceptions.h"
#include "eckit/runtime/ProcessControler.h"
#include "eckit/utils/Literals.h"
#include "eckit/utils/Tokenizer.h"
#include "eckit/utils/Translator.h"

#include "fdb5/LibFdb5.h"

using namespace eckit;
using namespace eckit::literals;

namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------


/// Read a list of ports to skip from /etc/services

std::set<int> readServices() {

    std::set<int> portsToSkip;

    PathName servicesFile = "/etc/services";
    if (!servicesFile.exists()) {
        return portsToSkip;
    }

    std::ifstream in(servicesFile.localPath());

    LOG_DEBUG_LIB(LibFdb5) << "Scanning for ports to avoid in services file " << servicesFile << std::endl;

    if (!in) {
        Log::error() << servicesFile << Log::syserr << std::endl;
        return portsToSkip;
    }

    eckit::Tokenizer parse("\t /");

    eckit::Translator<std::string, int> toInt;

    char line[1_KiB];
    while (in.getline(line, sizeof(line))) {

        std::vector<std::string> s;
        parse(line, s);

        size_t i = 0;
        while (i < s.size()) {
            if (s[i][0] == '#') {
                s.erase(s.begin() + i, s.end());
            }
            else if (s[i].length() == 0) {
                s.erase(s.begin() + i);
            }
            else {
                i++;
            }
        }

        if (s.size() == 0) {
            continue;
        }

        if (s.size() >= 3) {

            const std::string& sname = s[0];
            const std::string& port = s[1];
            const std::string& protocol = s[2];

            LOG_DEBUG_LIB(LibFdb5) << "Skipping port " << port << " service " << sname << " protocol " << protocol
                                   << std::endl;

            int p = toInt(port);
            portsToSkip.insert(p);
        }
        else {
            eckit::Log::warning() << "Unrecognized format in " << servicesFile << " line: [" << line << "]"
                                  << std::endl;
        }
    }

    return portsToSkip;
}


//----------------------------------------------------------------------------------------------------------------------

AvailablePortList::AvailablePortList(int startPort, size_t count) :
    shared_("~fdb/etc/fdb/serverPortLock", "/fdb-server-data-ports", count), startPort_(startPort), count_(count) {}

void AvailablePortList::initialise() {

    std::lock_guard<decltype(shared_)> lock(shared_);

    // Only initialise if any of the entries are not already initialised.

    bool initialised = true;
    for (const Entry& e : shared_) {
        if (e.port == 0) {
            initialised = false;
            break;
        }
    }
    if (initialised) {
        return;
    }

    // Get a list of everything that needs to be skipped

    std::set<int> portsToSkip = readServices();

    size_t foundCount = 0;
    int port = startPort_;

    eckit::Log::info() << "Initialising port list." << std::endl;
    while (foundCount < count_) {

        bool found = true;
        if (portsToSkip.find(port) != portsToSkip.end()) {
            found = false;
        }

        //        if (found && getservbyport(port, 0) != 0) found = false;

        if (found) {
            shared_[foundCount].port = port;
            shared_[foundCount].pid = 0;
            shared_[foundCount].deadTime = 0;
            foundCount++;
        }
        port++;
    }
    eckit::Log::info() << "Port list initialised" << std::endl;

    shared_.sync();
}


int AvailablePortList::acquire() {

    std::lock_guard<decltype(shared_)> lock(shared_);

    ASSERT(shared_.begin()->port != 0);

    pid_t pid = ::getpid();

    for (auto it = shared_.begin(); it != shared_.end(); ++it) {
        if (it->pid == 0) {
            it->pid = pid;
            it->deadTime = 0;
            shared_.sync();
            if (::getservbyport(it->port, 0) == 0) {
                eckit::Log::info() << "Acquiring port: " << it->port << std::endl;
                return it->port;
            }
            else {
                eckit::Log::info() << "Port " << it->port << " already belongs to a service."
                                   << " Marking as in use, and trying another." << std::endl;
            }
        }
    }

    throw eckit::SeriousBug("Data ports exhausted", Here());
}

void AvailablePortList::reap(int deadTime) {

    std::lock_guard<decltype(shared_)> lock(shared_);

    time_t now;
    SYSCALL(now = time(0));

    ASSERT(shared_.begin()->port != 0);

    for (auto it = shared_.begin(); it != shared_.end(); ++it) {

        if (it->deadTime != 0) {

            ASSERT(it->pid != 0);
            ASSERT(it->deadTime <= now);
            ASSERT(!ProcessControler::isRunning(it->pid));
            if ((now - it->deadTime) >= deadTime) {
                it->pid = 0;
                it->deadTime = 0;
            }
        }
        else if (it->pid != 0) {

            ASSERT(it->deadTime == 0);
            if (!ProcessControler::isRunning(it->pid)) {
                it->deadTime = now;
            }
        }
    }

    shared_.sync();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace remote
}  // namespace fdb5

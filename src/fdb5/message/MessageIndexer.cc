/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"
#include "eckit/log/Progress.h"
#include "eckit/log/Seconds.h"
#include "eckit/log/Timer.h"

#include "eckit/message/Message.h"
#include "eckit/message/Reader.h"

#include "fdb5/message/MessageIndexer.h"
#include "fdb5/toc/AdoptVisitor.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MessageIndexer::MessageIndexer(bool checkDuplicates) : MessageDecoder(checkDuplicates) {}

void MessageIndexer::index(const eckit::PathName& path) {
    eckit::Timer timer("fdb::service::archive");

    eckit::message::Reader reader(path);

    size_t count             = 0;
    eckit::Length total_size = 0;

    eckit::Length totalFileSize = path.size();

    eckit::Progress progress("Scanning", 0, totalFileSize);

    eckit::PathName full(path.realName());

    eckit::message::Message msg;
    while ((msg = reader.next())) {

        Key key;

        messageToKey(msg, key);

        // eckit::Log::info() << key << std::endl;

        eckit::Length length = msg.length();
        eckit::Offset offset = reader.position() - length;

        AdoptVisitor visitor{*this, key, full, offset, length};
        archive(key, visitor);

        total_size += length;
        progress(total_size);
        count++;
    }

    eckit::Log::info() << "FDB indexer " << eckit::Plural(count, "message") << ","
                       << " size " << eckit::Bytes(total_size) << ","
                       << " in " << eckit::Seconds(timer.elapsed()) << " (" << eckit::Bytes(total_size, timer) << ")"
                       << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

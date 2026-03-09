/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <iomanip>

#include "eccodes.h"

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"
#include "eckit/message/Message.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/message/MessageArchiver.h"
#include "fdb5/tools/FDBVisitTool.h"

#include "metkit/codes/CodesDataContent.h"
#include "metkit/codes/api/CodesAPI.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class PatchArchiver : public MessageArchiver {

public:  // methods

    explicit PatchArchiver(const Key& key) : key_(key) {}

private:  // methods

    eckit::message::Message patch(const eckit::message::Message& msg) override;

private:  // members

    const Key& key_;
};

eckit::message::Message PatchArchiver::patch(const eckit::message::Message& msg) {
    auto h = metkit::codes::codesHandleFromMessage({reinterpret_cast<const uint8_t*>(msg.data()), msg.length()});

    for (const auto& [key, value] : key_) {
        h->set(key, value);
    }

    return eckit::message::Message(new metkit::codes::CodesDataContent(std::move(h), msg.offset()));
}

//----------------------------------------------------------------------------------------------------------------------

class FDBPatch : public FDBVisitTool {

public:  // methods

    FDBPatch(int argc, char** argv) : FDBVisitTool(argc, argv, "class,expver,stream,date,time") {

        options_.push_back(new SimpleOption<std::string>("expver", "Set the expver"));
        options_.push_back(new SimpleOption<std::string>("class", "Set the class"));
    }

private:  // methods

    virtual void execute(const CmdArgs& args);
    virtual void init(const CmdArgs& args);
    virtual int minimumPositionalArguments() const;

private:  // members

    fdb5::Key key_;
};


int FDBPatch::minimumPositionalArguments() const {
    return 1;
}

void FDBPatch::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    std::string s;

    if (args.get("expver", s)) {
        std::ostringstream oss;
        oss << std::setfill('0') << std::setw(4) << s;
        key_.set("expver", oss.str());
    }

    if (args.get("class", s)) {
        key_.set("class", s);
    }

    if (key_.empty()) {
        Log::info() << "Please provide either --expver or --class" << std::endl;
        exit(1);
    }
}

void FDBPatch::execute(const CmdArgs& args) {


    FDB fdb(config(args));


    Timer timer(args.tool());

    std::set<Key> uniqueKeys;

    for (const FDBToolRequest& request : requests()) {

        // 1. List keys corresponding with the given FDB

        ListIterator listIter = fdb.list(request);

        size_t count = 0;
        ListElement elem;
        while (listIter.next(elem)) {

            // 2. Get DataHandle to data to retrieve
            //    (n.b. listed key is broken down as-per the schema)

            Key key;
            for (const Key& k : elem.keys()) {
                for (const auto& kv : k) {
                    key.set(kv.first, kv.second);
                }
            }

            uniqueKeys.insert(key);
            count++;
        }

        if (count == 0 && fail()) {
            std::ostringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }

    // Construct a retrieve data handle

    HandleGatherer handles(false);
    for (const Key& key : uniqueKeys) {
        metkit::mars::MarsRequest rq("retrieve", key.keyDict());
        handles.add(fdb.retrieve(rq));
    }

    // Do retrieve-patch-archive

    std::unique_ptr<DataHandle> handle(handles.dataHandle());

    PatchArchiver archiver(key_);

    Length bytes = archiver.archive(*handle);

    // Status report

    Log::info() << std::endl << "Summary" << std::endl << "=======" << std::endl << std::endl;
    Log::info() << Plural(uniqueKeys.size(), "field") << " (" << Bytes(bytes) << ") copied to " << key_ << std::endl;

    Log::info() << "Rates: " << Bytes(bytes, timer) << ", " << uniqueKeys.size() / timer.elapsed() << " fields/s"
                << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace tools
}  // namespace fdb5

int main(int argc, char** argv) {
    fdb5::tools::FDBPatch app(argc, argv);
    return app.start();
}

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date Nov 2016

#ifndef fdb5_FieldLocation_H
#define fdb5_FieldLocation_H

#include <eckit/filesystem/URI.h>
#include <memory>

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/memory/Owned.h"
#include "eckit/serialisation/Streamable.h"

#include "fdb5/database/Key.h"

namespace eckit {
class DataHandle;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FieldLocationVisitor;

class FieldLocation : public eckit::OwnedLock, public eckit::Streamable {
public:  // methods

    FieldLocation() : offset_(eckit::Offset(0)), length_(eckit::Length(0)), remapKey_(Key()) {}
    FieldLocation(const eckit::URI& uri);
    FieldLocation(const eckit::URI& uri, eckit::Offset offset, eckit::Length length, const Key& remapKey);
    FieldLocation(eckit::Stream&);

    FieldLocation(const FieldLocation&)            = delete;
    FieldLocation& operator=(const FieldLocation&) = delete;

    virtual const eckit::URI& uri() const { return uri_; }
    eckit::URI fullUri() const;
    std::string host() const { return uri_.hostport(); }
    virtual eckit::Offset offset() const { return offset_; }
    virtual eckit::Length length() const { return length_; }
    const Key& remapKey() const { return remapKey_; }

    virtual eckit::DataHandle* dataHandle() const = 0;

    /// Create a (shared) copy of the current object, for storage in a general container.
    virtual std::shared_ptr<const FieldLocation> make_shared() const = 0;

    virtual std::shared_ptr<const FieldLocation> stableLocation() const { return make_shared(); }

    virtual void visit(FieldLocationVisitor& visitor) const = 0;

    virtual void dump(std::ostream& out) const;

private:  // methods

    virtual void print(std::ostream& out) const;

protected:  // For Streamable

    void encode(eckit::Stream&) const override;

    static eckit::ClassSpec classSpec_;

protected:  // members

    eckit::URI uri_;
    eckit::Offset offset_;
    eckit::Length length_;
    Key remapKey_;

private:  // friends

    friend std::ostream& operator<<(std::ostream& s, const FieldLocation& x) {
        x.print(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

/// A self-registering factory for producing FieldLocation instances.

//----------------------------------------------------------------------------------------------------------------------

class FieldLocationBuilderBase {
    std::string name_;

public:

    FieldLocationBuilderBase(const std::string&);
    virtual ~FieldLocationBuilderBase();
    virtual FieldLocation* make(const eckit::URI& uri) = 0;
    virtual FieldLocation* make(const eckit::URI& uri, eckit::Offset offset, eckit::Length length,
                                const Key& remapKey)   = 0;
};

template <class T>
class FieldLocationBuilder : public FieldLocationBuilderBase {
    FieldLocation* make(const eckit::URI& uri) override { return new T(uri); }
    FieldLocation* make(const eckit::URI& uri, eckit::Offset offset, eckit::Length length,
                        const Key& remapKey) override {
        return new T(uri, offset, length, remapKey);
    }

public:

    FieldLocationBuilder(const std::string& name) : FieldLocationBuilderBase(name) {}
    virtual ~FieldLocationBuilder() = default;
};

class FieldLocationFactory {
public:

    static FieldLocationFactory& instance();

    void add(const std::string& name, FieldLocationBuilderBase* builder);
    void remove(const std::string& name);

    bool has(const std::string& name);
    void list(std::ostream&);

    /// @returns a specialized FieldLocation built by specified builder
    FieldLocation* build(const std::string&, const eckit::URI&);
    FieldLocation* build(const std::string&, const eckit::URI&, eckit::Offset offset, eckit::Length length,
                         const Key& remapKey);

private:

    FieldLocationFactory();

    std::map<std::string, FieldLocationBuilderBase*> builders_;
    eckit::Mutex mutex_;
};

//----------------------------------------------------------------------------------------------------------------------


class FieldLocationVisitor : private eckit::NonCopyable {

public:  // methods

    virtual ~FieldLocationVisitor();

    virtual void operator()(const FieldLocation& location) = 0;
};

class FieldLocationPrinter : public FieldLocationVisitor {
public:

    FieldLocationPrinter(std::ostream& out) : out_(out) {}
    virtual void operator()(const FieldLocation& location);

private:

    std::ostream& out_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

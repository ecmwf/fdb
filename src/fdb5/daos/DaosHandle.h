/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
///
/// @date Jul 2022

#pragma once
// TODO: pragma once everywhere

#include "eckit/io/DataHandle.h"
#include "eckit/filesystem/URI.h"

#include <daos.h>

// TODO: never do that
using eckit::Length;
using eckit::Offset;
using eckit::URI;

// TODO: comment separators and labels
namespace fdb5 {

// TODO: really necessary?
// TODO: naming? oid_to_string
std::string oidToStr(const daos_obj_id_t&);
bool strToOid(const std::string&, daos_obj_id_t*);

class DaosHandle : public eckit::DataHandle {

public:

    // TODO: please include relevant names
    // TODO: add classes for daos pool, cont and object
    DaosHandle(std::string&, std::string&);
    DaosHandle(std::string&, std::string&, std::string&);
    DaosHandle(std::string&);
    DaosHandle(URI&);

    ~DaosHandle();

    virtual void print(std::ostream& s) const override;

    virtual Length openForRead() override;
    virtual void openForWrite(const Length&) override;
    virtual void openForAppend(const Length&) override;

    virtual long read(void*, long) override;
    virtual long write(const void*, long) override;
    virtual void close() override;
    virtual void flush() override;

    virtual Length size() override;
    // TODO: implement this one as size()
    //virtual Length estimate() override;
    virtual Offset position() override;
    virtual Offset seek(const Offset&) override;
    virtual bool canSeek() const override;
    virtual void skip(const Length&) override;

    //virtual void rewind() override;
    //virtual void restartReadFrom(const Offset&) override;
    //virtual void restartWriteFrom(const Offset&) override;

    virtual std::string title() const override;

    //virtual void encode(Stream&) const override;
    //virtual const ReanimatorBase& reanimator() const override { return reanimator_; }

    //static const ClassSpec& classSpec() { return classSpec_; }

private:

    std::string pool_;
    std::string cont_;
    daos_obj_id_t oid_;
    daos_handle_t poh_;
    daos_handle_t coh_;
    daos_handle_t oh_;
    bool open_;
    eckit::Offset offset_;

    void construct(std::string&);

    //static ClassSpec classSpec_;
    //static Reanimator<DataHandle> reanimator_;

};

}  // namespace fdb5
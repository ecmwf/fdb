/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   Mar 2018

#ifndef fdb5_api_FDBFactory_H
#define fdb5_api_FDBFactory_H

#include "fdb5/config/Config.h"

#include "eckit/utils/Regex.h"

#include <memory>

class MarsRequest;


namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

/// The base class that FDB implementations are derived from

class FDBBase {

public: // methods

    FDBBase(const Config& config);
    virtual ~FDBBase();

    virtual void archive(const Key& key, const void* data, size_t length) = 0;

    virtual eckit::DataHandle* retrieve(const MarsRequest& request) = 0;

    /// ID used for hashing in the Rendezvous hash. Should be unique amongst those used
    /// within a DistFDB (i.e. within one Rendezvous hash).
    virtual std::string id() const = 0;

    virtual void flush() = 0;

    bool writable();
    bool visitable();

    void setNonWritable();

private: // methods

    virtual void print(std::ostream& s) const = 0;

    friend std::ostream& operator<<(std::ostream& s, const FDBBase& f) {
        f.print(s);
        return s;
    }

protected: // members

    Config config_;

    bool writable_;
    bool visitable_;
};

//----------------------------------------------------------------------------------------------------------------------


class FDBFactory {

protected: // methods

    FDBFactory(const std::string& name);
    ~FDBFactory();

public: // methods

    static std::unique_ptr<FDBBase> build(const Config& config);

private: // methods

    virtual std::unique_ptr<FDBBase> make(const Config& config) const = 0;

private: // members

    std::string name_;
};



template <typename T>
class FDBBuilder : public FDBFactory {

    static_assert(std::is_base_of<FDBBase, T>::value, "FDB Factorys can only build implementations of the FDB interface");

public: // methods

    FDBBuilder(const std::string& name) :
        FDBFactory(name) {}

private: // methods

    virtual std::unique_ptr<FDBBase> make(const Config& config) const {
        // TODO: C++14
//        return std::make_unique<T>(config);
        return std::unique_ptr<T>(new T(config));
    }
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_api_FDBFactory_H

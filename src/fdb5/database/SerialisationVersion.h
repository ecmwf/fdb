/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   SerialisationVersion.h
/// @author Emanuele Danovaro
/// @date   January 2022

#pragma once

namespace fdb5 {

/// Manages the serialisation version for FDB objects on storage
/// This object is copyable
class SerialisationVersion {
public:
    SerialisationVersion();

    /// Defines the serialisation versions the software is able to handle
    /// Entries with these versions will not issue errors
    virtual std::vector<unsigned int> supported() const = 0;

    /// Latest version of serialisation the software is capable to create
    /// To be used as default
    virtual unsigned int latest() const = 0;

    /// Default version of serialisation the software will use
    /// This may be behind the latest version
    virtual unsigned int defaulted() const = 0;

    /// List of supported versions as a string
    std::string supportedStr() const;

    /// Defines the serialisation version the software is meant to create new entries with
    /// Normally is the latest()
    /// But can be overriden by the variable FDB5_SERIALISATION_VERSION
    unsigned int used() const;

    /// Checks the serialisation version is supported by the software
    bool check(unsigned int version, bool throwOnFail = true) const;

protected:
    unsigned int used_; //< version to be used for serialisation on write
};

}

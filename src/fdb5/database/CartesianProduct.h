/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "eckit/exception/Exceptions.h"

#include <functional>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

//template<typename T>
//struct has_value_type {
//    typedef int8_t  yes;
//    typedef int16_t no;
//
//    template <typename S> static yes test(typename S::value_type);
//    template <typename S> static no  test(...);
//public:
//    static const bool value = sizeof(test<T>(0)) == sizeof(yes);
//};

namespace detail {

    template <typename T, typename=void>
    struct has_value_type : std::false_type {};

    template <typename T>
    struct has_value_type<T, std::void_t<typename T::value_type>> : std::true_type {};

    template <typename T>
    struct is_container : std::conjunction<has_value_type<T>, std::negation<std::is_same<T, std::string>>> {};

}


//----------------------------------------------------------------------------------------------------------------------

// Implementation for purely scalars
// --> Just return the set

template <typename TContainer, typename OutputT=void, typename=void>
class CartesianProduct {
public: // types
    using value_type = TContainer;
    using output_type = typename std::conditional<std::is_same<OutputT, void>::value, value_type, OutputT>::type;
public: // methods
    void append(const value_type& input, output_type& output) { output = input; }
    bool next() {
        returned_ = !returned_;
        return returned_;
    }
private: // members
    bool returned_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

// Proper implementation

template <typename TContainer, typename OutputT>
class CartesianProduct <TContainer, OutputT, std::void_t<std::enable_if_t<detail::is_container<TContainer>::value>>> {

public: // types

    using value_type = typename TContainer::value_type;
    using output_type = typename std::conditional<std::is_same<OutputT, void>::value, value_type, OutputT>::type;

public: // methods

    void append(const TContainer& input, output_type& output);
    void append(const value_type& input, output_type& output);

    bool next();

private: // members

    std::vector<std::reference_wrapper<const TContainer>> inputs_;
    std::vector<std::reference_wrapper<output_type>> output_;

    std::vector<size_t> indices_;
    bool haveScalars_ = false;
    bool returned_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

//template <typename TContainer, typename TOutput>
//CartesianProduct<TContainer, TOutput>::CartesianProduct(TOutput& output) : output_(output) {}

template <typename TContainer, typename OutputT>
void CartesianProduct<TContainer, OutputT, std::void_t<std::enable_if_t<detail::is_container<TContainer>::value>>>::append(const TContainer& input, output_type& output) {
    if (input.empty()) throw eckit::BadValue("Got empty list in cartesian product", Here());
    if (input.size() == 1) {
        append(input[0], output);
    } else {
        inputs_.push_back(std::cref(input));
        output_.push_back(std::ref(output));
    }
}

template <typename TContainer, typename OutputT>
void CartesianProduct<TContainer, OutputT, std::void_t<std::enable_if_t<detail::is_container<TContainer>::value>>>::append(const value_type& input, output_type& output) {
    output = input;
    haveScalars_ = true;
}

template <typename TContainer, typename OutputT>
bool CartesianProduct<TContainer, OutputT, std::void_t<std::enable_if_t<detail::is_container<TContainer>::value>>>::next() {

    // If this is the first iteration, initialise

    if (indices_.empty()) {
        if (inputs_.empty()) {
            if (haveScalars_) {
                returned_ = !returned_;
                return returned_;
            } else {
                throw eckit::SeriousBug("Cannot generate cartesian product from empty", Here());
            }
        }
        ASSERT(inputs_.size() == output_.size());
        indices_.resize(inputs_.size(), 0);
        for (int depth = 0; depth < inputs_.size(); ++depth) {
            output_[depth].get() = inputs_[depth].get()[0];
        }
        return true;
    }

    int depth;
    for (depth = inputs_.size()-1; depth >= 0; --depth) {
        indices_[depth]++;
        if (indices_[depth] < inputs_[depth].get().size()) {
            output_[depth].get() = inputs_[depth].get()[indices_[depth]];
            return true;
        } else {
            indices_[depth] = 0;
            output_[depth].get() = inputs_[depth].get()[indices_[depth]];
        }
    }

    indices_.clear();
    return false;
}

//----------------------------------------------------------------------------------------------------------------------

}

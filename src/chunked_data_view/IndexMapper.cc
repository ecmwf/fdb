#include "IndexMapper.h"
#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {


/**
 * @brief This function maps the axis indices to the buffer offset. Each index is the position in a 
 * axis object (either compound or normal axis). For chunked axis the axis has to contribution to the
 * buffer index because for each entry we send a request to the FDB.
 *
 * @param indices indices in the axes of the view
 * @param axes the axes
 * @return index in the buffer (an offset)
 */
size_t index_mapping::axis_index_to_buffer_index(const std::vector<size_t>& indices, const std::vector<Axis>& axes) {

    ASSERT(indices.size() == axes.size());

    size_t prod = 1;
    size_t index = 0;

    for (int i = axes.size() - 1; i >= 0; --i) {

        if(!axes[i].isChunked()){
            index += indices[i] * prod;
            prod *= axes[i].size();
        }

    }

    return index;
}

std::vector<size_t> index_mapping::delinearize(const size_t& index, const Axis& axis) {

    if (index >= axis.size()) {
        throw eckit::Exception("Index is out of bound for axis.");
    }

    if (axis.parameters().size() == 1) {
        return {index};
    }

    const auto dimCount = axis.parameters().size();
    std::vector<size_t> result(dimCount, 0);

    size_t chunk_index = index;

    // Idea: All dimension on the right side of the current index are multiplied and subtracted
    // from the current index. The division in (1) computes the current index. (2) computes the
    // remainder and afterwards we continue with the next index.
    for (size_t i = 0; i < dimCount - 1; ++i) {
        size_t dim_prod = 1;

        for (size_t j = i + 1; j < dimCount; ++j) {
            dim_prod *= axis.parameters()[j].values().size();
        }

        size_t index_in_dim = chunk_index / dim_prod;  // (1)
        result[i]           = index_in_dim;
        chunk_index -= index_in_dim * dim_prod;  // (2)
    }

    // Compute the final remainder
    result[dimCount - 1] = chunk_index % axis.parameters()[dimCount - 1].values().size();

    return result;
}

size_t index_mapping::linearize(const std::vector<size_t>& indices, const Axis& axis) {

    ASSERT(indices.size() == axis.parameters().size());

    for (size_t i = 0; i < indices.size(); ++i) {
        if (indices[i] >= axis.parameters()[i].values().size()) {
            throw eckit::Exception("Out of bound exception: Index is out of bound for axis parameter");
        }
    }

    if (indices.size() == 1) {
        return indices[0];
    }

    size_t prod = 1;
    size_t index = 0;

    for (int i = axis.parameters().size() - 1; i >= 0; --i) {

        index += indices[i] * prod;
        prod *= axis.parameters()[i].values().size();

    }

    return index;
}

std::vector<size_t> index_mapping::indexInAxisParameters(const Axis& axes, const fdb5::Key& key) {

    // Sanity check whether the key is containing information about the axis
    const std::vector<std::string> keys = key.names();

    std::vector<size_t> result;

    for (const auto& param : axes.parameters()) {

        auto [it, success] = key.find(param.name());

        if (!success) {
            throw eckit::Exception("Couldn't find the parameter name in the keys of the request.");
        }

        // Find the index of the key value in the axis
        auto res = std::find(param.values().begin(), param.values().end(), it->second);

        if (res == param.values().end()) {
            throw eckit::Exception("Couldn't request's key value in the axis.");
        }

        size_t index = std::distance(param.values().begin(), res);
        result.push_back(index);
    }

    return result;
}

};  // namespace chunked_data_view

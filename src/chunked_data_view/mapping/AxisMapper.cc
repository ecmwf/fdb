
#include "chunked_data_view/mapping/AxisMapper.h"
#include "eckit/exception/Exceptions.h"
#include "metkit/mars/MarsRequest.h"

namespace chunked_data_view {

std::vector<Axis> AxisMapper::mapRequestToAxis(const metkit::mars::MarsRequest& mars_request,
                                               const std::vector<AxisDefinition>& axis_defintion) {

    std::vector<Axis> resulting_axes;

    std::set<std::string> processedKeywords{};
    for (const auto& axis : axis_defintion) {

        std::vector<Parameter> parameters{};
        parameters.reserve(axis.keys.size());
        for (const auto& key : axis.keys) {
            if (processedKeywords.count(key) != 0) {
                throw eckit::UserError("ViewPart::ViewPart:Keyword already mapped by another axis");
            }
            processedKeywords.insert(key);
            parameters.emplace_back(std::make_tuple(key, mars_request.values(key)));
        }
        // NOTE(TKR): Extend here for configurable number of fields per chunk
        resulting_axes.emplace_back(parameters,
                                    std::holds_alternative<AxisDefinition::IndividualChunking>(axis.chunking));
    }

    for (const auto& p : mars_request.parameters()) {
        if (p.count() > 1 && processedKeywords.count(p.name()) != 1) {
            std::ostringstream ss;
            ss << "ViewPart::ViewPart:Keyword " << p.name() << " has " << p.count()
               << " values but is not mapped by an axis.";
            throw eckit::UserError(ss.str());
        }
    }

    return resulting_axes;
}

}  // namespace chunked_data_view

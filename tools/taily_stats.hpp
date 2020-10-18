#pragma once

#include <gsl/span>

#include "app.hpp"
#include "pisa/taily_stats.hpp"
#include "pisa/util/compiler_attribute.hpp"

namespace pisa {

template <typename Wand>
void extract_taily_stats(
    std::string const& wand_data_path,
    ScorerParams const& scorer_params,
    pisa::binary_freq_collection const& collection,
    std::string const& output_path)
{
    Wand wdata(pisa::MemorySource::mapped_file(wand_data_path));
    auto term_stats =
        pisa::extract_feature_stats(collection, pisa::scorer::from_params(scorer_params, wdata));
    pisa::write_feature_stats(term_stats, collection.num_docs(), output_path);
}

void extract_taily_stats(TailyStatsArgs const& args)
{
    pisa::binary_freq_collection collection(args.collection_path().c_str());
    if (args.is_wand_compressed()) {
        extract_taily_stats<wand_data<wand_data_compressed<>>>(
            args.wand_data_path(), args.scorer_params(), collection, args.output_path());
    } else {
        extract_taily_stats<wand_data<wand_data_raw>>(
            args.wand_data_path(), args.scorer_params(), collection, args.output_path());
    }
}

}  // namespace pisa

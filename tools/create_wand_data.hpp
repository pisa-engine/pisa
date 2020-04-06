#pragma once

#include <spdlog/spdlog.h>

#include "app.hpp"
#include "binary_collection.hpp"
#include "binary_freq_collection.hpp"
#include "mappable/mapper.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_range.hpp"
#include "wand_data_raw.hpp"

#include "app.hpp"

namespace pisa {

void create_wand_data(CreateWandDataArgs const& args)
{
    auto const block_size = args.block_size();
    auto const dropped_term_ids = args.dropped_term_ids();
    spdlog::info("Dropping {} terms", dropped_term_ids.size());

    std::string partition_type_name = (args.lambda()) ? "variable partition" : "static partition";
    spdlog::info("Block based wand creation with {}", partition_type_name);

    binary_collection sizes_coll((args.input_basename() + ".sizes").c_str());
    binary_freq_collection coll(args.input_basename().c_str());

    if (args.compress()) {
        wand_data<wand_data_compressed<>> wdata(
            sizes_coll.begin()->begin(),
            coll.num_docs(),
            coll,
            args.scorer(),
            block_size,
            args.quantize(),
            dropped_term_ids);
        mapper::freeze(wdata, args.output().c_str());
    } else if (args.range()) {
        wand_data<wand_data_range<128, 1024>> wdata(
            sizes_coll.begin()->begin(),
            coll.num_docs(),
            coll,
            args.scorer(),
            block_size,
            args.quantize(),
            dropped_term_ids);
        mapper::freeze(wdata, args.output().c_str());
    } else {
        wand_data<wand_data_raw> wdata(
            sizes_coll.begin()->begin(),
            coll.num_docs(),
            coll,
            args.scorer(),
            block_size,
            args.quantize(),
            dropped_term_ids);
        mapper::freeze(wdata, args.output().c_str());
    }
}

}  // namespace pisa

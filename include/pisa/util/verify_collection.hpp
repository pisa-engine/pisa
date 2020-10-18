#pragma once

#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "util/util.hpp"

namespace pisa {

template <typename InputCollection, typename Collection>
void verify_collection(InputCollection const& input, const char* filename)
{
    Collection coll;
    auto source = MemorySource::mapped_file(boost::filesystem::path(filename));
    pisa::mapper::map(coll, source.data());
    size_t size = 0;
    spdlog::info("Checking the written data, just to be extra safe...");
    size_t s = 0;
    for (auto seq: input) {
        size = seq.docs.size();
        auto e = coll[s];
        if (e.size() != size) {
            spdlog::error("sequence {} has wrong length! ({} != {})", s, e.size(), size);
            exit(1);
        }
        for (size_t i = 0; i < e.size(); ++i, e.next()) {
            uint64_t docid = *(seq.docs.begin() + i);
            uint64_t freq = *(seq.freqs.begin() + i);

            if (docid != e.docid()) {
                spdlog::error("docid in sequence {} differs at position {}!", s, i);
                spdlog::error("{} != {}", e.docid(), docid);
                spdlog::error("sequence length: {}", seq.docs.size());

                exit(1);
            }

            if (freq != e.freq()) {
                spdlog::error("freq in sequence {} differs at position {}!", s, i);
                spdlog::error("{} != {}", e.freq(), freq);
                spdlog::error("sequence length: {}", seq.docs.size());

                exit(1);
            }
        }
        s += 1;
    }
    spdlog::info("Everything is OK!");
}

}  // namespace pisa

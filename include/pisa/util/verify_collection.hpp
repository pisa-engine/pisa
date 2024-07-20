#pragma once

#include <optional>

#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "scorer/quantized.hpp"

namespace pisa {

template <typename InputCollection, typename Index>
void verify_collection(
    InputCollection const& input,
    Index const& index,
    std::optional<QuantizingScorer> quantizing_scorer = std::nullopt
) {
    size_t size = 0;
    spdlog::info("Checking the written data, just to be extra safe...");
    size_t s = 0;
    for (auto seq: input) {
        size = seq.docs.size();
        auto e = index[s];
        if (e.size() != size) {
            spdlog::error("sequence {} has wrong length! ({} != {})", s, e.size(), size);
            throw std::runtime_error("oops");
        }
        auto term_scorer = quantizing_scorer.has_value()
            ? std::make_optional<std::function<std::uint32_t(std::uint32_t, std::uint32_t)>>(
                  quantizing_scorer->term_scorer(s)
              )
            : std::nullopt;
        for (size_t i = 0; i < e.size(); ++i, e.next()) {
            uint64_t docid = *(seq.docs.begin() + i);
            uint64_t freq = *(seq.freqs.begin() + i);

            if (docid != e.docid()) {
                spdlog::error("docid in sequence {} differs at position {}!", s, i);
                spdlog::error("{} != {}", e.docid(), docid);
                spdlog::error("sequence length: {}", seq.docs.size());
                throw std::runtime_error("oops");
            }

            if (!term_scorer.has_value() && freq != e.freq()) {
                spdlog::error("freq in sequence {} differs at position {}!", s, i);
                spdlog::error("{} != {}", e.freq(), freq);
                spdlog::error("sequence length: {}", seq.docs.size());
                throw std::runtime_error("oops");
            }

            if (term_scorer.has_value()) {
                if ((*term_scorer)(docid, freq) != e.freq()) {
                    spdlog::error("quantized score in sequence {} differs at position {}!", s, i);
                    spdlog::error("{} != {}", e.freq(), (*term_scorer)(docid, freq));
                    spdlog::error("sequence length: {}", seq.docs.size());
                    throw std::runtime_error("oops");
                }
            }
        }
        s += 1;
    }
    spdlog::info("Everything is OK!");
}

template <typename InputCollection, typename Collection>
void verify_collection(
    InputCollection const& input,
    const char* filename,
    std::optional<QuantizingScorer> quantizing_scorer = std::nullopt
) {
    Collection coll;
    auto source = MemorySource::mapped_file(std::filesystem::path(filename));
    pisa::mapper::map(coll, source.data());
    verify_collection(input, coll, std::move(quantizing_scorer));
}

}  // namespace pisa

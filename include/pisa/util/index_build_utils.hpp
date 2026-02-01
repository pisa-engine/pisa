#pragma once

#include <spdlog/spdlog.h>

#include "block_inverted_index.hpp"
#include "freq_index.hpp"
#include "mappable/mapper.hpp"
#include "util/stats_builder.hpp"
#include "util/util.hpp"

namespace pisa {

template <typename DocsSequence, typename FreqsSequence>
void get_size_stats(
    freq_index<DocsSequence, FreqsSequence>& coll, uint64_t& docs_size, uint64_t& freqs_size
) {
    auto size_tree = mapper::size_tree_of(coll);
    size_tree->dump();
    for (auto const& node: size_tree->children) {
        if (node->name == "m_docs_sequences") {
            docs_size = node->size;
        } else if (node->name == "m_freqs_sequences") {
            freqs_size = node->size;
        }
    }
}

template <typename Collection>
void dump_stats(Collection& coll, std::string const& type, uint64_t postings) {
    uint64_t docs_size = 0, freqs_size = 0;
    get_size_stats(coll, docs_size, freqs_size);

    double bits_per_doc = docs_size * 8.0 / postings;
    double bits_per_freq = freqs_size * 8.0 / postings;
    spdlog::info("Documents: {} bytes, {} bits per element", docs_size, bits_per_doc);
    spdlog::info("Frequencies: {} bytes, {} bits per element", freqs_size, bits_per_freq);

    std::cout << pisa::stats_builder()
                     .add("type", type)
                     .add("size", docs_size + freqs_size)
                     .add("docs_size", docs_size)
                     .add("freqs_size", freqs_size)
                     .add("bits_per_doc", bits_per_doc)
                     .add("bits_per_freq", bits_per_freq)
                     .build();
}

inline void dump_stats(SizeStats const& stats, std::size_t postings) {
    stats.size_tree->dump();
    double bits_per_doc = stats.docs * 8.0 / postings;
    double bits_per_freq = stats.freqs * 8.0 / postings;
    spdlog::info("Documents: {} bytes, {} bits per element", stats.docs, bits_per_doc);
    spdlog::info("Frequencies: {} bytes, {} bits per element", stats.freqs, bits_per_freq);
    std::cout << pisa::stats_builder()
                     .add("size", stats.docs + stats.freqs)
                     .add("docs_size", stats.docs)
                     .add("freqs_size", stats.freqs)
                     .add("bits_per_doc", bits_per_doc)
                     .add("bits_per_freq", bits_per_freq)
                     .build();
}

}  // namespace pisa

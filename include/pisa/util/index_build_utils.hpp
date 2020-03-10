#pragma once

#include "spdlog/spdlog.h"

#include "boost/filesystem.hpp"
#include "gsl/span"
#include "index_types.hpp"
#include "mappable/mapper.hpp"
#include "util/progress.hpp"
#include "util/util.hpp"

namespace pisa {

template <typename DocsSequence, typename FreqsSequence>
void get_size_stats(
    freq_index<DocsSequence, FreqsSequence>& coll, uint64_t& docs_size, uint64_t& freqs_size)
{
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

template <typename BlockCodec, bool Profile>
void get_size_stats(block_freq_index<BlockCodec, Profile>& coll, uint64_t& docs_size, uint64_t& freqs_size)
{
    auto size_tree = mapper::size_tree_of(coll);
    size_tree->dump();
    uint64_t total_size = 0;
    for (auto const& node: size_tree->children) {
        if (node->name == "m_lists") {
            total_size = node->size;
        }
    }

    freqs_size = 0;
    for (size_t i = 0; i < coll.size(); ++i) {
        freqs_size += coll[i].stats_freqs_size();
    }
    docs_size = total_size - freqs_size;
}

template <typename Collection>
void dump_stats(Collection& coll, std::string const& type, uint64_t postings)
{
    uint64_t docs_size = 0, freqs_size = 0;
    get_size_stats(coll, docs_size, freqs_size);

    double bits_per_doc = docs_size * 8.0 / postings;
    double bits_per_freq = freqs_size * 8.0 / postings;
    spdlog::info("Documents: {} bytes, {} bits per element", docs_size, bits_per_doc);
    spdlog::info("Frequencies: {} bytes, {} bits per element", freqs_size, bits_per_freq);

    stats_line()("type", type)("size", docs_size + freqs_size)("docs_size", docs_size)(
        "freqs_size", freqs_size)("bits_per_doc", bits_per_doc)("bits_per_freq", bits_per_freq);
}

}  // namespace pisa

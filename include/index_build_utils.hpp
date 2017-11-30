#pragma once

#include "index_types.hpp"
#include "util.hpp"
#include "succinct/mapper.hpp"

namespace ds2i {

    struct progress_logger {
        progress_logger()
            : sequences(0)
            , postings(0)
        {}

        void log()
        {
            logger() << "Processed " << sequences << " sequences, "
                     << postings << " postings" << std::endl;
        }

        void done_sequence(size_t n)
        {
            sequences += 1;
            postings += n;
            if (sequences % 1000000 == 0) {
                log();
            }
        }

        size_t sequences, postings;
    };

    template <typename DocsSequence, typename FreqsSequence>
    void get_size_stats(freq_index<DocsSequence, FreqsSequence>& coll,
                        uint64_t& docs_size, uint64_t& freqs_size)
    {
        auto size_tree = succinct::mapper::size_tree_of(coll);
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
    void get_size_stats(block_freq_index<BlockCodec, Profile>& coll,
                        uint64_t& docs_size, uint64_t& freqs_size)
    {
        auto size_tree = succinct::mapper::size_tree_of(coll);
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
    void dump_stats(Collection& coll,
                    std::string const& type,
                    uint64_t postings)
    {

        uint64_t docs_size = 0, freqs_size = 0;
        get_size_stats(coll, docs_size, freqs_size);

        double bits_per_doc = docs_size * 8.0 / postings;
        double bits_per_freq = freqs_size * 8.0 / postings;
        logger() << "Documents: " << docs_size << " bytes, "
                 << bits_per_doc << " bits per element" << std::endl;
        logger() << "Frequencies: " << freqs_size << " bytes, "
                 << bits_per_freq << " bits per element" << std::endl;

        stats_line()
            ("type", type)
            ("size", docs_size + freqs_size)
            ("docs_size", docs_size)
            ("freqs_size", freqs_size)
            ("bits_per_doc", bits_per_doc)
            ("bits_per_freq", bits_per_freq)
            ;
    }
}

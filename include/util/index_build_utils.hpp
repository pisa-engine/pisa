#pragma once

#include "index_types.hpp"
#include "util/util.hpp"
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
    void get_size_stats(block_freq_index<BlockCodec, Profile>& coll,
                        uint64_t& docs_size, uint64_t& freqs_size)
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


    void emit(std::ostream& os, const uint32_t* vals, size_t n)
    {
        os.write(reinterpret_cast<const char*>(vals), sizeof(*vals) * n);
    }

    void emit(std::ostream& os, uint32_t val)
    {
        emit(os, &val, 1);
    }

    void reorder_inverted_index(const std::string &          input_basename,
                                const std::string &          output_basename,
                                const std::vector<uint32_t> &mapping) {
        std::ofstream output_mapping(output_basename + ".mapping");
        emit(output_mapping, mapping.data(), mapping.size());


        binary_collection input_sizes((input_basename + ".sizes").c_str());
        auto sizes = *input_sizes.begin();

        auto num_docs = sizes.size();
        std::vector<uint32_t> new_sizes(num_docs);
        for (size_t i = 0; i < num_docs; ++i) {
            new_sizes[mapping[i]] = sizes.begin()[i];
        }

        std::ofstream output_sizes(output_basename + ".sizes");
        emit(output_sizes, sizes.size());
        emit(output_sizes, new_sizes.data(), num_docs);


        std::ofstream output_docs(output_basename + ".docs");
        std::ofstream output_freqs(output_basename + ".freqs");
        emit(output_docs, 1);
        emit(output_docs, mapping.size());

        binary_freq_collection input(input_basename.c_str());

        std::vector<std::pair<uint32_t, uint32_t>> pl;
        for (const auto& seq: input) {

            for (size_t i = 0; i < seq.docs.size(); ++i) {
                pl.emplace_back(mapping[seq.docs.begin()[i]],
                                seq.freqs.begin()[i]);
            }

            std::sort(pl.begin(), pl.end());

            emit(output_docs, pl.size());
            emit(output_freqs, pl.size());

            for (const auto& posting: pl) {
                emit(output_docs, posting.first);
                emit(output_freqs, posting.second);
            }
            pl.clear();
        }

    }

    void sample_inverted_index(const std::string &input_basename,
                               const std::string &output_basename,
                               const uint32_t     max_doc) {

        binary_collection input_sizes((input_basename + ".sizes").c_str());
        auto sizes = *input_sizes.begin();

        std::vector<uint32_t> new_sizes(sizes.begin(), std::next(sizes.begin(), max_doc));
        std::ofstream output_sizes(output_basename + ".sizes");
        emit(output_sizes, sizes.size());
        emit(output_sizes, new_sizes.data(), max_doc);

        std::ofstream output_docs(output_basename + ".docs");
        std::ofstream output_freqs(output_basename + ".freqs");
        emit(output_docs, 1);
        emit(output_docs, max_doc);

        binary_freq_collection input(input_basename.c_str());

        std::vector<std::pair<uint32_t, uint32_t>> pl;
        for (const auto& seq: input) {

            auto dociter = seq.docs.begin();
            auto freqiter = seq.freqs.begin();
            for (; dociter != seq.docs.end(); ++dociter, ++freqiter) {
                auto doc = *dociter;
                if (doc >= max_doc) { break; }
                pl.emplace_back(doc, *freqiter);
            }
            if (pl.empty()) { continue; }

            emit(output_docs, pl.size());
            emit(output_freqs, pl.size());

            for (const auto& posting: pl) {
                emit(output_docs, posting.first);
                emit(output_freqs, posting.second);
            }
            pl.clear();
        }

    }

}

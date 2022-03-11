#pragma once

#include <unordered_set>

#include <boost/filesystem.hpp>
#include <gsl/span>

#include "binary_freq_collection.hpp"
#include "util/progress.hpp"

namespace pisa {

template <typename T>
std::ostream& write_sequence(std::ostream& os, gsl::span<T> sequence)
{
    auto length = static_cast<uint32_t>(sequence.size());
    os.write(reinterpret_cast<const char*>(&length), sizeof(length));
    os.write(reinterpret_cast<const char*>(sequence.data()), length * sizeof(T));
    return os;
}

inline void emit(std::ostream& os, const uint32_t* vals, size_t n)
{
    os.write(reinterpret_cast<const char*>(vals), sizeof(*vals) * n);
}

inline void emit(std::ostream& os, uint32_t val)
{
    emit(os, &val, 1);
}

// sample_fn must be stable, it must return a sorted vector
template <typename SampleFn>
void sample_inverted_index(
    std::string const& input_basename,
    std::string const& output_basename,
    SampleFn&& sample_fn,
    std::unordered_set<size_t>& terms_to_drop)
{
    binary_freq_collection input(input_basename.c_str());

    boost::filesystem::copy_file(
        fmt::format("{}.sizes", input_basename),
        fmt::format("{}.sizes", output_basename),
        boost::filesystem::copy_option::overwrite_if_exists);

    std::ofstream dos(output_basename + ".docs");
    std::ofstream fos(output_basename + ".freqs");

    auto document_count = static_cast<std::uint32_t>(input.num_docs());
    write_sequence(dos, gsl::make_span<std::uint32_t const>(&document_count, 1));
    pisa::progress progress("Sampling inverted index", input.size());
    size_t term = 0;
    for (auto const& plist: input) {
        auto sample = sample_fn(plist.docs);
        if (sample.empty()) {
            terms_to_drop.insert(term);
            term += 1;
            progress.update(1);
            continue;
        }
        assert(std::is_sorted(std::begin(sample), std::end(sample)));
        assert(not sample.empty());

        std::vector<std::uint32_t> sampled_docs;
        std::vector<std::uint32_t> sampled_freqs;
        for (auto index: sample) {
            sampled_docs.push_back(plist.docs[index]);
            sampled_freqs.push_back(plist.freqs[index]);
        }

        write_sequence(dos, gsl::span<std::uint32_t const>(sampled_docs));
        write_sequence(fos, gsl::span<std::uint32_t const>(sampled_freqs));
        term += 1;
        progress.update(1);
    }
}

inline void reorder_inverted_index(
    const std::string& input_basename,
    const std::string& output_basename,
    const std::vector<uint32_t>& mapping)
{
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
    pisa::progress reorder_progress(
        "Reorder inverted index", std::distance(input.begin(), input.end()));

    for (const auto& seq: input) {
        for (size_t i = 0; i < seq.docs.size(); ++i) {
            pl.emplace_back(mapping[seq.docs.begin()[i]], seq.freqs.begin()[i]);
        }

        std::sort(pl.begin(), pl.end());

        emit(output_docs, pl.size());
        emit(output_freqs, pl.size());

        for (const auto& posting: pl) {
            emit(output_docs, posting.first);
            emit(output_freqs, posting.second);
        }
        pl.clear();
        reorder_progress.update(1);
    }
}

}  // namespace pisa

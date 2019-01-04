#include <algorithm>
//#include <atomic>
//#include <fstream>
//#include <iostream>
//#include <numeric>
#include <thread>
#include <vector>
//#include <functional>
//#include <unordered_set>

#include "CLI/CLI.hpp"
#include "pstl/algorithm"
#include "pstl/execution"
//#include "tbb/concurrent_queue.h"
#include "tbb/task_group.h"
#include "gsl/span"
#include "tbb/task_scheduler_init.h"

#include "binary_collection.hpp"
#include "enumerate.hpp"
//#include "parsing/html.hpp"
//#include "parsing/stemmer.hpp"
//#include "parsing/warc.hpp"
#include "util/util.hpp"

using ds2i::logger;
using namespace ds2i;

template <typename T>
std::vector<T> concatenate(std::vector<std::vector<T>> const &containers)
{
    auto full_size = std::accumulate(
        containers.begin(), containers.end(), 0, [](auto const &acc, auto const &container) {
            return acc + container.size();
        });
    std::vector<T> vec(full_size);
    auto next_begin = std::begin(vec);
    for (auto const& container : containers) {
        std::copy(container.begin(), container.end(), next_begin);
        next_begin = std::next(next_begin, container.size());
    }
    return vec;
}

struct Batch {
    gsl::span<gsl::span<uint32_t const>> documents;
    Enumerator_Range<uint32_t>           document_ids;
};

std::vector<std::pair<uint32_t, uint32_t>> map_to_postings(Batch batch)
{
    auto docid = batch.document_ids.begin();
    std::vector<std::pair<uint32_t, uint32_t>> postings;
    for (auto const &document : batch.documents) {
        for (auto const &term : document) {
            postings.emplace_back(term, *docid);
        }
        ++docid;
    }
    return postings;
}

void invert_range(gsl::span<gsl::span<uint32_t const>> documents,
                  uint32_t                             first_document_id,
                  size_t                               batch_size)
{
    std::vector<Batch> batches;
    for (; first_document_id < documents.size(); first_document_id += batch_size) {
        auto last = std::min(static_cast<uint32_t>(first_document_id + batch_size),
                             static_cast<uint32_t>(documents.size()));
        batches.push_back(Batch{documents.subspan(first_document_id, last - first_document_id),
                                enumerate(first_document_id, last)});
    }
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> posting_vectors(batches.size());
    std::transform(std::execution::par_unseq,
                   batches.begin(),
                   batches.end(),
                   std::begin(posting_vectors),
                   map_to_postings);
    auto postings = concatenate(posting_vectors);
    posting_vectors.clear();
    posting_vectors.shrink_to_fit();

    std::sort(std::execution::par_unseq, postings.begin(), postings.end());

    struct Inverted_Index {
        using iterator_type = decltype(postings.begin());
        std::unordered_map<uint32_t, std::vector<uint32_t>> documents{};
        std::unordered_map<uint32_t, std::vector<uint32_t>> frequencies{};

        Inverted_Index() = default;
        Inverted_Index(Inverted_Index &, tbb::split) {}

        void operator()(tbb::blocked_range<iterator_type> const &r)
        {
            auto first = r.begin();
            while (first != r.end()) {
                auto [current_term, current_doc] = *first;
                auto last = std::find_if(first, r.end(), [&](auto const &posting) {
                    return posting.first != current_term || posting.second != current_doc;
                });
                uint32_t freq = std::distance(first, last);
                documents[current_term].push_back(current_doc);
                frequencies[current_term].push_back(freq);
                first = last;
            }
        }

        void join_term(std::vector<uint32_t> &lower_doc,
                       std::vector<uint32_t> &lower_freq,
                       std::vector<uint32_t> &higher_doc,
                       std::vector<uint32_t> &higher_freq)
        {
            if (lower_doc.back() == higher_doc.front()) {
                lower_freq.back() += higher_freq.front();
                lower_doc.insert(lower_doc.end(), std::next(higher_doc.begin()), higher_doc.end());
                lower_freq.insert(
                    lower_freq.end(), std::next(higher_freq.begin()), higher_freq.end());
            } else {
                lower_doc.insert(lower_doc.end(), higher_doc.begin(), higher_doc.end());
                lower_freq.insert(lower_freq.end(), higher_freq.begin(), higher_freq.end());
            }
        }

        void join(Inverted_Index& rhs)
        {
            for (auto&& [term_id, document_ids] : rhs.documents) {
                if (auto pos = documents.find(term_id); pos == documents.end()) {
                    std::swap(pos->second, document_ids);
                    std::swap(frequencies[term_id], rhs.frequencies[term_id]);
                } else if (pos->second.back() <= document_ids.front()) {
                    join_term(
                        pos->second, frequencies[term_id], document_ids, rhs.frequencies[term_id]);
                } else {
                    join_term(
                        document_ids, rhs.frequencies[term_id], pos->second, frequencies[term_id]);
                }
            }
        }
    };

    Inverted_Index index;
    tbb::parallel_reduce(tbb::blocked_range(postings.begin(), postings.end()), index);
}

int main(int argc, char **argv) {

    std::string input_basename;
    std::string output_filename;
    size_t      threads = std::thread::hardware_concurrency();
    //ptrdiff_t   batch_size = 100'000;

    CLI::App app{"parse_collection - parse collection and store as forward index."};
    app.add_option("-i,--input", input_basename, "Forward index filename")->required();
    app.add_option("-o,--output", output_filename, "Output inverted index filename")->required();
    app.add_option("-j,--threads", threads, "Thread count");
    //app.add_option(
    //    "-b,--batch-size", batch_size, "Number of documents to process in one thread", true);
    CLI11_PARSE(app, argc, argv);

    tbb::task_scheduler_init init(threads);
    logger() << "Number of threads: " << threads << '\n';

    //binary_collection coll(input_basename.c_str());
    //for (auto doc_iter = ++coll.begin(); doc_iter != coll.end(); ++doc_iter) {
    //    for (auto &term_id : *doc_iter) {
    //        term_id = mapping[term_id];
    //    }
    //}

    return 0;
}

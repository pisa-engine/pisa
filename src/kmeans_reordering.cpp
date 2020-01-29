#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
#include <CLI/CLI.hpp>
#include <boost/numeric/ublas/io.hpp>
#include <boost/numeric/ublas/vector_sparse.hpp>
#include <mutex>
#include <thread>

#include "pstl/algorithm"
#include "pstl/execution"
#include "tbb/task_group.h"

#include <boost/range/combine.hpp>
#include <complex>
#include <mio/mmap.hpp>
#include <optional>
#include <random>
#include <tbb/task_scheduler_init.h>

#include "binary_freq_collection.hpp"
#include "mappable/mapper.hpp"
#include "scorer/scorer.hpp"
#include "util/progress.hpp"
#include "wand_data.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;
using namespace boost::numeric::ublas;

using wand_raw_index = wand_data<wand_data_raw>;
using id_type = std::uint32_t;

class Cluster {
    std::vector<id_type> m_prev_document_ids;
    std::vector<id_type> m_cur_document_ids;
    id_type m_centroid;
    size_t m_depth;
    std::mutex mutex;

   public:
    Cluster(std::vector<id_type> const &ids, id_type centroid, size_t depth)
        : m_cur_document_ids(ids), m_centroid(centroid), m_depth(depth)
    {
        spdlog::info("Cluster created at depth: {}", m_depth);
    }

    explicit Cluster(id_type centroid, size_t depth) : m_centroid(centroid), m_depth(depth)
    {
        spdlog::info("Cluster created at depth: {}", m_depth);
    }

    Cluster(Cluster &&rhs)
        : m_prev_document_ids(rhs.m_prev_document_ids),
          m_cur_document_ids(rhs.m_cur_document_ids),
          m_centroid(rhs.m_centroid),
          m_depth(rhs.m_depth)
    {
    }

    size_t depth() const { return m_depth; }

    void dump()
    {
        m_prev_document_ids.swap(m_cur_document_ids);
        m_cur_document_ids.clear();
    }

    bool same_as_before() { return true; }

    std::vector<id_type> const &document_ids() const { return m_cur_document_ids; }

    void add_document_index(id_type id)
    {
        mutex.lock();
        m_cur_document_ids.push_back(id);
        mutex.unlock();
    }

    bool needs_partition()
    {
        spdlog::info("Depth: {}, Size: {}", m_depth, m_cur_document_ids.size());

        return m_cur_document_ids.size() > 128;
    }
};

template <typename SeedFn>
std::vector<Cluster> kmeans(
    std::vector<compressed_vector<float>> const &fwd,
    Cluster const &parent,
    std::function<float(compressed_vector<float>, compressed_vector<float>)> distance,
    SeedFn seed,
    uint32_t MAX_ITER = 10)
{
    std::vector<size_t> centroid_indexes = seed(parent);

    std::vector<Cluster> clusters;
    for (auto &&ci : centroid_indexes) {
        clusters.emplace_back(ci, parent.depth() + 1);
    }

    bool termination = false;
    uint32_t iterations = 0;
    while (!termination) {
        iterations += 1;
        tbb::task_group group;
        for (auto &&doc_index : parent.document_ids()) {
            group.run([&]() {
                auto &doc = fwd[doc_index];
                double smallest_distance = std::numeric_limits<float>::max();
                uint32_t closer_cluster_index = 0;
                for (uint32_t i = 0; i < centroid_indexes.size(); ++i) { // select best cluster
                    auto cur_distance = distance(doc, fwd[centroid_indexes[i]]);
                    if (cur_distance < smallest_distance) {
                        smallest_distance = cur_distance;
                        closer_cluster_index = i;
                    }
                }
                clusters[closer_cluster_index].add_document_index(doc_index);
            });
        }
        group.wait();

        termination = true;
        if (iterations != MAX_ITER) {
            // check if we can stop:
            auto k = clusters.size();
            for (uint32_t i = 0; i < k - 1; ++i) {
                auto &c = clusters[i];
                if (!c.same_as_before()) {
                    c.dump();
                    termination = false;
                }
            }
            if (!termination) {
                auto &c = clusters[k - 1];
                c.dump();
            }
        }
    }

    return clusters;
}

auto euclidean = [](compressed_vector<float> const &lhs, compressed_vector<float> const &rhs) {
    float square_sum = 0;
    if (lhs.size() != rhs.size()) {
        throw std::runtime_error("Cannot compute distance between vectors with difference sizes.");
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        float x = lhs[i];
        float y = rhs[i];
        square_sum += std::pow(y - x, 2);
    }
    return std::sqrt(square_sum);
};

template <typename Iter, typename RandomGenerator>
Iter select_randomly(Iter start, Iter end, RandomGenerator &g)
{
    std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
    std::advance(start, dis(g));
    return start;
}

auto seed = [](Cluster const &c) -> std::vector<size_t> {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    auto first = select_randomly(c.document_ids().begin(), c.document_ids().end(), gen);
    auto second = first;
    while (second == first) {
        second = select_randomly(c.document_ids().begin(), c.document_ids().end(), gen);
    }

    return {*first, *second};
};

std::list<Cluster> compute_clusters(std::vector<compressed_vector<float>> const &fwd)
{
    std::vector<id_type> ids(fwd.size());
    std::iota(ids.begin(), ids.end(), 0);
    Cluster root(ids, 0, 0);

    std::deque<Cluster> to_split;
    to_split.push_back(std::move(root));
    std::list<Cluster> final_clusters;
    while (to_split.size()) {
        auto &parent = to_split.front();
        auto children = kmeans(fwd, parent, euclidean, seed);
        to_split.pop_front(); // destroy front item
        for (auto &c : children) {
            if (c.needs_partition()) {
                to_split.push_back(std::move(c));
            } else {
                final_clusters.push_back(std::move(c));
            }
        }
    }

    return final_clusters;
}

std::vector<compressed_vector<float>> from_inverted_index(const std::string &input_basename,
                                                          const std::string &wand_data_filename,
                                                          std::string const &scorer_name,
                                                          size_t min_len)
{
    binary_freq_collection coll((input_basename).c_str());

    wand_raw_index wdata;
    mio::mmap_source md;
    std::error_code error;
    md.map(wand_data_filename, error);
    if (error) {
        std::cerr << "error mapping file: " << error.message() << ", exiting..." << std::endl;
        throw std::runtime_error("Error opening file");
    }
    mapper::map(wdata, md, mapper::map_flags::warmup);

    auto scorer = scorer::from_name(scorer_name, wdata);

    auto num_terms = 0;
    for (auto const &seq : coll) {
        if (seq.docs.size() >= min_len) {
            num_terms += 1;
        }
    }
    spdlog::info("Number of terms: {}", num_terms);

    std::vector<compressed_vector<float>> fwd(coll.num_docs());
    for (auto &&d : fwd) {
        d.resize(num_terms);
    }

    {
        progress p("Building forward index", num_terms);
        id_type tid = 0;
        for (auto const &seq : coll) {
            auto t_scorer = scorer->term_scorer(tid);
            if (seq.docs.size() >= min_len) {
                for (size_t i = 0; i < seq.docs.size(); ++i) {
                    uint64_t docid = *(seq.docs.begin() + i);
                    uint64_t freq = *(seq.freqs.begin() + i);
                    fwd[docid][tid] = t_scorer(docid, freq);
                }
                p.update(1);
                ++tid;
            }
        }
    }
    return fwd;
}

int main(int argc, char const *argv[])
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string input_basename;
    std::string wand_data_filename;
    std::string output_basename;
    std::optional<std::string> documents_filename;
    std::optional<std::string> reordered_documents_filename;
    size_t min_len = 0;
    size_t threads = std::thread::hardware_concurrency();

    CLI::App app{"K-means reordering algorithm used for inverted indexed reordering."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "WAND data filename")->required();
    app.add_option("-o,--output", output_basename, "Output basename");
    app.add_option("-m,--min-len", min_len, "Minimum list threshold");
    auto docs_opt = app.add_option("--documents", documents_filename, "Documents lexicon");
    app.add_option(
           "--reordered-documents", reordered_documents_filename, "Reordered documents lexicon")
        ->needs(docs_opt);
    CLI11_PARSE(app, argc, argv);
    tbb::task_scheduler_init init(threads);
    spdlog::info("Number of threads: {}", threads);

    auto fwd = from_inverted_index(input_basename, wand_data_filename, "bm25", min_len);
    spdlog::info("Computing clusters");
    auto clusters = compute_clusters(fwd);
    spdlog::info("Reordering documents");
    size_t document_idx = 0;
    for (auto &&c : clusters) {
        for (auto &&d : c.document_ids()) {
            std::cout << document_idx << " " << d << std::endl;
            document_idx += 1;
        }
    }
}
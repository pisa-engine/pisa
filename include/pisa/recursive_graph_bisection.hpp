#pragma once

#include <cmath>
#include <fstream>
#include <iterator>
#include <thread>
#include <vector>

#include "tbb/enumerable_thread_specific.h"
#include "tbb/task_group.h"

#include "algorithm.hpp"
#include "forward_index.hpp"
#include "payload_vector.hpp"
#include "util/index_build_utils.hpp"
#include "util/inverted_index_utils.hpp"
#include "util/log.hpp"
#include "util/progress.hpp"
#include "util/single_init_vector.hpp"

namespace pisa {
const Log2<4096> log2;

namespace bp {

    using ThreadLocalGains = tbb::enumerable_thread_specific<single_init_vector<double>>;
    using ThreadLocalDegrees = tbb::enumerable_thread_specific<single_init_vector<size_t>>;

    struct ThreadLocal {
        ThreadLocalGains gains;
        ThreadLocalDegrees left_degrees;
        ThreadLocalDegrees right_degrees;
    };

    PISA_ALWAYSINLINE double expb(double logn1, double logn2, size_t deg1, size_t deg2)
    {
        __m128 _deg = _mm_cvtepi32_ps(_mm_set_epi32(deg1, deg1, deg2, deg2));
        __m128 _log = _mm_set_ps(logn1, log2(deg1 + 1), logn2, log2(deg2 + 1));
        __m128 _result = _mm_mul_ps(_deg, _log);
        float a[4];
        _mm_store_ps(a, _result);
        return a[3] - a[2] + a[1] - a[0];  // Can we do it with SIMD?
    };

    template <typename ThreadLocalContainer>
    [[nodiscard]] PISA_ALWAYSINLINE auto&
    clear_or_init(ThreadLocalContainer&& container, std::size_t size)
    {
        bool exists = false;
        auto& ref = container.local(exists);
        if (exists) {
            ref.clear();
        } else {
            ref.resize(size);
        }
        return ref;
    }

}  // namespace bp

template <class Iterator>
struct document_partition;

template <class Iterator>
class document_range {
  public:
    using value_type = typename std::iterator_traits<Iterator>::value_type;

    document_range(
        Iterator first,
        Iterator last,
        std::reference_wrapper<const forward_index> fwdidx,
        std::reference_wrapper<std::vector<double>> gains)
        : m_first(first), m_last(last), m_fwdidx(fwdidx), m_gains(gains)
    {}

    Iterator begin() { return m_first; }
    Iterator end() { return m_last; }
    std::ptrdiff_t size() const { return std::distance(m_first, m_last); }

    PISA_ALWAYSINLINE document_partition<Iterator> split() const
    {
        Iterator mid = std::next(m_first, size() / 2);
        return {document_range(m_first, mid, m_fwdidx, m_gains),
                document_range(mid, m_last, m_fwdidx, m_gains),
                term_count()};
    }

    PISA_ALWAYSINLINE document_range operator()(std::ptrdiff_t left, std::ptrdiff_t right) const
    {
        assert(left < right);
        assert(right <= size());
        return document_range(std::next(m_first, left), std::next(m_first, right), m_fwdidx, m_gains);
    }

    std::size_t term_count() const { return m_fwdidx.get().term_count(); }
    std::vector<uint32_t> terms(value_type document) const
    {
        return m_fwdidx.get().terms(document);
    }
    double gain(value_type document) const { return m_gains.get()[document]; }
    double& gain(value_type document) { return m_gains.get()[document]; }

    auto by_gain()
    {
        return [this](const value_type& lhs, const value_type& rhs) {
            return m_gains.get()[lhs] > m_gains.get()[rhs];
        };
    }

  private:
    Iterator m_first;
    Iterator m_last;
    std::reference_wrapper<const forward_index> m_fwdidx;
    std::reference_wrapper<std::vector<double>> m_gains;
};

template <class Iterator>
struct document_partition {
    document_range<Iterator> left;
    document_range<Iterator> right;
    size_t term_count;

    std::ptrdiff_t size() const { return left.size() + right.size(); }
};

template <class Iterator>
struct computation_node {
    int level;
    int iteration_count;
    document_partition<Iterator> partition;
    bool cache;

    static computation_node from_stream(std::istream& is, const document_range<Iterator>& range)
    {
        int level, iteration_count;
        std::ptrdiff_t left_first, right_first, left_last, right_last;
        bool cache;
        is >> level >> iteration_count >> left_first >> left_last >> right_first >> right_last;
        document_partition<Iterator> partition{
            range(left_first, left_last), range(right_first, right_last), range.term_count()};
        if (not(is >> std::noboolalpha >> cache)) {
            cache = partition.size() > 64;
        }
        return computation_node{level, iteration_count, std::move(partition), cache};
    };

    bool operator<(const computation_node& other) const { return level < other.level; }
};

auto get_mapping = [](const auto& collection) {
    std::vector<uint32_t> mapping(collection.size(), 0U);
    size_t p = 0;
    for (const auto& id: collection) {
        mapping[id] = p++;
    }
    return mapping;
};

template <class Iterator>
void compute_degrees(document_range<Iterator>& range, single_init_vector<size_t>& deg_map)
{
    for (const auto& document: range) {
        auto terms = range.terms(document);
        auto deg_map_inc = [&](const auto& t) { deg_map.set(t, deg_map[t] + 1); };
        pisa::for_each(pisa::execution::par_unseq, terms.begin(), terms.end(), deg_map_inc);
    }
}

template <bool isLikelyCached = true, typename Iter>
void compute_move_gains_caching(
    document_range<Iter>& range,
    const std::ptrdiff_t from_n,
    const std::ptrdiff_t to_n,
    const single_init_vector<size_t>& from_lex,
    const single_init_vector<size_t>& to_lex,
    bp::ThreadLocal& thread_local_data)
{
    const auto logn1 = log2(from_n);
    const auto logn2 = log2(to_n);

    auto& gain_cache = bp::clear_or_init(thread_local_data.gains, from_lex.size());
    auto compute_document_gain = [&](auto& d) {
        double gain = 0.0;
        auto terms = range.terms(d);
        for (const auto& t: terms) {
            if constexpr (isLikelyCached) {  // NOLINT(readability-braces-around-statements)
                if (PISA_UNLIKELY(not gain_cache.has_value(t))) {
                    const auto& from_deg = from_lex[t];
                    const auto& to_deg = to_lex[t];
                    const auto term_gain = bp::expb(logn1, logn2, from_deg, to_deg)
                        - bp::expb(logn1, logn2, from_deg - 1, to_deg + 1);
                    gain_cache.set(t, term_gain);
                }
            } else {
                if (PISA_LIKELY(not gain_cache.has_value(t))) {
                    const auto& from_deg = from_lex[t];
                    const auto& to_deg = to_lex[t];
                    const auto term_gain = bp::expb(logn1, logn2, from_deg, to_deg)
                        - bp::expb(logn1, logn2, from_deg - 1, to_deg + 1);
                    gain_cache.set(t, term_gain);
                }
            }
            gain += gain_cache[t];
        }
        range.gain(d) = gain;
    };
    std::for_each(range.begin(), range.end(), compute_document_gain);
}

template <class Iterator, class GainF>
void compute_gains(
    document_partition<Iterator>& partition,
    const degree_map_pair& degrees,
    GainF gain_function,
    bp::ThreadLocal& thread_local_data)
{
    auto n1 = partition.left.size();
    auto n2 = partition.right.size();
    gain_function(partition.left, n1, n2, degrees.left, degrees.right, thread_local_data);
    gain_function(partition.right, n2, n1, degrees.right, degrees.left, thread_local_data);
}

template <class Iterator>
void swap(document_partition<Iterator>& partition, degree_map_pair& degrees)
{
    auto left = partition.left;
    auto right = partition.right;
    auto lit = left.begin();
    auto rit = right.begin();
    for (; lit != left.end() && rit != right.end(); ++lit, ++rit) {
        if (PISA_UNLIKELY(left.gain(*lit) + right.gain(*rit) <= 0)) {
            break;
        }
        {
            auto terms = left.terms(*lit);
            for (auto& term: terms) {
                degrees.left.set(term, degrees.left[term] - 1);
                degrees.right.set(term, degrees.right[term] + 1);
            }
        }
        {
            auto terms = right.terms(*rit);
            for (auto& term: terms) {
                degrees.left.set(term, degrees.left[term] + 1);
                degrees.right.set(term, degrees.right[term] - 1);
            }
        }

        std::iter_swap(lit, rit);
    }
}

template <class Iterator, class GainF>
void process_partition(
    document_partition<Iterator>& partition,
    GainF gain_function,
    bp::ThreadLocal& thread_local_data,
    int iterations = 20)
{
    auto& left_degree =
        bp::clear_or_init(thread_local_data.left_degrees, partition.left.term_count());
    auto& right_degree =
        bp::clear_or_init(thread_local_data.right_degrees, partition.right.term_count());
    compute_degrees(partition.left, left_degree);
    compute_degrees(partition.right, right_degree);
    degree_map_pair degrees{left_degree, right_degree};

    for (int iteration = 0; iteration < iterations; ++iteration) {
        compute_gains(partition, degrees, gain_function, thread_local_data);
        tbb::parallel_invoke(
            [&] {
                pisa::sort(
                    pisa::execution::par_unseq,
                    partition.left.begin(),
                    partition.left.end(),
                    partition.left.by_gain());
            },
            [&] {
                pisa::sort(
                    pisa::execution::par_unseq,
                    partition.right.begin(),
                    partition.right.end(),
                    partition.right.by_gain());
            });
        swap(partition, degrees);
    }
}

template <class Iterator>
void recursive_graph_bisection(
    document_range<Iterator> documents,
    size_t depth,
    size_t cache_depth,
    progress& p,
    std::shared_ptr<bp::ThreadLocal> thread_local_data = nullptr)
{
    if (thread_local_data == nullptr) {
        thread_local_data = std::make_shared<bp::ThreadLocal>();
    }
    std::sort(documents.begin(), documents.end());
    auto partition = documents.split();
    if (cache_depth >= 1) {
        process_partition(partition, compute_move_gains_caching<true, Iterator>, *thread_local_data);
        --cache_depth;
    } else {
        process_partition(partition, compute_move_gains_caching<false, Iterator>, *thread_local_data);
    }

    p.update(documents.size());
    if (depth > 1 && documents.size() > 2) {
        tbb::parallel_invoke(
            [&, thread_local_data] {
                recursive_graph_bisection(
                    partition.left, depth - 1, cache_depth, p, thread_local_data);
            },
            [&, thread_local_data] {
                recursive_graph_bisection(
                    partition.right, depth - 1, cache_depth, p, thread_local_data);
            });
    } else {
        std::sort(partition.left.begin(), partition.left.end());
        std::sort(partition.right.begin(), partition.right.end());
    }
}

/// Runs the Network-BP according to the configuration in `nodes`.
///
/// All nodes on the same level of recursion are allowed to be executed in parallel.
/// The caller must ensure that no range on the same level intersects with another.
/// Failure to do so leads to undefined behavior.
template <class Iterator>
void recursive_graph_bisection(std::vector<computation_node<Iterator>> nodes, progress& p)
{
    bp::ThreadLocal thread_local_data;
    std::sort(nodes.begin(), nodes.end());
    auto first = nodes.begin();
    auto end = nodes.end();
    while (first != end) {
        auto last = std::find_if(
            first, end, [&first](const auto& node) { return node.level > first->level; });
        bool last_level = last == end;
        tbb::task_group level_group;
        std::for_each(first, last, [&thread_local_data, &level_group, last_level, &p](auto& node) {
            level_group.run([&]() {
                std::sort(node.partition.left.begin(), node.partition.left.end());
                std::sort(node.partition.right.begin(), node.partition.right.end());
                if (node.cache) {
                    process_partition(
                        node.partition,
                        compute_move_gains_caching<true, Iterator>,
                        thread_local_data,
                        node.iteration_count);
                } else {
                    process_partition(
                        node.partition,
                        compute_move_gains_caching<false, Iterator>,
                        thread_local_data,
                        node.iteration_count);
                }
                if (last_level) {
                    std::sort(node.partition.left.begin(), node.partition.left.end());
                    std::sort(node.partition.right.begin(), node.partition.right.end());
                }
                p.update(node.partition.size());
            });
        });
        level_group.wait();
        first = last;
    }
}

}  // namespace pisa

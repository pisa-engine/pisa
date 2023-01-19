#include <algorithm>
#include <random>
#include <string>

#include <benchmark/benchmark.h>

#include <pisa/topk_queue.hpp>

using Entry = std::pair<float, std::uint32_t>;

enum Series : std::int64_t {
    INCREASING = 0,
    DECREASING = 1,
    RANDOM = 2,
};

auto generate_increasing_scores(std::size_t length)
{
    std::vector<Entry> vals;
    std::generate_n(std::back_inserter(vals), length, [score = 100.0, docid = 0]() mutable {
        score += 0.1;
        return Entry{score, docid++};
    });
    return vals;
}

auto generate_decreasing_scores(std::size_t length)
{
    std::vector<Entry> vals;
    std::generate_n(std::back_inserter(vals), length, [score = 100.0, docid = 0]() mutable {
        score -= 0.1;
        return Entry{score, docid++};
    });
    return vals;
}

auto generate_random_scores(std::size_t length)
{
    std::mt19937 gen(1902741074);
    std::uniform_real_distribution<> dis(0.0, 10.0);
    std::vector<Entry> vals;
    std::generate_n(std::back_inserter(vals), length, [&, docid = 0]() mutable {
        return Entry{dis(gen), docid++};
    });
    return vals;
}

auto generate_entries(std::size_t length, Series series)
{
    switch (series) {
    case INCREASING: return generate_increasing_scores(length);
    case DECREASING: return generate_decreasing_scores(length);
    case RANDOM: return generate_random_scores(length);
    }
    throw std::logic_error("unreachable");
}

void insert_all(pisa::topk_queue& queue, std::vector<Entry> const& entries)
{
    for (auto const& [score, docid]: entries) {
        benchmark::DoNotOptimize(queue.insert(score, docid));
    }
}

static void bm_topk_queue(benchmark::State& state)
{
    auto entries = generate_entries(state.range(0), Series{state.range(2)});
    for (auto _: state) {
        pisa::topk_queue queue(state.range(1));

        auto start = std::chrono::high_resolution_clock::now();

        benchmark::DoNotOptimize(queue.topk().data());
        insert_all(queue, entries);

        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        benchmark::ClobberMemory();
    }
}

BENCHMARK(bm_topk_queue)
    ->ArgNames({"len", "k", "series"})
    ->ArgsProduct({{1'000'000}, {10}, {INCREASING, DECREASING, RANDOM}})
    ->Unit(benchmark::kMicrosecond)
    ->Repetitions(20)
    ->DisplayAggregatesOnly();

BENCHMARK_MAIN();

#pragma once

#include <optional>
#include <string>
#include <thread>

#include <CLI/CLI.hpp>
#include <range/v3/view/getlines.hpp>
#include <range/v3/view/transform.hpp>
#include <tl/optional.hpp>

#include "io.hpp"
#include "v1/index_metadata.hpp"
#include "v1/runtime_assert.hpp"

namespace pisa {

namespace arg {

    struct Index {
        explicit Index(CLI::App* app)
        {
            app->add_option("-i,--index",
                            m_metadata_path,
                            "Path of .yml file of an index "
                            "(if not provided, it will be looked for in the current directory)",
                            false);
        }

        [[nodiscard]] auto index_metadata() const -> v1::IndexMetadata
        {
            return v1::IndexMetadata::from_file(v1::resolve_yml(m_metadata_path));
        }

       private:
        tl::optional<std::string> m_metadata_path;
    };

    enum class QueryMode : bool { Ranked, Unranked };

    template <QueryMode Mode = QueryMode::Ranked, int DefaultK = 1'000>
    struct Query {
        explicit Query(CLI::App* app)
        {
            app->add_option("-q,--query", m_query_file, "Path to file with queries", false);
            app->add_option("--qf,--query-fmt", m_query_input_format, "Input file format", true);
            if constexpr (Mode == QueryMode::Ranked) {
                app->add_option("-k", m_k, "The number of top results to return", true);
            }
            app->add_flag("--force-parse",
                          m_force_parse,
                          "Force parsing of query string even ifterm IDs already available");
        }

        [[nodiscard]] auto query_file() -> tl::optional<std::string const&>
        {
            if (m_query_file) {
                return m_query_file.value();
            }
            return tl::nullopt;
        }

        [[nodiscard]] auto queries(v1::IndexMetadata const& meta) const -> std::vector<v1::Query>
        {
            std::vector<v1::Query> queries;
            auto parser = meta.query_parser();
            auto parse_line = [&](auto&& line) {
                auto query = [&line, this]() {
                    if (m_query_input_format == "jl") {
                        return v1::Query::from_json(line);
                    }
                    return v1::Query::from_plain(line);
                }();
                if (not query.term_ids() || m_force_parse) {
                    query.parse(parser);
                }
                if constexpr (Mode == QueryMode::Ranked) {
                    query.k(m_k);
                }
                queries.push_back(std::move(query));
            };
            if (m_query_file) {
                std::ifstream is(*m_query_file);
                pisa::io::for_each_line(is, parse_line);
            } else {
                pisa::io::for_each_line(std::cin, parse_line);
            }
            return queries;
        }

        [[nodiscard]] auto query_range(v1::IndexMetadata const& meta)
        {
            auto lines = [&] {
                if (m_query_file) {
                    m_query_file_handle = std::make_unique<std::ifstream>(*m_query_file);
                    return ranges::getlines(*m_query_file_handle);
                }
                return ranges::getlines(std::cin);
            }();
            return ranges::views::transform(lines,
                                            [force_parse = m_force_parse,
                                             k = m_k,
                                             parser = meta.query_parser(),
                                             qfmt = m_query_input_format](auto&& line) {
                                                auto query = [&]() {
                                                    if (qfmt == "jl") {
                                                        return v1::Query::from_json(line);
                                                    }
                                                    if (qfmt == "plain") {
                                                        return v1::Query::from_plain(line);
                                                    }
                                                    spdlog::error("Unknown  query format: {}",
                                                                  qfmt);
                                                    std::exit(1);
                                                }();
                                                if (not query.term_ids() || force_parse) {
                                                    query.parse(parser);
                                                }
                                                // Not constexpr to silence unused k value warning.
                                                // Performance is not a concern.
                                                if (Mode == QueryMode::Ranked) {
                                                    query.k(k);
                                                }
                                                return query;
                                            });
        }

       private:
        std::unique_ptr<std::ifstream> m_query_file_handle = nullptr;
        tl::optional<std::string> m_query_file;
        std::string m_query_input_format = "jl";
        int m_k = DefaultK;
        bool m_force_parse{false};
    };

    struct Benchmark {
        explicit Benchmark(CLI::App* app)
        {
            app->add_flag("--benchmark", m_is_benchmark, "Run benchmark");
        }

        [[nodiscard]] auto is_benchmark() const -> bool { return m_is_benchmark; }

       private:
        bool m_is_benchmark = false;
    };

    struct QuantizedScores {
        explicit QuantizedScores(CLI::App* app)
        {
            app->add_flag("--quantized", m_use_quantized, "Use quantized scores");
        }

        [[nodiscard]] auto use_quantized() const -> bool { return m_use_quantized; }

       private:
        bool m_use_quantized = false;
    };

    struct Threads {
        explicit Threads(CLI::App* app)
        {
            app->add_option("-j,--threads", m_threads, "Number of threads");
        }

        [[nodiscard]] auto threads() const -> std::size_t { return m_threads; }

       private:
        std::size_t m_threads = std::thread::hardware_concurrency();
    };

} // namespace arg

template <typename... Mixin>
struct App : public CLI::App, public Mixin... {
    explicit App(std::string const& description) : CLI::App(description), Mixin(this)... {}
};

struct QueryApp : public App<arg::Index,
                             arg::Query<arg::QueryMode::Ranked, 1'000>,
                             arg::Benchmark,
                             arg::QuantizedScores> {
    explicit QueryApp(std::string const& description) : App(description) {}
};

} // namespace pisa

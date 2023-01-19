#pragma once

#include <fstream>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <thread>

#include <CLI/CLI.hpp>
#include <range/v3/view/getlines.hpp>
#include <range/v3/view/transform.hpp>
#include <spdlog/spdlog.h>
#include <unordered_set>

#include "io.hpp"
#include "pisa/query/query_parser.hpp"
#include "pisa/term_map.hpp"
#include "pisa/text_analyzer.hpp"
#include "query/queries.hpp"
#include "scorer/scorer.hpp"
#include "sharding.hpp"
#include "tokenizer.hpp"
#include "type_safe.hpp"
#include "wand_utils.hpp"

namespace pisa {

namespace arg {

    struct Encoding {
        explicit Encoding(CLI::App* app);
        [[nodiscard]] auto index_encoding() const -> std::string const&;

      private:
        std::string m_encoding;
    };

    enum class WandMode : bool { Required, Optional };

    template <WandMode Mode = WandMode::Required>
    struct WandData {
        explicit WandData(CLI::App* app)
        {
            auto* wand = app->add_option("-w,--wand", m_wand_data_path, "WAND data filename");
            app->add_flag("--compressed-wand", m_wand_compressed, "Compressed WAND data file")
                ->needs(wand);

            if constexpr (Mode == WandMode::Required) {
                wand->required();
            }
        }

        [[nodiscard]] auto wand_data_path() const
        {
            if constexpr (Mode == WandMode::Required) {
                return *m_wand_data_path;
            } else {
                return m_wand_data_path;
            }
        }
        [[nodiscard]] auto is_wand_compressed() const -> bool { return m_wand_compressed; }

        /// Transform paths for `shard`.
        void apply_shard(Shard_Id shard)
        {
            if (m_wand_data_path) {
                m_wand_data_path = expand_shard(*m_wand_data_path, shard);
            }
        }

      private:
        std::optional<std::string> m_wand_data_path;
        bool m_wand_compressed = false;
    };

    struct Index: public Encoding {
        explicit Index(CLI::App* app);
        [[nodiscard]] auto index_filename() const -> std::string const&;

      private:
        std::string m_index;
    };

    /**
     * CLI arguments related to
     */
    struct Analyzer {
        static const std::set<std::string> VALID_TOKENIZERS;
        static const std::set<std::string> VALID_TOKEN_FILTERS;

        explicit Analyzer(CLI::App* app);
        [[nodiscard]] auto tokenizer() const -> std::unique_ptr<::pisa::Tokenizer>;
        [[nodiscard]] auto text_analyzer() const -> TextAnalyzer;

      private:
        std::string m_tokenizer = "english";
        bool m_strip_html = false;
        std::vector<std::string> m_token_filters{};
        std::optional<std::string> m_stopwords_file{};
    };

    enum class QueryMode : bool { Ranked, Unranked };

    template <QueryMode Mode = QueryMode::Ranked>
    struct Query: public Analyzer {
        explicit Query(CLI::App* app) : Analyzer(app)
        {
            app->add_option("-q,--queries", m_query_file, "Path to file with queries", false);
            m_terms_option = app->add_option("--terms", m_term_lexicon, "Term lexicon");
            app->add_flag("--weighted", m_weighted, "Weights scores by query frequency");
            if constexpr (Mode == QueryMode::Ranked) {
                app->add_option("-k", m_k, "The number of top results to return")->required();
            }
        }

        [[nodiscard]] auto query_file() -> std::optional<std::reference_wrapper<std::string const>>
        {
            if (m_query_file) {
                return m_query_file.value();
            }
            return std::nullopt;
        }

        [[nodiscard]] auto queries() const -> std::vector<::pisa::Query>
        {
            std::vector<::pisa::Query> q;
            std::unique_ptr<TermMap> term_map = [this]() -> std::unique_ptr<TermMap> {
                if (this->m_term_lexicon) {
                    return std::make_unique<LexiconMap>(*this->m_term_lexicon);
                }
                return std::make_unique<IntMap>();
            }();
            QueryParser parser(text_analyzer(), std::move(term_map));
            auto parse_query = [&q, &parser](auto&& line) { q.push_back(parser.parse(line)); };
            if (m_query_file) {
                std::ifstream is(*m_query_file);
                io::for_each_line(is, parse_query);
            } else {
                io::for_each_line(std::cin, parse_query);
            }
            return q;
        }

        [[nodiscard]] auto k() const -> int { return m_k; }

        [[nodiscard]] auto weighted() const -> bool { return m_weighted; }

      protected:
        [[nodiscard]] auto terms_option() const -> CLI::Option* { return m_terms_option; }
        void override_term_lexicon(std::string term_lexicon)
        {
            m_term_lexicon = std::move(term_lexicon);
        }

      private:
        std::optional<std::string> m_query_file;
        int m_k = 0;
        bool m_weighted = false;
        std::optional<std::string> m_term_lexicon{std::nullopt};
        CLI::Option* m_terms_option{};
    };

    struct Algorithm {
        explicit Algorithm(CLI::App* app);
        [[nodiscard]] auto algorithm() const -> std::string const&;

      private:
        std::string m_algorithm;
    };

    enum class ScorerMode : bool { Required, Optional };

    template <typename T>
    CLI::Option* add_scorer_options(CLI::App* app, T& args, ScorerMode scorer_mode)
    {
        CLI::Option* scorer;
        if (scorer_mode == ScorerMode::Required) {
            scorer =
                app->add_option("-s,--scorer", args.m_params.name, "Scorer function")->required();
        } else {
            scorer = app->add_option("-s,--scorer", args.m_params.name, "Scorer function");
        }

        app->add_option("--bm25-k1", args.m_params.bm25_k1, "BM25 k1 parameter.")->needs(scorer);
        app->add_option("--bm25-b", args.m_params.bm25_b, "BM25 b parameter.")->needs(scorer);
        app->add_option("--pl2-c", args.m_params.pl2_c, "PL2 c parameter.")->needs(scorer);
        app->add_option("--qld-mu", args.m_params.qld_mu, "QLD mu parameter.")->needs(scorer);
        return scorer;
    }

    struct Quantize {
        explicit Quantize(CLI::App* app);
        [[nodiscard]] auto scorer_params() const -> ScorerParams;
        [[nodiscard]] auto wand_data_path() const -> std::optional<std::string> const&;
        [[nodiscard]] auto quantize() const -> bool;

        template <typename T>
        friend CLI::Option* add_scorer_options(CLI::App* app, T& args, ScorerMode scorer_mode);

      private:
        ScorerParams m_params;
        std::optional<std::string> m_wand_data_path;
        bool m_quantize = false;
    };

    struct Scorer {
        explicit Scorer(CLI::App* app);
        [[nodiscard]] auto scorer_params() const -> ScorerParams;

        template <typename T>
        friend CLI::Option* add_scorer_options(CLI::App* app, T& args, ScorerMode scorer_mode);

      private:
        ScorerParams m_params;
    };

    struct Thresholds {
        explicit Thresholds(CLI::App* app);
        [[nodiscard]] auto thresholds_file() const -> std::optional<std::string> const&;
        [[nodiscard]] auto thresholds_option() -> CLI::Option*;

      private:
        std::optional<std::string> m_thresholds_filename;
        CLI::Option* m_option;
    };

    struct Verbose {
        explicit Verbose(CLI::App* app);
        [[nodiscard]] auto verbose() const -> bool;
        auto print_args(std::ostream& os) const -> std::ostream&;

      private:
        bool m_verbose{false};
    };

    struct Threads {
        explicit Threads(CLI::App* app);
        [[nodiscard]] auto threads() const -> std::size_t;
        auto print_args(std::ostream& os) const -> std::ostream&;

      private:
        std::size_t m_threads = std::thread::hardware_concurrency();
    };

    template <std::size_t Default = 100'000>
    struct BatchSize {
        explicit BatchSize(CLI::App* app)
        {
            app->add_option(
                "--batch-size", m_batch_size, "Number of documents to process at a time", true);
        }

        [[nodiscard]] auto batch_size() const -> std::size_t { return m_batch_size; }

      private:
        std::size_t m_batch_size = Default;
    };

    struct Invert {
        explicit Invert(CLI::App* app);
        [[nodiscard]] auto input_basename() const -> std::string;
        [[nodiscard]] auto output_basename() const -> std::string;
        [[nodiscard]] auto term_count() const -> std::optional<std::uint32_t>;

        /// Transform paths for `shard`.
        void apply_shard(Shard_Id shard);

      private:
        std::string m_input_basename{};
        std::string m_output_basename{};
        std::optional<std::uint32_t> m_term_count{};
    };

    struct Compress {
        explicit Compress(CLI::App* app)
        {
            app->add_option("-c,--collection", m_input_basename, "Forward index basename")->required();
            app->add_option("-o,--output", m_output, "Output inverted index")->required();
            app->add_flag("--check", m_check, "Check the correctness of the index");
        }

        [[nodiscard]] auto input_basename() const -> std::string { return m_input_basename; }
        [[nodiscard]] auto output() const -> std::string { return m_output; }
        [[nodiscard]] auto check() const -> bool { return m_check; }

        /// Transform paths for `shard`.
        void apply_shard(Shard_Id shard)
        {
            m_input_basename = expand_shard(m_input_basename, shard);
            m_output = expand_shard(m_output, shard);
        }

      private:
        std::string m_input_basename{};
        std::string m_output{};
        bool m_check = false;
    };

    struct CreateWandData {
        explicit CreateWandData(CLI::App* app);
        [[nodiscard]] auto input_basename() const -> std::string;
        [[nodiscard]] auto output() const -> std::string;
        [[nodiscard]] auto scorer_params() const -> ScorerParams;
        [[nodiscard]] auto block_size() const -> BlockSize;
        [[nodiscard]] auto dropped_term_ids() const -> std::unordered_set<size_t>;
        [[nodiscard]] auto lambda() const -> std::optional<float>;
        [[nodiscard]] auto compress() const -> bool;
        [[nodiscard]] auto range() const -> bool;
        [[nodiscard]] auto quantize() const -> bool;

        /// Transform paths for `shard`.
        void apply_shard(Shard_Id shard);

        template <typename T>
        friend CLI::Option* add_scorer_options(CLI::App* app, T& args, ScorerMode scorer_mode);

      private:
        std::optional<float> m_lambda{};
        std::optional<uint64_t> m_fixed_block_size{};
        std::string m_input_basename;
        std::string m_output;
        ScorerParams m_params;
        bool m_compress = false;
        bool m_range = false;
        bool m_quantize = false;
        std::optional<std::string> m_terms_to_drop_filename;
    };

    struct ReorderDocuments {
        explicit ReorderDocuments(CLI::App* app);
        [[nodiscard]] auto input_basename() const -> std::string;
        [[nodiscard]] auto output_basename() const -> std::optional<std::string>;
        [[nodiscard]] auto document_lexicon() const -> std::optional<std::string>;
        [[nodiscard]] auto reordered_document_lexicon() const -> std::optional<std::string>;
        [[nodiscard]] auto random() const -> bool;
        [[nodiscard]] auto feature_file() const -> std::optional<std::string>;
        [[nodiscard]] auto bp() const -> bool;
        [[nodiscard]] auto mapping_file() const -> std::optional<std::string>;
        [[nodiscard]] auto seed() const -> std::uint64_t;
        [[nodiscard]] auto input_collection() const -> binary_freq_collection;
        [[nodiscard]] auto input_fwd() const -> std::optional<std::string>;
        [[nodiscard]] auto output_fwd() const -> std::optional<std::string>;
        [[nodiscard]] auto min_length() const -> std::size_t;
        [[nodiscard]] auto depth() const -> std::optional<std::size_t>;
        [[nodiscard]] auto nogb() const -> bool;
        [[nodiscard]] auto print() const -> bool;
        [[nodiscard]] auto node_config() const -> std::optional<std::string>;

        void apply_shard(Shard_Id shard);

      private:
        std::string m_input_basename{};
        std::optional<std::string> m_output_basename{};
        std::optional<std::string> m_doclex{};
        std::optional<std::string> m_reordered_doclex{};

        bool m_random = false;
        bool m_bp = false;
        std::optional<std::string> m_feature{};
        std::optional<std::string> m_mapping{};

        // --random
        std::uint64_t m_seed = std::random_device{}();

        // --bp
        std::optional<std::string> m_output_fwd{};
        std::optional<std::string> m_input_fwd{};
        std::size_t m_min_len = 0;
        std::optional<std::size_t> m_depth{};
        bool m_nogb = false;
        bool m_print = false;
        std::optional<std::string> m_node_config{};
    };

    struct Separator {
        explicit Separator(CLI::App* app, std::string default_separator = "\t");
        [[nodiscard]] auto separator() const -> std::string const&;

      private:
        std::string m_separator;
    };

    struct PrintQueryId {
        explicit PrintQueryId(CLI::App* app);
        [[nodiscard]] auto print_query_id() const -> bool;

      private:
        bool m_print_query_id = false;
    };

    /**
     * Log level configuration.
     *
     * This option takes one of the valid string values and translates it into spdlog log level
     * values.
     */
    struct LogLevel {
        static const std::set<std::string> VALID_LEVELS;
        static const std::map<std::string, spdlog::level::level_enum> ENUM_MAP;

        explicit LogLevel(CLI::App* app);
        [[nodiscard]] auto log_level() const -> spdlog::level::level_enum;

      private:
        std::string m_level = "info";
    };

}  // namespace arg

/**
 * A declarative way to define CLI interface. This class inherits from `CLI::App` and therefore it
 * can be used like a regular `CLI::App` object once it is defined. This way, we can have a
 * declarative base with the ability to customize it.
 */
template <typename... Args>
struct App: public CLI::App, public Args... {
    explicit App(std::string const& description) : CLI::App(description), Args(this)...
    {
        this->set_config("--config", "", "Configuration .ini file", false);
    }
};

template <typename... T>
struct Args: public T... {
    explicit Args(CLI::App* app) : T(app)...
    {
        app->set_config("--config", "", "Configuration .ini file", false);
    }

    auto print_args(std::ostream& os) const -> std::ostream&
    {
        (T::print_args(os), ...);
        return os;
    }
};

using InvertArgs = Args<arg::Invert, arg::Threads, arg::BatchSize<100'000>, arg::LogLevel>;
using ReorderDocuments = Args<arg::ReorderDocuments, arg::Threads, arg::LogLevel>;
using CompressArgs = pisa::Args<arg::Compress, arg::Encoding, arg::Quantize, arg::LogLevel>;
using CreateWandDataArgs = pisa::Args<arg::CreateWandData, arg::LogLevel>;

struct TailyStatsArgs
    : pisa::Args<arg::WandData<arg::WandMode::Required>, arg::Scorer, arg::LogLevel> {
    explicit TailyStatsArgs(CLI::App* app)
        : pisa::Args<arg::WandData<arg::WandMode::Required>, arg::Scorer, arg::LogLevel>(app)
    {
        app->add_option("-c,--collection", m_collection_path, "Binary collection basename")->required();
        app->add_option("-o,--output", m_output_path, "Output file path")->required();
        app->set_config("--config", "", "Configuration .ini file", false);
    }

    [[nodiscard]] auto collection_path() const -> std::string const& { return m_collection_path; }
    [[nodiscard]] auto output_path() const -> std::string const& { return m_output_path; }

    /// Transform paths for `shard`.
    void apply_shard(Shard_Id shard)
    {
        arg::WandData<arg::WandMode::Required>::apply_shard(shard);
        m_collection_path = expand_shard(m_collection_path, shard);
        m_output_path = expand_shard(m_output_path, shard);
    }

  private:
    std::string m_collection_path;
    std::string m_output_path;
};

struct TailyRankArgs: pisa::Args<arg::Query<arg::QueryMode::Ranked>> {
    explicit TailyRankArgs(CLI::App* app) : pisa::Args<arg::Query<arg::QueryMode::Ranked>>(app)
    {
        arg::Query<arg::QueryMode::Ranked>::terms_option()->required(true);
        app->add_option("--global-stats", m_global_stats, "Global Taily statistics")->required();
        app->add_option("--shard-stats", m_shard_stats, "Shard-level Taily statistics")->required();
        app->add_option("--shard-terms", m_shard_term_lexicon, "Shard-level term lexicons")->required();
        app->set_config("--config", "", "Configuration .ini file", false);
    }

    [[nodiscard]] auto global_stats() const -> std::string const& { return m_global_stats; }
    [[nodiscard]] auto shard_stats() const -> std::string const& { return m_shard_stats; }

    void apply_shard(Shard_Id shard)
    {
        m_shard_term_lexicon = expand_shard(m_shard_term_lexicon, shard);
        override_term_lexicon(m_shard_term_lexicon);
        m_shard_stats = expand_shard(m_shard_stats, shard);
    }

  private:
    std::string m_global_stats;
    std::string m_shard_stats;
    std::string m_shard_term_lexicon;
};

struct TailyThresholds: pisa::Args<arg::Query<arg::QueryMode::Ranked>, arg::LogLevel> {
    explicit TailyThresholds(CLI::App* app)
        : pisa::Args<arg::Query<arg::QueryMode::Ranked>, arg::LogLevel>(app)
    {
        app->add_option("--stats", m_stats, "Taily statistics file")->required();
        app->set_config("--config", "", "Configuration .ini file", false);
    }

    [[nodiscard]] auto stats() const -> std::string const& { return m_stats; }

    /// Transform paths for `shard`.
    void apply_shard(Shard_Id shard) { m_stats = expand_shard(m_stats, shard); }

  private:
    std::string m_stats;
};

}  // namespace pisa

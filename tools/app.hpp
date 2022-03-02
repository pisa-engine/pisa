#pragma once

#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <thread>

#include <CLI/CLI.hpp>
#include <range/v3/view/getlines.hpp>
#include <range/v3/view/transform.hpp>
#include <spdlog/spdlog.h>

#include "io.hpp"
#include "query/queries.hpp"
#include "scorer/scorer.hpp"
#include "sharding.hpp"
#include "type_safe.hpp"
#include "wand_utils.hpp"

namespace pisa {

namespace arg {

    struct Encoding {
        explicit Encoding(CLI::App* app)
        {
            app->add_option("-e,--encoding", m_encoding, "Index encoding")->required();
        }
        [[nodiscard]] auto index_encoding() const -> std::string const& { return m_encoding; }

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
        explicit Index(CLI::App* app) : Encoding(app)
        {
            app->add_option("-i,--index", m_index, "Inverted index filename")->required();
        }

        [[nodiscard]] auto index_filename() const -> std::string const& { return m_index; }

      private:
        std::string m_index;
    };

    enum class QueryMode : bool { Ranked, Unranked };

    template <QueryMode Mode = QueryMode::Ranked>
    struct Query {
        explicit Query(CLI::App* app)
        {
            app->add_option("-q,--queries", m_query_file, "Path to file with queries", false);
            m_terms_option = app->add_option("--terms", m_term_lexicon, "Term lexicon");
            app->add_option(
                   "--stopwords", m_stop_words, "List of blacklisted stop words to filter out")
                ->needs(m_terms_option);
            app->add_option("--stemmer", m_stemmer, "Stemmer type")->needs(m_terms_option);

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
            auto parse_query = resolve_query_parser(q, m_term_lexicon, m_stop_words, m_stemmer);
            if (m_query_file) {
                std::ifstream is(*m_query_file);
                io::for_each_line(is, parse_query);
            } else {
                io::for_each_line(std::cin, parse_query);
            }
            return q;
        }

        [[nodiscard]] auto k() const -> int { return m_k; }

      protected:
        [[nodiscard]] auto terms_option() const -> CLI::Option* { return m_terms_option; }
        void override_term_lexicon(std::string term_lexicon) { m_term_lexicon = term_lexicon; }

      private:
        std::optional<std::string> m_query_file;
        int m_k = 0;
        std::optional<std::string> m_stop_words{std::nullopt};
        std::optional<std::string> m_stemmer{std::nullopt};
        std::optional<std::string> m_term_lexicon{std::nullopt};
        CLI::Option* m_terms_option{};
    };

    struct Algorithm {
        explicit Algorithm(CLI::App* app)
        {
            app->add_option("-a,--algorithm", m_algorithm, "Query processing algorithm")->required();
        }

        [[nodiscard]] auto algorithm() const -> std::string const& { return m_algorithm; }

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

    template <ScorerMode Mode = ScorerMode::Required>
    struct Quantize {
        explicit Quantize(CLI::App* app) : m_params("")
        {
            auto* wand = app->add_option("-w,--wand", m_wand_data_path, "WAND data filename");
            auto* scorer = add_scorer_options(app, *this, Mode);
            auto* quant = app->add_flag("--quantize", m_quantize, "Quantizes the scores");
            wand->needs(scorer);
            scorer->needs(wand);
            scorer->needs(quant);
            quant->needs(scorer);
        }

        [[nodiscard]] auto scorer_params() const { return m_params; }

        [[nodiscard]] auto wand_data_path() const -> std::optional<std::string> const&
        {
            return m_wand_data_path;
        }

        [[nodiscard]] auto quantize() const { return m_quantize; }

        template <typename T>
        friend CLI::Option* add_scorer_options(CLI::App* app, T& args, ScorerMode scorer_mode);

      private:
        ScorerParams m_params;
        std::optional<std::string> m_wand_data_path;
        bool m_quantize = false;
    };

    struct Scorer {
        explicit Scorer(CLI::App* app) : m_params("")
        {
            add_scorer_options(app, *this, ScorerMode::Required);
        }

        [[nodiscard]] auto scorer_params() const { return m_params; }

        template <typename T>
        friend CLI::Option* add_scorer_options(CLI::App* app, T& args, ScorerMode scorer_mode);

      private:
        ScorerParams m_params;
    };

    struct Thresholds {
        explicit Thresholds(CLI::App* app)
        {
            m_option = app->add_option(
                "-T,--thresholds", m_thresholds_filename, "File containing query thresholds");
        }

        [[nodiscard]] auto thresholds_file() const { return m_thresholds_filename; }
        [[nodiscard]] auto* thresholds_option() { return m_option; }

      private:
        std::optional<std::string> m_thresholds_filename;
        CLI::Option* m_option;
    };

    struct Verbose {
        explicit Verbose(CLI::App* app)
        {
            app->add_flag("-v,--verbose", m_verbose, "Print additional information");
        }

        [[nodiscard]] auto verbose() const -> bool { return m_verbose; }

        auto print_args(std::ostream& os) const -> std::ostream&
        {
            os << fmt::format("verbose: {}\n", verbose());
            return os;
        }

      private:
        bool m_verbose{false};
    };

    struct Threads {
        explicit Threads(CLI::App* app)
        {
            app->add_option("--threads", m_threads, "Number of threads");
        }

        [[nodiscard]] auto threads() const -> std::size_t { return m_threads; }

        auto print_args(std::ostream& os) const -> std::ostream&
        {
            os << fmt::format("threads: {}\n", threads());
            return os;
        }

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
        explicit Invert(CLI::App* app)
        {
            app->add_option("-i,--input", m_input_basename, "Forward index basename")->required();
            app->add_option("-o,--output", m_output_basename, "Output inverted index basename")
                ->required();
            app->add_option(
                "--term-count", m_term_count, "Number of distinct terms in the forward index");
        }

        [[nodiscard]] auto input_basename() const -> std::string { return m_input_basename; }
        [[nodiscard]] auto output_basename() const -> std::string { return m_output_basename; }
        [[nodiscard]] auto term_count() const -> std::optional<std::uint32_t>
        {
            return m_term_count;
        }

        /// Transform paths for `shard`.
        void apply_shard(Shard_Id shard)
        {
            m_input_basename = expand_shard(m_input_basename, shard);
            m_output_basename = expand_shard(m_output_basename, shard);
        }

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
        explicit CreateWandData(CLI::App* app) : m_params("")
        {
            app->add_option("-c,--collection", m_input_basename, "Collection basename")->required();
            app->add_option("-o,--output", m_output, "Output filename")->required();
            auto block_group = app->add_option_group("blocks");
            auto block_size_opt = block_group->add_option(
                "-b,--block-size", m_fixed_block_size, "Block size for fixed-length blocks");
            auto block_lambda_opt =
                block_group
                    ->add_option("-l,--lambda", m_lambda, "Lambda parameter for variable blocks")
                    ->excludes(block_size_opt);
            block_group->require_option();

            app->add_flag("--compress", m_compress, "Compress additional data");
            app->add_flag("--quantize", m_quantize, "Quantize scores");
            add_scorer_options(app, *this, ScorerMode::Required);
            app->add_flag("--range", m_range, "Create docid-range based data")
                ->excludes(block_size_opt)
                ->excludes(block_lambda_opt);
            app->add_option(
                "--terms-to-drop",
                m_terms_to_drop_filename,
                "A filename containing a list of term IDs that we want to drop");
        }

        [[nodiscard]] auto input_basename() const -> std::string { return m_input_basename; }
        [[nodiscard]] auto output() const -> std::string { return m_output; }
        [[nodiscard]] auto scorer_params() const { return m_params; }
        [[nodiscard]] auto block_size() const -> BlockSize
        {
            if (m_lambda) {
                spdlog::info("Lambda {}", *m_lambda);
                return VariableBlock(*m_lambda);
            }
            spdlog::info("Fixed block size: {}", *m_fixed_block_size);
            return FixedBlock(*m_fixed_block_size);
        }
        [[nodiscard]] auto dropped_term_ids() const
        {
            std::ifstream dropped_terms_file(m_terms_to_drop_filename);
            std::unordered_set<size_t> dropped_term_ids;
            copy(
                std::istream_iterator<size_t>(dropped_terms_file),
                std::istream_iterator<size_t>(),
                std::inserter(dropped_term_ids, dropped_term_ids.end()));
            return dropped_term_ids;
        }
        [[nodiscard]] auto lambda() const -> std::optional<float> { return m_lambda; }
        [[nodiscard]] auto compress() const -> bool { return m_compress; }
        [[nodiscard]] auto range() const -> bool { return m_range; }
        [[nodiscard]] auto quantize() const -> bool { return m_quantize; }

        /// Transform paths for `shard`.
        void apply_shard(Shard_Id shard)
        {
            m_input_basename = expand_shard(m_input_basename, shard);
            m_output = expand_shard(m_output, shard);
        }

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
        std::string m_terms_to_drop_filename;
    };

    struct ReorderDocuments {
        explicit ReorderDocuments(CLI::App* app)
        {
            app->add_option("-c,--collection", m_input_basename, "Collection basename")->required();
            auto output = app->add_option("-o,--output", m_output_basename, "Output basename");
            auto docs_opt = app->add_option("--documents", m_doclex, "Document lexicon");
            app->add_option(
                   "--reordered-documents", m_reordered_doclex, "Reordered document lexicon")
                ->needs(docs_opt);
            auto methods = app->add_option_group("methods");
            auto random = methods
                              ->add_flag(
                                  "--random",
                                  m_random,
                                  "Assign IDs randomly. You can use --seed for deterministic "
                                  "results.")
                              ->needs(output);
            auto mapping = methods->add_option(
                "--from-mapping",
                m_mapping,
                "Use the mapping defined in this new-line delimited text file");
            auto feature =
                methods->add_option("--by-feature", m_feature, "Order by URLs from this file");
            auto bp = methods->add_flag(
                "--recursive-graph-bisection,--bp", m_bp, "Use recursive graph bisection algorithm");
            methods->require_option(1);

            // --random
            app->add_option("--seed", m_seed, "Random seed.")->needs(random);

            // --bp
            app->add_option("--store-fwdidx", m_output_fwd, "Output basename (forward index)")->needs(bp);
            app->add_option("--fwdidx", m_input_fwd, "Use this forward index")->needs(bp);
            app->add_option("-m,--min-len", m_min_len, "Minimum list threshold")->needs(bp);
            auto optdepth = app->add_option("-d,--depth", m_depth, "Recursion depth")
                                ->check(CLI::Range(1, 64))
                                ->needs(bp);
            auto optconf =
                app->add_option("--node-config", m_node_config, "Node configuration file")->needs(bp);
            app->add_flag("--nogb", m_nogb, "No VarIntGB compression in forward index")->needs(bp);
            app->add_flag("-p,--print", m_print, "Print ordering to standard output")->needs(bp);
            optconf->excludes(optdepth);
        }

        [[nodiscard]] auto input_basename() const -> std::string { return m_input_basename; }
        [[nodiscard]] auto output_basename() const -> std::optional<std::string>
        {
            return m_output_basename;
        }
        [[nodiscard]] auto document_lexicon() const -> std::optional<std::string>
        {
            return m_doclex;
        }
        [[nodiscard]] auto reordered_document_lexicon() const -> std::optional<std::string>
        {
            return m_reordered_doclex;
        }

        [[nodiscard]] auto random() const -> bool { return m_random; }
        [[nodiscard]] auto feature_file() const -> std::optional<std::string> { return m_feature; }
        [[nodiscard]] auto bp() const -> bool { return m_bp; }
        [[nodiscard]] auto mapping_file() const -> std::optional<std::string> { return m_mapping; }

        [[nodiscard]] auto seed() const -> std::uint64_t { return m_seed; }

        [[nodiscard]] auto input_collection() const -> binary_freq_collection
        {
            return binary_freq_collection(input_basename().c_str());
        }

        [[nodiscard]] auto input_fwd() const -> std::optional<std::string> { return m_input_fwd; }
        [[nodiscard]] auto output_fwd() const -> std::optional<std::string> { return m_output_fwd; }
        [[nodiscard]] auto min_length() const -> std::size_t { return m_min_len; }
        [[nodiscard]] auto depth() const -> std::optional<std::size_t> { return m_depth; }
        [[nodiscard]] auto nogb() const -> bool { return m_nogb; }
        [[nodiscard]] auto print() const -> bool { return m_print; }
        [[nodiscard]] auto node_config() const -> std::optional<std::string>
        {
            return m_node_config;
        }

        void apply_shard(Shard_Id shard)
        {
            m_input_basename = expand_shard(m_input_basename, shard);
            if (m_output_basename) {
                m_output_basename = expand_shard(*m_output_basename, shard);
            }
            if (m_output_fwd) {
                m_output_fwd = expand_shard(*m_output_fwd, shard);
            }
            if (m_input_fwd) {
                m_input_fwd = expand_shard(*m_input_fwd, shard);
            }
            if (m_doclex) {
                m_doclex = expand_shard(*m_doclex, shard);
                m_reordered_doclex = expand_shard(*m_reordered_doclex, shard);
            }
            if (m_mapping) {
                m_mapping = expand_shard(*m_mapping, shard);
            }
            if (m_feature) {
                m_feature = expand_shard(*m_feature, shard);
            }
        }

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
        explicit Separator(CLI::App* app, std::string default_separator = "\t")
            : m_separator(std::move(default_separator))
        {
            app->add_option("--sep", m_separator, "Separator string");
        }

        [[nodiscard]] auto separator() const -> std::string const& { return m_separator; }

      private:
        std::string m_separator;
    };

    struct PrintQueryId {
        explicit PrintQueryId(CLI::App* app)
        {
            app->add_flag(
                "--query-id",
                m_print_query_id,
                "Print query ID at the beginning of each line, separated by a colon");
        }

        [[nodiscard]] auto print_query_id() const -> bool { return m_print_query_id; }

      private:
        bool m_print_query_id = false;
    };

}  // namespace arg

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

using InvertArgs = Args<arg::Invert, arg::Threads, arg::BatchSize<100'000>>;
using ReorderDocuments = Args<arg::ReorderDocuments, arg::Threads>;
using CompressArgs =
    pisa::Args<arg::Compress, arg::Encoding, arg::Quantize<arg::ScorerMode::Optional>>;
using CreateWandDataArgs = pisa::Args<arg::CreateWandData>;

struct TailyStatsArgs: pisa::Args<arg::WandData<arg::WandMode::Required>, arg::Scorer> {
    explicit TailyStatsArgs(CLI::App* app)
        : pisa::Args<arg::WandData<arg::WandMode::Required>, arg::Scorer>(app)
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

struct TailyThresholds: pisa::Args<arg::Query<arg::QueryMode::Ranked>> {
    explicit TailyThresholds(CLI::App* app) : pisa::Args<arg::Query<arg::QueryMode::Ranked>>(app)
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

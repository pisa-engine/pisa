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

    struct WandData {
        explicit WandData(CLI::App* app)
        {
            auto* wand = app->add_option("-w,--wand", m_wand_data_path, "WAND data filename");
            app->add_flag("--compressed-wand", m_wand_compressed, "Compressed WAND data file")
                ->needs(wand);
        }
        [[nodiscard]] auto wand_data_path() const -> std::optional<std::string> const&
        {
            return m_wand_data_path;
        }
        [[nodiscard]] auto is_wand_compressed() const -> bool { return m_wand_compressed; }

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
            auto* terms = app->add_option("--terms", m_term_lexicon, "Term lexicon");
            app->add_option(
                   "--stopwords", m_stop_words, "List of blacklisted stop words to filter out")
                ->needs(terms);
            app->add_option("--stemmer", m_stemmer, "Stemmer type")->needs(terms);

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

      private:
        std::optional<std::string> m_query_file;
        int m_k = 0;
        std::optional<std::string> m_stop_words{std::nullopt};
        std::optional<std::string> m_stemmer{std::nullopt};
        std::optional<std::string> m_term_lexicon{std::nullopt};
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

    struct Quantize {
        explicit Quantize(CLI::App* app)
        {
            auto* wand = app->add_option("-w,--wand", m_wand_data_path, "WAND data filename");
            auto* scorer =
                app->add_option("-s,--scorer", m_params.m_name, "Query processing algorithm")->needs(wand);
            app->add_flag("--quantize", m_quantize, "Quantizes the scores")->needs(scorer);
            app->add_option("--bm25-k1", m_params.m_bm25_k1, "BM25 k1 parameter.");
            app->add_option("--bm25-b", m_params.m_bm25_b, "BM25 b parameter.");
            app->add_option("--pl2-c", m_params.m_pl2_c, "PL2 c parameter.");
            app->add_option("--qld-mu", m_params.m_qld_mu, "QLD mu parameter.");
        }

        [[nodiscard]] auto scorer_params() const -> std::optional<ScorerParams> const& 
        { 
            return m_params; 
        }

        [[nodiscard]] auto wand_data_path() const -> std::optional<std::string> const&
        {
            return m_wand_data_path;
        }
        [[nodiscard]] auto quantize() const { return m_quantize; }

      private:
        std::optional<ScorerParams> m_params;
        std::optional<std::string> m_wand_data_path;
        bool m_quantize = false;
    };

    struct Scorer {
        explicit Scorer(CLI::App* app)
        {
            app->add_option("-s,--scorer", m_params.m_name, "Query processing algorithm")->required();
            app->add_option("--bm25-k1", m_params.m_bm25_k1, "BM25 k1 parameter.");
            app->add_option("--bm25-b", m_params.m_bm25_b, "BM25 b parameter.");
            app->add_option("--pl2-c", m_params.m_pl2_c, "PL2 c parameter.");
            app->add_option("--qld-mu", m_params.m_qld_mu, "QLD mu parameter.");
        }

        [[nodiscard]] auto scorer_params() const { return m_params; }

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
        explicit CreateWandData(CLI::App* app)
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
            app->add_option("-s,--scorer", m_scorer_name, "Scorer function")->required();
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
        [[nodiscard]] auto scorer() const -> std::string { return m_scorer_name; }

        /// Transform paths for `shard`.
        void apply_shard(Shard_Id shard)
        {
            m_input_basename = expand_shard(m_input_basename, shard);
            m_output = expand_shard(m_output, shard);
        }

      private:
        std::optional<float> m_lambda{};
        std::optional<uint64_t> m_fixed_block_size{};
        std::string m_input_basename;
        std::string m_output;
        std::string m_scorer_name;
        bool m_compress = false;
        bool m_range = false;
        bool m_quantize = false;
        std::string m_terms_to_drop_filename;
    };

    struct RecursiveGraphBisection {
        explicit RecursiveGraphBisection(CLI::App* app)
        {
            app->add_option("-c,--collection", m_input_basename, "Collection basename")->required();
            app->add_option("-o,--output", m_output_basename, "Output basename");
            app->add_option("--store-fwdidx", m_output_fwd, "Output basename (forward index)");
            app->add_option("--fwdidx", m_input_fwd, "Use this forward index");
            auto docs_opt = app->add_option("--documents", m_doclex, "Documents lexicon");
            app->add_option(
                   "--reordered-documents", m_reordered_doclex, "Reordered documents lexicon")
                ->needs(docs_opt);
            app->add_option("-m,--min-len", m_min_len, "Minimum list threshold");
            auto optdepth =
                app->add_option("-d,--depth", m_depth, "Recursion depth")->check(CLI::Range(1, 64));
            auto optconf =
                app->add_option("--node-config", m_node_config, "Node configuration file");
            app->add_flag("--nogb", m_nogb, "No VarIntGB compression in forward index");
            app->add_flag("-p,--print", m_print, "Print ordering to standard output");
            optconf->excludes(optdepth);
        }

        [[nodiscard]] auto input_basename() const -> std::string { return m_input_basename; }
        [[nodiscard]] auto output_basename() const -> std::optional<std::string>
        {
            return m_output_basename;
        }
        [[nodiscard]] auto input_fwd() const -> std::optional<std::string> { return m_input_fwd; }
        [[nodiscard]] auto output_fwd() const -> std::optional<std::string> { return m_output_fwd; }
        [[nodiscard]] auto document_lexicon() const -> std::optional<std::string>
        {
            return m_doclex;
        }
        [[nodiscard]] auto reordered_document_lexicon() const -> std::optional<std::string>
        {
            return m_reordered_doclex;
        }
        [[nodiscard]] auto min_length() const -> std::size_t { return m_min_len; }
        [[nodiscard]] auto depth() const -> std::optional<std::size_t> { return m_depth; }
        [[nodiscard]] auto nogb() const -> bool { return m_nogb; }
        [[nodiscard]] auto print() const -> bool { return m_print; }
        [[nodiscard]] auto node_config() const -> std::optional<std::string>
        {
            return m_node_config;
        }

        auto print_args(std::ostream& os) const -> std::ostream&
        {
            os << fmt::format("input basename: {}\n", input_basename());
            os << fmt::format("output basename: {}\n", output_basename().value_or("-"));
            os << fmt::format("input fwd index: {}\n", input_fwd().value_or("-"));
            os << fmt::format("output fwd index: {}\n", output_fwd().value_or("-"));
            os << fmt::format("document lexicon: {}\n", document_lexicon().value_or("-"));
            os << fmt::format(
                "reordered document lexicon: {}\n", reordered_document_lexicon().value_or("-"));
            os << fmt::format("min. list length: {}\n", min_length());
            os << fmt::format("depth: {}\n", [this] {
                if (auto depth = this->depth(); depth) {
                    return fmt::format("{}", *depth);
                }
                return std::string("-");
            }());
            os << fmt::format("No compression: {}\n", nogb());
            os << fmt::format("Print: {}\n", print());
            os << fmt::format("Node config: {}\n", node_config().value_or("-"));
            return os;
        }

        /// Transform paths for `shard`.
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
        }

      private:
        std::string m_input_basename{};
        std::optional<std::string> m_output_basename{};
        std::optional<std::string> m_output_fwd{};
        std::optional<std::string> m_input_fwd{};
        std::optional<std::string> m_doclex{};
        std::optional<std::string> m_reordered_doclex{};
        std::size_t m_min_len = 0;
        std::optional<std::size_t> m_depth{};
        bool m_nogb = false;
        bool m_print = false;
        std::optional<std::string> m_node_config{};
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
using RecursiveGraphBisectionArgs = Args<arg::RecursiveGraphBisection, arg::Threads, arg::Verbose>;
using CompressArgs = pisa::Args<arg::Compress, arg::Encoding, arg::Quantize>;
using CreateWandDataArgs = pisa::Args<arg::CreateWandData>;

}  // namespace pisa

#pragma once

#include <optional>
#include <string>
#include <thread>

#include <CLI/CLI.hpp>
#include <range/v3/view/getlines.hpp>
#include <range/v3/view/transform.hpp>

#include "io.hpp"
#include "query/queries.hpp"

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
                app->add_option("-s,--scorer", m_scorer, "Query processing algorithm")->needs(wand);
            app->add_flag("--quantize", m_quantize, "Quantizes the scores")->needs(scorer);
        }

        [[nodiscard]] auto scorer() const -> std::optional<std::string> const& { return m_scorer; }
        [[nodiscard]] auto wand_data_path() const -> std::optional<std::string> const&
        {
            return m_wand_data_path;
        }
        [[nodiscard]] auto quantize() const { return m_quantize; }

      private:
        std::optional<std::string> m_scorer;
        std::optional<std::string> m_wand_data_path;
        bool m_quantize = false;
    };

    struct Scorer {
        explicit Scorer(CLI::App* app)
        {
            auto* opt =
                app->add_option("-s,--scorer", m_scorer, "Query processing algorithm")->required();
        }

        [[nodiscard]] auto scorer() const { return m_scorer; }

      private:
        std::string m_scorer;
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

    struct Threads {
        explicit Threads(CLI::App* app)
        {
            app->add_option("--threads", m_threads, "Number of threads");
        }

        [[nodiscard]] auto threads() const -> std::size_t { return m_threads; }

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

    struct InvertRequired {
        explicit InvertRequired(CLI::App* app)
        {
            app->add_option("-i,--input", m_input_basename, "Forward index basename")->required();
            app->add_option("-o,--output", m_output_basename, "Output inverted index basename")
                ->required();
        }

        [[nodiscard]] auto input_basename() const -> std::string { return m_input_basename; }
        [[nodiscard]] auto output_basename() const -> std::string { return m_output_basename; }

      private:
        std::string m_input_basename{};
        std::string m_output_basename{};
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

      private:
        std::string m_input_basename{};
        std::string m_output{};
        bool m_check = false;
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
        std::string m_node_config{};
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
};

using InvertArgs = Args<arg::InvertRequired, arg::Threads, arg::BatchSize<100'000>>;
using RecursiveGraphBisectionArgs = Args<arg::RecursiveGraphBisection, arg::Threads>;
using CompressArgs = pisa::Args<arg::Compress, arg::Encoding, arg::Quantize>;

}  // namespace pisa

#include "app.hpp"
#include "type_safe.hpp"

namespace pisa::arg {

Encoding::Encoding(CLI::App* app) {
    app->add_option("-e,--encoding", m_encoding, "Index encoding")->required();
}
auto Encoding::index_encoding() const -> std::string const& {
    return m_encoding;
}

Index::Index(CLI::App* app) : Encoding(app) {
    app->add_option("-i,--index", m_index, "Inverted index filename")->required();
}

auto Index::index_filename() const -> std::string const& {
    return m_index;
}

Analyzer::Analyzer(CLI::App* app) {
    app->add_option("--tokenizer", m_tokenizer, "Tokenizer")
        ->capture_default_str()
        ->check(CLI::IsMember(VALID_TOKENIZERS));
    app->add_flag("-H,--html", m_strip_html, "Strip HTML")->capture_default_str();
    app->add_option("-F,--token-filters", m_token_filters, "Token filters")
        ->check(CLI::IsMember(VALID_TOKEN_FILTERS));
    app->add_option(
        "--stopwords", m_stopwords_file, "Path to file containing a list of stop words to filter out"
    );
}

auto Analyzer::tokenizer() const -> std::unique_ptr<::pisa::Tokenizer> {
    if (m_tokenizer == "whitespace") {
        return std::make_unique<WhitespaceTokenizer>();
    }
    return std::make_unique<EnglishTokenizer>();
}

auto Analyzer::text_analyzer() const -> TextAnalyzer {
    TextAnalyzer analyzer(tokenizer());
    if (m_strip_html) {
        analyzer.emplace_text_filter<StripHtmlFilter>();
    }
    for (auto const& filter: m_token_filters) {
        if (filter == "lowercase") {
            analyzer.emplace_token_filter<LowercaseFilter>();
        } else if (filter == "porter2") {
            analyzer.emplace_token_filter<Porter2Stemmer>();
        } else if (filter == "krovetz") {
            analyzer.emplace_token_filter<KrovetzStemmer>();
        }
    }
    if (m_stopwords_file) {
        std::unordered_set<std::string> stopwords{};
        std::ifstream is(*m_stopwords_file);
        io::for_each_line(is, [&](auto&& word) { stopwords.insert(word); });
        analyzer.emplace_token_filter<StopWordRemover>(std::move(stopwords));
    }
    return analyzer;
}

const std::set<std::string> Analyzer::VALID_TOKENIZERS = {"whitespace", "english"};
const std::set<std::string> Analyzer::VALID_TOKEN_FILTERS = {"lowercase", "porter2", "krovetz"};

LogLevel::LogLevel(CLI::App* app) {
    app->add_option("-L,--log-level", m_level, "Log level")
        ->capture_default_str()
        ->check(CLI::IsMember(VALID_LEVELS));
}

auto LogLevel::log_level() const -> spdlog::level::level_enum {
    return ENUM_MAP.at(m_level);
}

const std::set<std::string> LogLevel::VALID_LEVELS = {
    "trace", "debug", "info", "warn", "err", "critical", "off"
};
const std::map<std::string, spdlog::level::level_enum> LogLevel::ENUM_MAP = {
    {"trace", spdlog::level::level_enum::trace},
    {"debug", spdlog::level::level_enum::debug},
    {"info", spdlog::level::level_enum::info},
    {"warn", spdlog::level::level_enum::warn},
    {"err", spdlog::level::level_enum::err},
    {"critical", spdlog::level::level_enum::critical},
    {"off", spdlog::level::level_enum::off}
};

Algorithm::Algorithm(CLI::App* app) {
    app->add_option("-a,--algorithm", m_algorithm, "Query processing algorithm")->required();
}

auto Algorithm::algorithm() const -> std::string const& {
    return m_algorithm;
}

Quantize::Quantize(CLI::App* app) : m_params("") {
    auto* wand = app->add_option("-w,--wand", m_wand_data_path, "WAND data filename");
    auto* scorer = add_scorer_options(app, *this, ScorerMode::Optional);
    auto* quant = app->add_option(
        "--quantize", m_quantization_bits, "Quantizes the scores using this many bits"
    );
    wand->needs(scorer);
    scorer->needs(wand);
    scorer->needs(quant);
    quant->needs(scorer);
}

auto Quantize::scorer_params() const -> ScorerParams {
    return m_params;
}

auto Quantize::wand_data_path() const -> std::optional<std::string> const& {
    return m_wand_data_path;
}

auto Quantize::quantization_bits() const -> std::optional<Size> {
    if (m_quantization_bits.has_value()) {
        return Size(*m_quantization_bits);
    }
    return std::nullopt;
}

Scorer::Scorer(CLI::App* app) : m_params("") {
    add_scorer_options(app, *this, ScorerMode::Required);
}

auto Scorer::scorer_params() const -> ScorerParams {
    return m_params;
}

Thresholds::Thresholds(CLI::App* app) {
    m_option =
        app->add_option("-T,--thresholds", m_thresholds_filename, "File containing query thresholds");
}

auto Thresholds::thresholds_file() const -> std::optional<std::string> const& {
    return m_thresholds_filename;
}

auto Thresholds::thresholds_option() -> CLI::Option* {
    return m_option;
}

Verbose::Verbose(CLI::App* app) {
    app->add_flag("-v,--verbose", m_verbose, "Print additional information");
}

auto Verbose::verbose() const -> bool {
    return m_verbose;
}

auto Verbose::print_args(std::ostream& os) const -> std::ostream& {
    os << fmt::format("verbose: {}\n", verbose());
    return os;
}

Threads::Threads(CLI::App* app) {
    app->add_option("-j,--threads", m_threads, "Number of threads");
}

auto Threads::threads() const -> std::size_t {
    return m_threads;
}

auto Threads::print_args(std::ostream& os) const -> std::ostream& {
    os << fmt::format("threads: {}\n", threads());
    return os;
}

Invert::Invert(CLI::App* app) {
    app->add_option("-i,--input", m_input_basename, "Forward index basename")->required();
    app->add_option("-o,--output", m_output_basename, "Output inverted index basename")->required();
    app->add_option("--term-count", m_term_count, "Number of distinct terms in the forward index");
}

auto Invert::input_basename() const -> std::string {
    return m_input_basename;
}

auto Invert::output_basename() const -> std::string {
    return m_output_basename;
}

auto Invert::term_count() const -> std::optional<std::uint32_t> {
    return m_term_count;
}

/// Transform paths for `shard`.
void Invert::apply_shard(Shard_Id shard) {
    m_input_basename = expand_shard(m_input_basename, shard);
    m_output_basename = expand_shard(m_output_basename, shard);
}

CreateWandData::CreateWandData(CLI::App* app) : m_params("") {
    app->add_option("-c,--collection", m_input_basename, "Collection basename")->required();
    app->add_option("-o,--output", m_output, "Output filename")->required();
    auto block_group = app->add_option_group("blocks");
    auto block_size_opt = block_group->add_option(
        "-b,--block-size", m_fixed_block_size, "Block size for fixed-length blocks"
    );
    auto block_lambda_opt =
        block_group->add_option("-l,--lambda", m_lambda, "Lambda parameter for variable blocks")
            ->excludes(block_size_opt);
    block_group->require_option();

    auto* quant = app->add_option(
        "--quantize", m_quantization_bits, "Quantizes the scores using this many bits"
    );
    app->add_flag("--compress", m_compress, "Compress additional data")->needs(quant);
    add_scorer_options(app, *this, ScorerMode::Required);
    app->add_flag("--range", m_range, "Create docid-range based data")
        ->excludes(block_size_opt)
        ->excludes(block_lambda_opt);
    app->add_option(
        "--terms-to-drop",
        m_terms_to_drop_filename,
        "A filename containing a list of term IDs that we want to drop"
    );
}

auto CreateWandData::input_basename() const -> std::string {
    return m_input_basename;
}

auto CreateWandData::output() const -> std::string {
    return m_output;
}

auto CreateWandData::scorer_params() const -> ScorerParams {
    return m_params;
}

auto CreateWandData::block_size() const -> BlockSize {
    if (m_lambda) {
        spdlog::info("Lambda {}", *m_lambda);
        return VariableBlock(*m_lambda);
    }
    spdlog::info("Fixed block size: {}", *m_fixed_block_size);
    return FixedBlock(*m_fixed_block_size);
}

auto CreateWandData::dropped_term_ids() const -> std::unordered_set<size_t> {
    std::unordered_set<size_t> dropped_term_ids;
    if (!m_terms_to_drop_filename) {
        return dropped_term_ids;
    }
    std::ifstream dropped_terms_file(*m_terms_to_drop_filename);
    copy(
        std::istream_iterator<size_t>(dropped_terms_file),
        std::istream_iterator<size_t>(),
        std::inserter(dropped_term_ids, dropped_term_ids.end())
    );
    return dropped_term_ids;
}

auto CreateWandData::lambda() const -> std::optional<float> {
    return m_lambda;
}

auto CreateWandData::compress() const -> bool {
    return m_compress;
}

auto CreateWandData::range() const -> bool {
    return m_range;
}

auto CreateWandData::quantization_bits() const -> std::optional<Size> {
    if (m_quantization_bits.has_value()) {
        return Size(*m_quantization_bits);
    }
    return std::nullopt;
}

/// Transform paths for `shard`.
void CreateWandData::apply_shard(Shard_Id shard) {
    m_input_basename = expand_shard(m_input_basename, shard);
    m_output = expand_shard(m_output, shard);
}

ReorderDocuments::ReorderDocuments(CLI::App* app) {
    app->add_option("-c,--collection", m_input_basename, "Collection basename")->required();
    auto output = app->add_option("-o,--output", m_output_basename, "Output basename");
    auto docs_opt = app->add_option("--documents", m_doclex, "Document lexicon");
    app->add_option("--reordered-documents", m_reordered_doclex, "Reordered document lexicon")
        ->needs(docs_opt);
    auto methods = app->add_option_group("methods");
    auto random = methods
                      ->add_flag(
                          "--random",
                          m_random,
                          "Assign IDs randomly. You can use --seed for deterministic "
                          "results."
                      )
                      ->needs(output);
    methods->add_option(
        "--from-mapping", m_mapping, "Use the mapping defined in this new-line delimited text file"
    );
    methods->add_option("--by-feature", m_feature, "Order by URLs from this file");
    auto bp = methods->add_flag(
        "--recursive-graph-bisection,--bp", m_bp, "Use recursive graph bisection algorithm"
    );
    methods->require_option(1);

    // --random
    app->add_option("--seed", m_seed, "Random seed.")->needs(random);

    // --bp
    app->add_option("--store-fwdidx", m_output_fwd, "Output basename (forward index)")->needs(bp);
    app->add_option("--fwdidx", m_input_fwd, "Use this forward index")->needs(bp);
    app->add_option("-m,--min-len", m_min_len, "Minimum list threshold")->needs(bp);
    auto optdepth =
        app->add_option("-d,--depth", m_depth, "Recursion depth")->check(CLI::Range(1, 64))->needs(bp);
    auto optconf =
        app->add_option("--node-config", m_node_config, "Node configuration file")->needs(bp);
    app->add_flag("--nogb", m_nogb, "No VarIntGB compression in forward index")->needs(bp);
    app->add_flag("-p,--print", m_print, "Print ordering to standard output")->needs(bp);
    optconf->excludes(optdepth);
}

auto ReorderDocuments::input_basename() const -> std::string {
    return m_input_basename;
}

auto ReorderDocuments::output_basename() const -> std::optional<std::string> {
    return m_output_basename;
}

auto ReorderDocuments::document_lexicon() const -> std::optional<std::string> {
    return m_doclex;
}

auto ReorderDocuments::reordered_document_lexicon() const -> std::optional<std::string> {
    return m_reordered_doclex;
}

auto ReorderDocuments::random() const -> bool {
    return m_random;
}

auto ReorderDocuments::feature_file() const -> std::optional<std::string> {
    return m_feature;
}

auto ReorderDocuments::bp() const -> bool {
    return m_bp;
}

auto ReorderDocuments::mapping_file() const -> std::optional<std::string> {
    return m_mapping;
}

auto ReorderDocuments::seed() const -> std::uint64_t {
    return m_seed;
}

auto ReorderDocuments::input_collection() const -> binary_freq_collection {
    return binary_freq_collection(input_basename().c_str());
}

auto ReorderDocuments::input_fwd() const -> std::optional<std::string> {
    return m_input_fwd;
}
auto ReorderDocuments::output_fwd() const -> std::optional<std::string> {
    return m_output_fwd;
}

auto ReorderDocuments::min_length() const -> std::size_t {
    return m_min_len;
}

auto ReorderDocuments::depth() const -> std::optional<std::size_t> {
    return m_depth;
}

auto ReorderDocuments::nogb() const -> bool {
    return m_nogb;
}

auto ReorderDocuments::print() const -> bool {
    return m_print;
}

auto ReorderDocuments::node_config() const -> std::optional<std::string> {
    return m_node_config;
}

void ReorderDocuments::apply_shard(Shard_Id shard) {
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

Separator::Separator(CLI::App* app, std::string default_separator)
    : m_separator(std::move(default_separator)) {
    app->add_option("--sep", m_separator, "Separator string");
}

auto Separator::separator() const -> std::string const& {
    return m_separator;
}

PrintQueryId::PrintQueryId(CLI::App* app) {
    app->add_flag(
        "--query-id",
        m_print_query_id,
        "Print query ID at the beginning of each line, separated by a colon"
    );
}

auto PrintQueryId::print_query_id() const -> bool {
    return m_print_query_id;
}

}  // namespace pisa::arg

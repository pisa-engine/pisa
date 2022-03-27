#include "sharding.hpp"

#include <random>

#include <range/v3/action/shuffle.hpp>
#include <range/v3/algorithm/for_each.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/zip.hpp>

#include "algorithm.hpp"
#include "binary_collection.hpp"
#include "payload_vector.hpp"
#include "util/util.hpp"

namespace pisa {

using pisa::literals::operator""_d;
using pisa::literals::operator""_s;

auto format_shard(std::string_view basename, Shard_Id shard, std::string_view suffix) -> std::string
{
    return fmt::format("{}.{:03d}{}", basename, shard.as_int(), suffix);
}

auto expand_shard(std::string_view basename, Shard_Id shard) -> std::string
{
    if (auto pos = basename.find("{}"); pos != std::string_view::npos) {
        return fmt::format(
            "{}{:03d}{}", basename.substr(0, pos), shard.as_int(), basename.substr(pos + 2));
    }
    return format_shard(basename, shard);
}

auto resolve_shards(std::string_view basename, std::string_view suffix) -> std::vector<Shard_Id>
{
    Shard_Id shard{0};
    std::vector<Shard_Id> shards;
    while (true) {
        boost::filesystem::path p(fmt::format("{}{}", expand_shard(basename, shard), suffix));
        if (boost::filesystem::exists(p)) {
            shards.push_back(shard);
            shard += 1;
        } else {
            if (shards.empty()) {
                spdlog::error("No shards found (failed at finding: {})", p.string());
            }
            break;
        }
    }
    return shards;
}

auto mapping_from_files(std::istream* full_titles, gsl::span<std::istream*> shard_titles)
    -> VecMap<Document_Id, Shard_Id>
{
    std::unordered_map<std::string, Shard_Id> map;
    auto shard_id = Shard_Id(0);
    for (auto* is: shard_titles) {
        io::for_each_line(*is, [&](auto const& title) {
            if (auto pos = map.find(title); pos == map.end()) {
                map[title] = shard_id;
            } else {
                spdlog::warn(
                    "Document {} already belongs to shard {}: mapping for shard {} ignored",
                    title,
                    pos->second.as_int(),
                    shard_id.as_int());
            }
        });
        shard_id += 1;
    }

    VecMap<Document_Id, Shard_Id> result;
    result.reserve(map.size());
    io::for_each_line(*full_titles, [&](auto const& title) {
        if (auto pos = map.find(title); pos != map.end()) {
            result.push_back(pos->second);
        } else {
            spdlog::warn("No shard assignment for document {}; will be assigned to shard 0", title);
            result.push_back(Shard_Id(0));
        }
    });
    return result;
}

auto mapping_from_files(std::string const& full_titles, gsl::span<std::string const> shard_titles)
    -> VecMap<Document_Id, Shard_Id>
{
    std::ifstream fis(full_titles);
    std::vector<std::unique_ptr<std::istream>> shard_is;
    for (auto const& shard_file: shard_titles) {
        if (!boost::filesystem::exists(shard_file)) {
            throw std::invalid_argument(fmt::format("Shard file does not exist: {}", shard_file));
        }
        shard_is.push_back(std::make_unique<std::ifstream>(shard_file));
    }
    auto title_files =
        shard_is | ranges::views::transform([](auto& is) { return is.get(); }) | ranges::to_vector;
    return mapping_from_files(&fis, gsl::make_span(title_files));
}

auto create_random_mapping(int document_count, int shard_count, std::optional<std::uint64_t> seed)
    -> VecMap<Document_Id, Shard_Id>
{
    std::random_device rd;
    std::mt19937 g(seed.value_or(rd()));
    VecMap<Document_Id, Shard_Id> mapping(document_count);
    auto shard_size = ceil_div(document_count, shard_count);
    auto documents = ranges::views::iota(0_d, Document_Id{document_count}) | ranges::to_vector
        | ranges::actions::shuffle(g);

    ranges::for_each(
        ranges::views::zip(
            ranges::views::iota(0_s, Shard_Id(shard_count)),
            ranges::views::chunk(documents, shard_size)),
        [&](auto&& entry) {
            auto&& [shard_id, shard_documents] = entry;
            for (auto document: shard_documents) {
                mapping[document] = shard_id;
            }
        });
    return mapping;
}

auto create_random_mapping(
    std::string const& input_basename, int shard_count, std::optional<std::uint64_t> seed)
    -> VecMap<Document_Id, Shard_Id>
{
    auto document_count = *(*binary_collection(input_basename.c_str()).begin()).begin();
    return create_random_mapping(document_count, shard_count, seed);
}

void copy_sequence(std::istream& is, std::ostream& os)
{
    uint32_t len;
    is.read(reinterpret_cast<char*>(&len), sizeof(len));
    os.write(reinterpret_cast<char const*>(&len), sizeof(len));
    std::vector<char> buf(len * sizeof(uint32_t));
    is.read(buf.data(), buf.size());
    os.write(buf.data(), buf.size());
}

void rearrange_sequences(
    std::string const& input_basename,
    std::string const& output_basename,
    VecMap<Document_Id, Shard_Id>& mapping,
    std::optional<Shard_Id> shard_count)
{
    spdlog::info("Rearranging documents");
    if (not shard_count) {
        *shard_count = *std::max_element(mapping.begin(), mapping.end()) + 1;
    }
    std::ifstream is(input_basename);
    std::ifstream dis(fmt::format("{}.documents", input_basename));
    std::ifstream uis(fmt::format("{}.urls", input_basename));
    VecMap<Shard_Id, std::ofstream> os;
    VecMap<Shard_Id, std::ofstream> dos;
    VecMap<Shard_Id, std::ofstream> uos;
    for (auto shard: ranges::views::iota(0_s, *shard_count)) {
        spdlog::debug("Initializing file for shard {}", shard.as_int());
        auto filename = fmt::format("{}.{:03d}", output_basename, shard.as_int());
        os.emplace_back(filename);
        constexpr std::array<char, 8> zero{0, 0, 0, 0, 0, 0, 0, 0};
        os.back().write(zero.data(), zero.size());
        dos.emplace_back(fmt::format("{}.documents", filename));
        uos.emplace_back(fmt::format("{}.urls", filename));
    }
    is.ignore(8);
    VecMap<Shard_Id, std::uint32_t> shard_sizes(shard_count->as_int(), 0U);
    spdlog::info("Copying sequences and titles");
    int idx = 0;
    for (auto shard: mapping) {
        spdlog::debug("Copying sequence {} to shard {}", idx++, shard.as_int());
        copy_sequence(is, os[shard]);
        {
            std::string title;
            std::getline(dis, title);
            dos[shard] << title << '\n';
        }
        {
            std::string url;
            std::getline(uis, url);
            uos[shard] << url << '\n';
        }
        shard_sizes[shard]++;
    }
    spdlog::info("Writing sizes");
    for (auto&& [o, size]: ranges::views::zip(os.as_vector(), shard_sizes.as_vector())) {
        o.seekp(0);
        uint32_t one = 1;
        o.write(reinterpret_cast<char const*>(&one), sizeof(uint32_t));
        o.write(reinterpret_cast<char const*>(&size), sizeof(uint32_t));
    }
}

void process_shard(
    std::string const& input_basename,
    std::string const& output_basename,
    Shard_Id shard_id,
    VecMap<Term_Id, std::string> const& terms)
{
    auto basename = fmt::format("{}.{:03d}", output_basename, shard_id.as_int());
    auto shard = writable_binary_collection(basename.c_str());

    spdlog::debug("[Shard {}] Calculating term occurrences", shard_id.as_int());
    std::vector<uint32_t> has_term(terms.size(), 0U);
    for (auto iter = ++shard.begin(); iter != shard.end(); ++iter) {
        for (auto term: *iter) {
            has_term[term] = 1U;
        }
    }

    spdlog::debug("[Shard {}] Writing terms", shard_id.as_int());
    {
        std::ofstream tos(fmt::format("{}.terms", basename));
        for (auto&& [term, occurs]: ranges::views::zip(terms.as_vector(), has_term)) {
            if (occurs != 0U) {
                tos << term << '\n';
            }
        }
    }

    {
        spdlog::debug("[Shard {}] Creating term lexicon", shard_id.as_int());
        std::ifstream title_is(fmt::format("{}.terms", basename));
        encode_payload_vector(
            std::istream_iterator<io::Line>(title_is), std::istream_iterator<io::Line>())
            .to_file(fmt::format("{}.termlex", basename));
    }

    spdlog::debug("[Shard {}] Remapping term IDs", shard_id.as_int());
    if (auto pos = std::find(has_term.begin(), has_term.end(), 1U); pos != has_term.end()) {
        *pos = 0U;
    }
    std::partial_sum(has_term.begin(), has_term.end(), has_term.begin());
    auto remapped_term_id = [&](auto term) { return has_term[term]; };
    spdlog::debug("[Shard {}] Writing remapped collection", shard_id.as_int());
    for (auto iter = ++shard.begin(); iter != shard.end(); ++iter) {
        for (auto& term: *iter) {
            term = remapped_term_id(term);
        }
    }
    {
        spdlog::debug("[Shard {}] Creating document lexicon", shard_id.as_int());
        std::ifstream os(fmt::format("{}.documents", basename));
        encode_payload_vector(std::istream_iterator<io::Line>(os), std::istream_iterator<io::Line>())
            .to_file(fmt::format("{}.doclex", basename));
    }
    spdlog::info("Shard {} finished.", shard_id.as_int());
}

void partition_fwd_index(
    std::string const& input_basename,
    std::string const& output_basename,
    VecMap<Document_Id, Shard_Id>& mapping)
{
    auto terms = read_string_vec_map<Term_Id>(fmt::format("{}.terms", input_basename));
    auto shard_count = *std::max_element(mapping.begin(), mapping.end()) + 1;
    auto shard_ids = ranges::views::iota(0_s, shard_count) | ranges::to_vector;
    rearrange_sequences(input_basename, output_basename, mapping, shard_count);
    spdlog::info("Remapping shards");
    pisa::for_each(pisa::execution::par_unseq, shard_ids.begin(), shard_ids.end(), [&](auto&& id) {
        process_shard(input_basename, output_basename, id, terms);
    });
}

}  // namespace pisa

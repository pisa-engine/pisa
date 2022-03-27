#pragma once

#include <optional>

#include <gsl/span>
#include <spdlog/spdlog.h>

#include "io.hpp"
#include "type_safe.hpp"
#include "vec_map.hpp"

namespace pisa {

[[nodiscard]] auto
format_shard(std::string_view basename, Shard_Id shard, std::string_view suffix = {}) -> std::string;

[[nodiscard]] auto expand_shard(std::string_view basename, Shard_Id shard) -> std::string;

auto resolve_shards(std::string_view basename, std::string_view suffix = {}) -> std::vector<Shard_Id>;

auto mapping_from_files(std::istream* full_titles, gsl::span<std::istream*> shard_titles)
    -> VecMap<Document_Id, Shard_Id>;

auto mapping_from_files(std::string const& full_titles, gsl::span<std::string const> shard_titles)
    -> VecMap<Document_Id, Shard_Id>;

auto create_random_mapping(
    int document_count, int shard_count, std::optional<std::uint64_t> seed = std::nullopt)
    -> VecMap<Document_Id, Shard_Id>;

auto create_random_mapping(
    std::string const& input_basename,
    int shard_count,
    std::optional<std::uint64_t> seed = std::nullopt) -> VecMap<Document_Id, Shard_Id>;

void copy_sequence(std::istream& is, std::ostream& os);

void rearrange_sequences(
    std::string const& input_basename,
    std::string const& output_basename,
    VecMap<Document_Id, Shard_Id>& mapping,
    std::optional<Shard_Id> shard_count = std::nullopt);

void process_shard(
    std::string const& input_basename,
    std::string const& output_basename,
    Shard_Id shard_id,
    VecMap<Term_Id, std::string> const& terms);

void partition_fwd_index(
    std::string const& input_basename,
    std::string const& output_basename,
    VecMap<Document_Id, Shard_Id>& mapping);

}  // namespace pisa

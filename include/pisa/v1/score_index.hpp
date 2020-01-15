#pragma once

#include <string>

#include <boost/iterator/counting_iterator.hpp>

#include "v1/index_metadata.hpp"

namespace pisa::v1 {

struct FixedBlock {
    std::size_t size;
};

struct VariableBlock {
    float lambda;
};

using BlockType = std::variant<FixedBlock, VariableBlock>;

template <typename CharT,
          typename Index,
          typename Writer,
          typename Scorer,
          typename Quantizer,
          typename Callback>
auto score_index(Index const& index,
                 std::basic_ostream<CharT>& os,
                 Writer writer,
                 Scorer scorer,
                 Quantizer&& quantizer,
                 Callback&& callback) -> std::vector<std::size_t>
{
    PostingBuilder<typename Writer::value_type> score_builder(writer);
    score_builder.write_header(os);
    std::for_each(boost::counting_iterator<TermId>(0),
                  boost::counting_iterator<TermId>(index.num_terms()),
                  [&](auto term) {
                      for_each(index.scoring_cursor(term, scorer), [&](auto&& cursor) {
                          score_builder.accumulate(quantizer(cursor.payload()));
                      });
                      score_builder.flush_segment(os);
                      callback();
                  });
    return std::move(score_builder.offsets());
}

template <typename CharT, typename Index, typename Writer, typename Scorer>
auto score_index(Index const& index, std::basic_ostream<CharT>& os, Writer writer, Scorer scorer)
    -> std::vector<std::size_t>
{
    PostingBuilder<typename Writer::value_type> score_builder(writer);
    score_builder.write_header(os);
    std::for_each(boost::counting_iterator<TermId>(0),
                  boost::counting_iterator<TermId>(index.num_terms()),
                  [&](auto term) {
                      for_each(index.scoring_cursor(term, scorer),
                               [&](auto& cursor) { score_builder.accumulate(cursor.payload()); });
                      score_builder.flush_segment(os);
                  });
    return std::move(score_builder.offsets());
}

auto score_index(IndexMetadata meta, std::size_t threads) -> IndexMetadata;
auto bm_score_index(IndexMetadata meta, BlockType block_type, std::size_t threads) -> IndexMetadata;

} // namespace pisa::v1

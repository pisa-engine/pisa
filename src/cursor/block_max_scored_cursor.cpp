#include "cursor/block_max_scored_cursor.hpp"

namespace pisa {

#define LOOP_BODY(R, DATA, T)                                                           \
    struct T;                                                                           \
                                                                                        \
    template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                wand_data<wand_data_raw>,               \
                                                bm25<wand_data<wand_data_raw>>>(        \
        block_freq_index<T, false> const &,                                             \
        wand_data<wand_data_raw> const &,                                               \
        bm25<wand_data<wand_data_raw>> const &,                                         \
        Query);                                                                         \
    template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                wand_data<wand_data_raw>,               \
                                                dph<wand_data<wand_data_raw>>>(         \
        block_freq_index<T, false> const &,                                             \
        wand_data<wand_data_raw> const &,                                               \
        dph<wand_data<wand_data_raw>> const &,                                          \
        Query);                                                                         \
    template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                wand_data<wand_data_raw>,               \
                                                pl2<wand_data<wand_data_raw>>>(         \
        block_freq_index<T, false> const &,                                             \
        wand_data<wand_data_raw> const &,                                               \
        pl2<wand_data<wand_data_raw>> const &,                                          \
        Query);                                                                         \
    template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                wand_data<wand_data_raw>,               \
                                                qld<wand_data<wand_data_raw>>>(         \
        block_freq_index<T, false> const &,                                             \
        wand_data<wand_data_raw> const &,                                               \
        qld<wand_data<wand_data_raw>> const &,                                          \
        Query);                                                                         \
                                                                                        \
    template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                wand_data<wand_data_compressed>,        \
                                                bm25<wand_data<wand_data_compressed>>>( \
        block_freq_index<T, false> const &,                                             \
        wand_data<wand_data_compressed> const &,                                        \
        bm25<wand_data<wand_data_compressed>> const &,                                  \
        Query);                                                                         \
    template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                wand_data<wand_data_compressed>,        \
                                                dph<wand_data<wand_data_compressed>>>(  \
        block_freq_index<T, false> const &,                                             \
        wand_data<wand_data_compressed> const &,                                        \
        dph<wand_data<wand_data_compressed>> const &,                                   \
        Query);                                                                         \
    template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                wand_data<wand_data_compressed>,        \
                                                pl2<wand_data<wand_data_compressed>>>(  \
        block_freq_index<T, false> const &,                                             \
        wand_data<wand_data_compressed> const &,                                        \
        pl2<wand_data<wand_data_compressed>> const &,                                   \
        Query);                                                                         \
    template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                wand_data<wand_data_compressed>,        \
                                                qld<wand_data<wand_data_compressed>>>(  \
        block_freq_index<T, false> const &,                                             \
        wand_data<wand_data_compressed> const &,                                        \
        qld<wand_data<wand_data_compressed>> const &,                                   \
        Query);                                                                         \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa

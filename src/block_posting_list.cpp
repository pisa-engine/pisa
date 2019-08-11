#include "block_posting_list.hpp"
#include "index_types.hpp"

namespace pisa {

#define LOOP_BODY(R, DATA, T)                                                        \
    struct T;                                                                        \
    template BlockPostingCursor<T> BlockPostingCursor<T>::from(std::uint8_t const *, \
                                                               std::uint64_t);       \
    template void BlockPostingCursor<T>::decode_docs_block(std::uint64_t);           \
    template void BlockPostingCursor<T>::decode_freqs_block();                       \
    template std::uint64_t BlockPostingCursor<T>::stats_freqs_size() const;          \
    template std::vector<typename BlockPostingCursor<T>::BlockData>                  \
    BlockPostingCursor<T>::get_blocks();
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa

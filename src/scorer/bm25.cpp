#include "scorer/bm25.hpp"
#include "wand_data.hpp"

namespace pisa {

template struct bm25<wand_data<wand_data_raw>>;
template struct bm25<wand_data<wand_data_compressed>>;

} // namespace pisa

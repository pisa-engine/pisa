#include "scorer/pl2.hpp"
#include "wand_data.hpp"

namespace pisa {

template struct pl2<wand_data<wand_data_raw>>;
template struct pl2<wand_data<wand_data_compressed>>;

} // namespace pisa

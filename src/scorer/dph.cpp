#include "scorer/dph.hpp"
#include "wand_data.hpp"

namespace pisa {

template struct dph<wand_data<wand_data_raw>>;
template struct dph<wand_data<wand_data_compressed>>;

} // namespace pisa

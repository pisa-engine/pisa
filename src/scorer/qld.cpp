#include "scorer/qld.hpp"
#include "wand_data.hpp"

namespace pisa {

template struct qld<wand_data<wand_data_raw>>;
template struct qld<wand_data<wand_data_compressed>>;

} // namespace pisa

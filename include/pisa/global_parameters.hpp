#pragma once
#include <cstdint>
namespace pisa {

struct global_parameters {
    global_parameters() = default;

    template <typename Visitor>
    void map(Visitor& visit)
    {
        visit(ef_log_sampling0, "ef_log_sampling0")(ef_log_sampling1, "ef_log_sampling1")(
            rb_log_rank1_sampling, "rb_log_rank1_sampling")(rb_log_sampling1, "rb_log_sampling1")(
            log_partition_size, "log_partition_size");
    }

    uint8_t ef_log_sampling0{9};
    uint8_t ef_log_sampling1{8};
    uint8_t rb_log_rank1_sampling{9};
    uint8_t rb_log_sampling1{8};
    uint8_t log_partition_size{7};
};

}  // namespace pisa

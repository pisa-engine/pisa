#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <thread>

#include "boost/lexical_cast.hpp"

namespace pisa {

class configuration {
  public:
    static configuration const& get()
    {
        static configuration instance;
        return instance;
    }

    double eps1;
    double eps2;
    double eps3;

    double eps1_wand;
    double eps2_wand;

    double fixed_cost_wand_partition;
    uint64_t fix_cost;
    uint64_t k;
    uint64_t block_size;

    size_t log_partition_size;
    size_t worker_threads;
    size_t threshold_wand_list;
    size_t reference_size;
    size_t quantization_bits;

    bool heuristic_greedy;

  private:
    configuration()
    {
        fillvar("PISA_K", k, 10);
        fillvar("PISA_BLOCK_SIZE", block_size, 5);
        fillvar("PISA_EPS1", eps1, 0.03);
        fillvar("PISA_EPS2", eps2, 0.3);
        fillvar("PISA_EPS3", eps3, 0.01);
        fillvar("PISA_FIXCOST", fix_cost, 64);
        fillvar("PISA_LOG_PART", log_partition_size, 7);
        fillvar("PISA_THRESHOLD_WAND_LIST", threshold_wand_list, 0);
        fillvar("PISA_THREADS", worker_threads, std::thread::hardware_concurrency());
        fillvar("PISA_HEURISTIC_GREEDY", heuristic_greedy, false);
        fillvar("PISA_FIXED_COST_WAND_PARTITION", fixed_cost_wand_partition, 12.0);
        fillvar("PISA_EPS1_WAND", eps1_wand, 0.01);
        fillvar("PISA_EPS2_WAND", eps2_wand, 0.4);
        fillvar("PISA_SCORE_REFERENCES_SIZE", reference_size, 128);
        fillvar("PISA_QUANTIZTION_BITS", quantization_bits, 8);
    }

    template <typename T, typename T2>
    void fillvar(const char* envvar, T& var, T2 def)
    {
        const char* val = std::getenv(envvar);
        if (!val || !strlen(val)) {
            var = def;
        } else {
            var = boost::lexical_cast<T>(val);
        }
    }
};

}  // namespace pisa

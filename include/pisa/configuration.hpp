#pragma once

#include <cstdlib>
#include <cstdint>
#include <memory>
#include <thread>

#include "boost/lexical_cast.hpp"

namespace pisa {

    class configuration {
    public:
        static configuration const& get() {
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



        bool heuristic_greedy;

    private:
        configuration()
        {
            fillvar("DS2I_K", k, 10);
            fillvar("DS2I_BLOCK_SIZE", block_size, 5);
            fillvar("DS2I_EPS1", eps1, 0.03);
            fillvar("DS2I_EPS2", eps2, 0.3);
            fillvar("DS2I_EPS3", eps3, 0.01);
            fillvar("DS2I_FIXCOST", fix_cost, 64);
            fillvar("DS2I_LOG_PART", log_partition_size, 7);
            fillvar("DS2I_THRESHOLD_WAND_LIST", threshold_wand_list, 0);
            fillvar("DS2I_THREADS", worker_threads, std::thread::hardware_concurrency());
            fillvar("DS2I_HEURISTIC_GREEDY", heuristic_greedy, false);
            fillvar("DS2I_FIXED_COST_WAND_PARTITION", fixed_cost_wand_partition, 12.0);
            fillvar("DS2I_EPS1_WAND", eps1_wand, 0.01);
            fillvar("DS2I_EPS2_WAND", eps2_wand, 0.4);
            fillvar("DS2I_SCORE_REFERENCES_SIZE", reference_size, 128);
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

}

#pragma once

#include <cstdlib>
#include <cstdint>
#include <memory>
#include <thread>

#define BOOST_THREAD_VERSION 4
#define BOOST_THREAD_PROVIDES_EXECUTORS
#include <boost/config.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/executors/basic_thread_pool.hpp>
#include <boost/thread/experimental/parallel/v2/task_region.hpp>

namespace ds2i {
    typedef boost::executors::basic_thread_pool executor_type;
    typedef boost::experimental::parallel::v2::task_region_handle_gen<executor_type>
        task_region_handle;
    using boost::experimental::parallel::v2::task_region;

    class configuration {
    public:
        static configuration const& get() {
            static configuration instance;
            return instance;
        }

        double eps1;
        double eps2;
        uint64_t fix_cost;

        size_t log_partition_size;
        size_t worker_threads;

        bool heuristic_greedy;

        std::unique_ptr<executor_type> executor;

    private:
        configuration()
        {
            fillvar("DS2I_EPS1", eps1, 0.03);
            fillvar("DS2I_EPS2", eps2, 0.3);
            fillvar("DS2I_FIXCOST", fix_cost, 64);
            fillvar("DS2I_LOG_PART", log_partition_size, 7);
            fillvar("DS2I_THREADS", worker_threads, std::thread::hardware_concurrency());
            fillvar("DS2I_HEURISTIC_GREEDY", heuristic_greedy, false);
            executor.reset(new executor_type(worker_threads));
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

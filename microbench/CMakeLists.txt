if (PISA_SYSTEM_GOOGLE_BENCHMARK)
    find_package(benchmark REQUIRED)
endif()

add_executable(topk_queue_bench topk_queue_bench.cpp)
target_link_libraries(topk_queue_bench pisa benchmark::benchmark)

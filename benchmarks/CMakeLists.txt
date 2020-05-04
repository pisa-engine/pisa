add_executable(index_perftest index_perftest.cpp)
target_link_libraries(index_perftest
  pisa
)

add_executable(perftest_interpolative perftest_interpolative.cpp)
target_link_libraries(perftest_interpolative
  pisa
)

add_executable(scan_perftest scan_perftest.cpp)
target_link_libraries(scan_perftest
  pisa
)

add_executable(bench-cursors bench_cursors.cpp)
target_link_libraries(bench-cursors
  pisa
  Catch2
)
target_compile_options(bench-cursors PRIVATE -Wno-return-type)
target_compile_definitions(bench-cursors PUBLIC CATCH_CONFIG_ENABLE_BENCHMARKING)

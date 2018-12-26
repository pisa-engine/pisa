add_executable(index_perftest index_perftest.cpp)
target_link_libraries(index_perftest
  mio
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  )

add_executable(perftest_interpolative perftest_interpolative.cpp)
target_link_libraries(perftest_interpolative
  mio
  FastPFor
  QMX
  simdcomp
  )

add_executable(selective_queries selective_queries.cpp)
target_link_libraries(selective_queries
  mio
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  )

add_executable(scan_perftest scan_perftest.cpp)
target_link_libraries(scan_perftest
  mio
  )

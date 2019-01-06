add_executable(index_perftest index_perftest.cpp)
target_link_libraries(index_perftest
  Boost::boost
  mio
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  ParallelSTL
  GSL
  )

add_executable(perftest_interpolative perftest_interpolative.cpp)
target_link_libraries(perftest_interpolative
  Boost::boost
  mio
  FastPFor
  QMX
  simdcomp
  GSL
  )

add_executable(selective_queries selective_queries.cpp)
target_link_libraries(selective_queries
  Boost::boost
  mio
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  ParallelSTL
  GSL
  )

add_executable(scan_perftest scan_perftest.cpp)
target_link_libraries(scan_perftest
  Boost::boost
  mio
  ParallelSTL
  GSL
  )

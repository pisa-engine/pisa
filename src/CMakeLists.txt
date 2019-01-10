add_executable(create_freq_index create_freq_index.cpp)
target_link_libraries(create_freq_index
  Boost::boost
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  CLI11
  mio
  ParallelSTL
  )

add_executable(optimal_hybrid_index optimal_hybrid_index.cpp)
target_link_libraries(optimal_hybrid_index
  Boost::boost
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  ${STXXL_LIBRARIES}
  mio
  ParallelSTL
)
set_target_properties(optimal_hybrid_index PROPERTIES
            CXX_STANDARD 14
)

add_executable(create_wand_data create_wand_data.cpp)
target_link_libraries(create_wand_data
  Boost::boost
  QMX
  simdcomp
  CLI11
  mio
  ParallelSTL
  )

add_executable(queries queries.cpp)
target_link_libraries(queries
  Boost::boost
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  CLI11
  mio
  ParallelSTL
  )


add_executable(profile_queries profile_queries.cpp)
target_link_libraries(profile_queries
  Boost::boost
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  mio
  ParallelSTL
  )

add_executable(profile_decoding profile_decoding.cpp)
target_link_libraries(profile_decoding
  Boost::boost
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  mio
  ParallelSTL
  )

add_executable(shuffle_docids shuffle_docids.cpp)
target_link_libraries(shuffle_docids
  Boost::boost
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  mio
  ParallelSTL
  )

add_executable(recursive_graph_bisection recursive_graph_bisection.cpp)
target_link_libraries(recursive_graph_bisection
  Boost::boost
  CLI11
  QMX
  ParallelSTL
  mio
)

add_executable(evaluate_collection_ordering evaluate_collection_ordering.cpp)
target_link_libraries(evaluate_collection_ordering
  Boost::boost
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  mio
  ParallelSTL
  )

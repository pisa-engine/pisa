add_executable(create_freq_index create_freq_index.cpp)
target_link_libraries(create_freq_index
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

add_executable(create_wand_data create_wand_data.cpp)
target_link_libraries(create_wand_data
  QMX
  simdcomp
  CLI11
  mio
  ParallelSTL
  )

add_executable(queries queries.cpp)
target_link_libraries(queries
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
  CLI11
  QMX
  ParallelSTL
  mio
)
set_target_properties(recursive_graph_bisection PROPERTIES
            CXX_STANDARD 17
)

add_executable(evaluate_collection_ordering evaluate_collection_ordering.cpp)
target_link_libraries(evaluate_collection_ordering
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  mio
  ParallelSTL
  )

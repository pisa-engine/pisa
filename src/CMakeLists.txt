add_executable(create_freq_index create_freq_index.cpp)
target_link_libraries(create_freq_index
  ${Boost_LIBRARIES}
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  CLI11
  )

add_executable(optimal_hybrid_index optimal_hybrid_index.cpp)
target_link_libraries(optimal_hybrid_index
  ${Boost_LIBRARIES}
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  ${STXXL_LIBRARIES}
)

add_executable(create_wand_data create_wand_data.cpp)
target_link_libraries(create_wand_data
  ${Boost_LIBRARIES}
  QMX
  simdcomp
  CLI11
  )

add_executable(queries queries.cpp)
target_link_libraries(queries
  ${Boost_LIBRARIES}
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  CLI11
  )


add_executable(profile_queries profile_queries.cpp)
target_link_libraries(profile_queries
  ${Boost_LIBRARIES}
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  )

add_executable(profile_decoding profile_decoding.cpp)
target_link_libraries(profile_decoding
  ${Boost_LIBRARIES}
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  )

add_executable(shuffle_docids shuffle_docids.cpp)
target_link_libraries(shuffle_docids
  ${Boost_LIBRARIES}
  FastPFor
  streamvbyte
  MaskedVByte
  QMX
  simdcomp
  )

add_executable(sample_index sample_index.cpp)
target_link_libraries(sample_index
  ${Boost_LIBRARIES}
  CLI11
  QMX
  )

add_executable(recursive_graph_bisection recursive_graph_bisection.cpp)
target_link_libraries(recursive_graph_bisection
  ${Boost_LIBRARIES}
  CLI11
  QMX
  ParallelSTL
  )

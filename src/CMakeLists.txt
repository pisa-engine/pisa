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
  GSL
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
  GSL
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
  GSL
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
  GSL
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
  GSL
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
  GSL
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
  GSL
  )

add_executable(recursive_graph_bisection recursive_graph_bisection.cpp)
target_link_libraries(recursive_graph_bisection
  Boost::boost
  CLI11
  QMX
  ParallelSTL
  mio
  GSL
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
  GSL
  )

add_executable(parse_collection parse_collection.cpp)
target_link_libraries(parse_collection
  ${Boost_LIBRARIES}
  CLI11
  mio
  ParallelSTL
  GSL
  gumbo::gumbo
  Porter2
  )
target_compile_features(parse_collection PRIVATE cxx_std_17)

add_executable(invert invert.cpp)
target_link_libraries(invert
  ${Boost_LIBRARIES}
  CLI11
  mio
  ParallelSTL
  GSL
  gumbo::gumbo
  )
target_compile_features(invert PRIVATE cxx_std_17)

add_executable(read_collection read_collection.cpp)
target_link_libraries(read_collection
  ${Boost_LIBRARIES}
  CLI11
  mio
  ParallelSTL
  GSL
  gumbo::gumbo
  )
target_compile_features(parse_collection PRIVATE cxx_std_17)

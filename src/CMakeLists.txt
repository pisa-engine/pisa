add_executable(create_freq_index create_freq_index.cpp)
target_link_libraries(create_freq_index
  ${Boost_LIBRARIES}
  FastPFor
  cxxopts
  )

add_executable(optimal_hybrid_index optimal_hybrid_index.cpp)
target_link_libraries(optimal_hybrid_index
  ${Boost_LIBRARIES}
  FastPFor
  ${STXXL_LIBRARIES}
)

add_executable(create_wand_data create_wand_data.cpp)
target_link_libraries(create_wand_data
  ${Boost_LIBRARIES}
  )

add_executable(queries queries.cpp)
target_link_libraries(queries
  ${Boost_LIBRARIES}
  FastPFor
  )


add_executable(profile_queries profile_queries.cpp)
target_link_libraries(profile_queries
  ${Boost_LIBRARIES}
  FastPFor
  )

add_executable(profile_decoding profile_decoding.cpp)
target_link_libraries(profile_decoding
  ${Boost_LIBRARIES}
  FastPFor
  )

add_executable(shuffle_docids shuffle_docids.cpp)
target_link_libraries(shuffle_docids
  ${Boost_LIBRARIES}
  FastPFor
  )
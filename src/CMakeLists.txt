add_executable(create_freq_index create_freq_index.cpp)
target_link_libraries(create_freq_index
  pisa
  CLI11
)

add_executable(optimal_hybrid_index optimal_hybrid_index.cpp)
target_include_directories(optimal_hybrid_index ${STXXL_INCLUDE_DIRS})
target_link_libraries(optimal_hybrid_index
  ${STXXL_LIBRARIES}
  pisa
)
set_target_properties(optimal_hybrid_index PROPERTIES
            CXX_STANDARD 14
)

add_executable(create_wand_data create_wand_data.cpp)
target_link_libraries(create_wand_data
  pisa
  CLI11
)

add_executable(queries queries.cpp)
target_link_libraries(queries
  pisa
  CLI11
)

add_executable(profile_queries profile_queries.cpp)
target_link_libraries(profile_queries
  pisa
)

add_executable(profile_decoding profile_decoding.cpp)
target_link_libraries(profile_decoding
  pisa
)

add_executable(shuffle_docids shuffle_docids.cpp)
target_link_libraries(shuffle_docids
  pisa
)

add_executable(recursive_graph_bisection recursive_graph_bisection.cpp)
target_link_libraries(recursive_graph_bisection
  pisa
  CLI11
)

add_executable(evaluate_collection_ordering evaluate_collection_ordering.cpp)
target_link_libraries(evaluate_collection_ordering
  pisa
  )

add_executable(parse_collection parse_collection.cpp)
target_link_libraries(parse_collection
  pisa
  CLI11
)

add_executable(invert invert.cpp)
target_link_libraries(invert
  CLI11
  pisa
)

add_executable(read_collection read_collection.cpp)
target_link_libraries(read_collection
  pisa
  CLI11
)

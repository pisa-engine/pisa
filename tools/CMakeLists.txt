add_executable(compress compress.cpp)
target_link_libraries(compress
  pisa
  CLI11
)

add_executable(optimal_hybrid_index optimal_hybrid_index.cpp)
target_include_directories(optimal_hybrid_index PRIVATE ${STXXL_INCLUDE_DIRS})
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

add_executable(evaluate_queries evaluate_queries.cpp)
target_link_libraries(evaluate_queries
  pisa
  CLI11
)

add_executable(thresholds thresholds.cpp)
target_link_libraries(thresholds
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
  CLI11
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

add_executable(partition_fwd_index partition_fwd_index.cpp)
target_link_libraries(partition_fwd_index
  pisa
  CLI11
)

add_executable(compute_intersection compute_intersection.cpp)
target_link_libraries(compute_intersection
  pisa
  CLI11
)

add_executable(lexicon lexicon.cpp)
target_link_libraries(lexicon
  pisa
  CLI11
)

add_executable(extract_topics extract_topics.cpp)
target_link_libraries(extract_topics
  pisa
  CLI11
)

add_executable(sample_inverted_index sample_inverted_index.cpp)
target_link_libraries(sample_inverted_index
  pisa
  CLI11
)

add_executable(map_queries map_queries.cpp)
target_link_libraries(map_queries
  pisa
  CLI11
)

add_executable(count_postings count_postings.cpp)
target_link_libraries(count_postings
  pisa
  CLI11
)

add_executable(selective_queries selective_queries.cpp)
target_link_libraries(selective_queries
  pisa
  CLI11
)

add_executable(shards shards.cpp)
target_link_libraries(shards
  pisa
  CLI11
)

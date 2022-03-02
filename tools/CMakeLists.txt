add_executable(compress_inverted_index compress_inverted_index.cpp)
target_link_libraries(compress_inverted_index
  pisa
  CLI11
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

add_executable(stem_queries stem_queries.cpp)
target_link_libraries(stem_queries
  pisa
  CLI11
)

add_executable(count-postings count_postings.cpp)
target_link_libraries(count-postings
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

add_executable(reorder-docids reorder_docids.cpp)
target_link_libraries(reorder-docids
  pisa
  CLI11
)

add_executable(kth_threshold kth_threshold.cpp)
target_link_libraries(kth_threshold
  pisa
  CLI11
)

add_executable(taily-stats taily_stats.cpp)
target_link_libraries(taily-stats
  pisa
  CLI11
)

add_executable(taily-thresholds taily_thresholds.cpp)
target_link_libraries(taily-thresholds
  pisa
  CLI11
)

add_executable(extract-maxscores extract_maxscores.cpp)
target_link_libraries(extract-maxscores
  pisa
  CLI11
)

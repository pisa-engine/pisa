set(CMAKE_RUNTIME_OUTPUT_DIRECTORY  ${CMAKE_BINARY_DIR}/test)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../external/Catch2/contrib)
include(CTest)
include(Catch)

add_library(testlib in_memory_index.cpp)
target_link_libraries(testlib pisa)

#file(GLOB TEST_SOURCES test_*.cpp)
file(GLOB TEST_SOURCES 
test_algorithm.cpp
test_bit_vector.cpp
test_block_codecs.cpp
test_block_freq_index.cpp
test_block_posting_list.cpp
test_bmw_queries.cpp
test_compact_elias_fano.cpp
test_compact_ranked_bitvector.cpp
test_compress.cpp
test_cow_string.cpp
test_cursors.cpp
test_forward_index.cpp
test_forward_index_builder.cpp
test_freq_index.cpp
test_html.cpp
test_indexed_sequence.cpp
test_intersection.cpp
test_invert.cpp
test_linear_quantizer.cpp
test_mapper.cpp
test_memory.cpp
test_memory_source.cpp
test_partition_fwd_index.cpp
test_partitioned_sequence.cpp
test_payload_vector.cpp
test_positive_sequence.cpp
test_ranked_queries.cpp
test_recursive_graph_bisection.cpp
test_sample_inverted_index.cpp
test_scorer.cpp
test_sequence_collection.cpp
test_stream_builder.cpp
test_strict_elias_fano.cpp
test_taily_stats.cpp
test_text_analyzer.cpp
test_token_filter.cpp
test_token_stream.cpp
test_tokenizer.cpp
test_topk_queue.cpp
test_trec_topic_reader.cpp
test_uniform_partitioned_sequence.cpp
test_wand_data.cpp
	)
foreach(TEST_SRC ${TEST_SOURCES})
  get_filename_component (TEST_SRC_NAME ${TEST_SRC} NAME_WE)
  add_executable(${TEST_SRC_NAME} ${TEST_SRC})
  target_link_libraries(${TEST_SRC_NAME}
    pisa
    Catch2
    rapidcheck
    )
  catch_discover_tests(${TEST_SRC_NAME} TEST_PREFIX "${TEST_SRC_NAME}:")
  if (ENABLE_COVERAGE)
    # enable code coverage
    add_coverage(${TEST_SRC_NAME})
  endif()
endforeach(TEST_SRC)

target_link_libraries(test_intersection testlib)
target_link_libraries(test_cursors testlib)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test_data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

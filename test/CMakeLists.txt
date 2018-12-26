set(CMAKE_RUNTIME_OUTPUT_DIRECTORY  ${CMAKE_BINARY_DIR}/test)

file(GLOB TEST_SOURCES test_*.cpp)
foreach(TEST_SRC ${TEST_SOURCES})
  get_filename_component (TEST_SRC_NAME ${TEST_SRC} NAME_WE)
  add_executable(${TEST_SRC_NAME} ${TEST_SRC})
  target_link_libraries(${TEST_SRC_NAME}
    ${Boost_LIBRARIES}
    QMX
    )
  add_test(${TEST_SRC_NAME} ${TEST_SRC_NAME})

  # enable code coverage
  add_coverage(${TEST_SRC_NAME})
endforeach(TEST_SRC)

target_link_libraries(test_block_codecs
    FastPFor
    streamvbyte
    MaskedVByte
    QMX
    simdcomp
)

target_link_libraries(test_block_posting_list
    FastPFor
    streamvbyte
    MaskedVByte
    QMX
    simdcomp
)

target_link_libraries(test_block_freq_index
    FastPFor
    streamvbyte
    MaskedVByte
    QMX
    simdcomp
)

target_link_libraries(test_sample_index
    FastPFor
    streamvbyte
    MaskedVByte
    QMX
    simdcomp
)

target_link_libraries(test_forward_index
    FastPFor
    streamvbyte
    MaskedVByte
    QMX
    simdcomp
    ParallelSTL
)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test_data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

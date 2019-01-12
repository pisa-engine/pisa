set(CMAKE_RUNTIME_OUTPUT_DIRECTORY  ${CMAKE_BINARY_DIR}/test)

file(GLOB TEST_SOURCES test_*.cpp)
foreach(TEST_SRC ${TEST_SOURCES})
  get_filename_component (TEST_SRC_NAME ${TEST_SRC} NAME_WE)
  add_executable(${TEST_SRC_NAME} ${TEST_SRC})
  target_link_libraries(${TEST_SRC_NAME}
    Boost::boost
    QMX
    mio
    ParallelSTL
    GSL
    Catch2
    )
  add_test(${TEST_SRC_NAME} ${TEST_SRC_NAME})

  # enable code coverage
  add_coverage(${TEST_SRC_NAME})
endforeach(TEST_SRC)

target_link_libraries(test_block_codecs
    FastPFor
    streamvbyte
    MaskedVByte
    simdcomp
)

target_link_libraries(test_block_posting_list
    FastPFor
    streamvbyte
    MaskedVByte
    simdcomp
)

target_link_libraries(test_block_freq_index
    FastPFor
    streamvbyte
    MaskedVByte
    simdcomp
)

target_link_libraries(test_sample_index
    FastPFor
    streamvbyte
    MaskedVByte
    simdcomp
)

target_link_libraries(test_forward_index
    FastPFor
    streamvbyte
    MaskedVByte
    simdcomp
    ParallelSTL
)

target_compile_features(test_forward_index_builder PRIVATE cxx_std_17)
target_link_libraries(test_forward_index_builder
    ParallelSTL
    gumbo::gumbo
    Boost::filesystem
    Porter2
)

target_compile_features(test_invert PRIVATE cxx_std_17)
target_link_libraries(test_invert
    ParallelSTL
    Boost::filesystem
)

target_compile_features(test_html PRIVATE cxx_std_17)
target_link_libraries(test_html
    gumbo::gumbo
    Boost::filesystem
    Porter2
)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test_data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

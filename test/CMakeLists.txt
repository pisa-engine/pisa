set(CMAKE_RUNTIME_OUTPUT_DIRECTORY  ${CMAKE_BINARY_DIR}/test)

file(GLOB TEST_SOURCES test_*.cpp)
foreach(TEST_SRC ${TEST_SOURCES})
  get_filename_component (TEST_SRC_NAME ${TEST_SRC} NAME_WE)
  add_executable(${TEST_SRC_NAME} ${TEST_SRC})
  target_link_libraries(${TEST_SRC_NAME}
    pisa
    Catch2
    )
  add_test(${TEST_SRC_NAME} ${TEST_SRC_NAME})

  # enable code coverage
  add_coverage(${TEST_SRC_NAME})
endforeach(TEST_SRC)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test_data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

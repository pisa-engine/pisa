set(CMAKE_RUNTIME_OUTPUT_DIRECTORY  ${CMAKE_BINARY_DIR}/test)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../external/Catch2/contrib)	
include(CTest)
include(Catch)

add_library(testlib in_memory_index.cpp)
target_link_libraries(testlib pisa)

file(GLOB TEST_SOURCES test_*.cpp)
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

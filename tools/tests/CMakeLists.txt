set(CMAKE_RUNTIME_OUTPUT_DIRECTORY  ${CMAKE_BINARY_DIR}/test)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../../external/Catch2/contrib)
include(CTest)
include(Catch)

file(GLOB TEST_SOURCES test_*.cpp)
foreach(TEST_SRC ${TEST_SOURCES})
  get_filename_component (TEST_SRC_NAME ${TEST_SRC} NAME_WE)
  add_executable(${TEST_SRC_NAME} ${TEST_SRC})
  target_link_libraries(${TEST_SRC_NAME}
    pisa
    app
    Catch2
    CLI11
    #rapidcheck
    )
  catch_discover_tests(${TEST_SRC_NAME} TEST_PREFIX "${TEST_SRC_NAME}:")
  if (ENABLE_COVERAGE)
    # enable code coverage
    add_coverage(${TEST_SRC_NAME})
  endif()
endforeach(TEST_SRC)

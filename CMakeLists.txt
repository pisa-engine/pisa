cmake_minimum_required(VERSION 2.8)
project(DS2I CXX C)

configure_file(
  ${DS2I_SOURCE_DIR}/include/ds2i_config.hpp.in
  ${DS2I_SOURCE_DIR}/include/ds2i_config.hpp
  ESCAPE_QUOTES)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
endif()
MESSAGE( STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE} )

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_subdirectory(external)

# Add code coverage
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/external/CMake-codecov/cmake")
find_package(codecov)
list(APPEND LCOV_REMOVE_PATTERNS "'${PROJECT_SOURCE_DIR}/external/*'")


if (UNIX)
   # C++14
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

   # For hardware popcount and other special instructions
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

   # Extensive warnings
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-missing-braces")
   # Silence a warning bug in Boost
   if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
     set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-local-typedefs")
   endif ()
   # set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wconversion")

   if (USE_SANITIZERS)
     set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
   endif ()

   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb") # Add debug info anyway

endif()

find_package(OpenMP)
if(OPENMP_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

# set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.42.0 COMPONENTS iostreams unit_test_framework filesystem system thread REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})

# add the root directory to include path to make includes absolute
include_directories(${DS2I_SOURCE_DIR}/external
		    ${STXXL_INCLUDE_DIRS}
		    ${DS2I_SOURCE_DIR}/include
		    )

add_subdirectory(src)

enable_testing()
add_subdirectory(test)
add_subdirectory(benchmarks)

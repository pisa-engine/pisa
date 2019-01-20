cmake_minimum_required(VERSION 3.0)
project(DS2I CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

configure_file(
  ${DS2I_SOURCE_DIR}/include/pisa/ds2i_config.hpp.in
  ${DS2I_SOURCE_DIR}/include/pisa/ds2i_config.hpp
  ESCAPE_QUOTES)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
endif()
MESSAGE( STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE} )

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# include(external/external.cmake)
add_subdirectory(external)

include(ExternalProject)
ExternalProject_Add(gumbo-external
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/external/gumbo-parser
    BINARY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/external/gumbo-parser
    CONFIGURE_COMMAND ./autogen.sh && ./configure --prefix=${CMAKE_BINARY_DIR}/gumbo-parser
    BUILD_COMMAND ${MAKE})
add_library(gumbo::gumbo UNKNOWN IMPORTED)
set_target_properties(gumbo::gumbo PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
    ${CMAKE_CURRENT_SOURCE_DIR}/external/gumbo-parser/src)
set_property(TARGET gumbo::gumbo APPEND PROPERTY IMPORTED_LOCATION
    ${CMAKE_BINARY_DIR}/gumbo-parser/lib/libgumbo.a)
add_dependencies( gumbo::gumbo gumbo-external )

# Add code coverage
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/external/CMake-codecov/cmake")
find_package(codecov)
list(APPEND LCOV_REMOVE_PATTERNS "'${PROJECT_SOURCE_DIR}/external/*'")


if (UNIX)

   # For hardware popcount and other special instructions
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

   # Extensive warnings
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-missing-braces")

   if (USE_SANITIZERS)
     set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
   endif ()

   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb") # Add debug info anyway

endif()

find_package(OpenMP)
if(OPENMP_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
link_libraries(Threads::Threads)


include_directories(include)
add_library(pisa INTERFACE)
target_include_directories(pisa INTERFACE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/pisa>
)
target_link_libraries(pisa INTERFACE
    Threads::Threads
    Boost::boost
    QMX
    mio
    ParallelSTL
    GSL
    FastPFor
    streamvbyte
    MaskedVByte
    simdcomp
    gumbo::gumbo
    Boost::filesystem
    Porter2
    warcpp
    spdlog
)
target_include_directories(pisa INTERFACE external)

add_subdirectory(src)

enable_testing()
add_subdirectory(test)

add_subdirectory(benchmarks)


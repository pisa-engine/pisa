cmake_minimum_required(VERSION 3.0)

project(DS2I CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(PISA_ENABLE_TESTING "Enable testing of the library." ON)
option(PISA_ENABLE_BENCHMARKING "Enable benchmarking of the library." ON)

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
add_library(gumbo::gumbo STATIC IMPORTED)
set_property(TARGET gumbo::gumbo APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES
    ${CMAKE_CURRENT_SOURCE_DIR}/external/gumbo-parser/src)
set_property(TARGET gumbo::gumbo APPEND PROPERTY INCLUDE_DIRECTORIES
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
    mio_base
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
    fmt::fmt
    range-v3
    te
)
target_include_directories(pisa INTERFACE external)

add_subdirectory(src)

if (PISA_ENABLE_TESTING AND BUILD_TESTING)
    enable_testing()
    add_subdirectory(test)
endif()

if (PISA_ENABLE_BENCHMARKING)
  add_subdirectory(benchmarks)
endif()


if(PISA_COMPILE_HEADERS)
  get_property(PISA_INTERFACE_INCLUDE_DIRECTORIES TARGET pisa PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
  list(APPEND PISA_INCLUDE_DIRECTORIES ${PISA_INTERFACE_INCLUDE_DIRECTORIES})
  get_property(PISA_INTERFACE_LINK_LIBRARIES TARGET pisa PROPERTY INTERFACE_LINK_LIBRARIES)
  foreach(PISA_INTERFACE_LINK_LIBRARY ${PISA_INTERFACE_LINK_LIBRARIES})
    get_property(PISA_INTERFACE_LINK_LIBRARY_TYPE TARGET ${PISA_INTERFACE_LINK_LIBRARY} PROPERTY TYPE)
    if(PISA_INTERFACE_LINK_LIBRARY_TYPE STREQUAL "INTERFACE_LIBRARY")
      get_property(PISA_INTERFACE_INCLUDE_DIRECTORIES TARGET ${PISA_INTERFACE_LINK_LIBRARY} PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
      list(APPEND PISA_INCLUDE_DIRECTORIES ${PISA_INTERFACE_INCLUDE_DIRECTORIES})
    else ()
      get_property(PISA_INTERFACE_INCLUDE_DIRECTORIES TARGET ${PISA_INTERFACE_LINK_LIBRARY} PROPERTY INCLUDE_DIRECTORIES)
      list(APPEND PISA_INCLUDE_DIRECTORIES ${PISA_INTERFACE_INCLUDE_DIRECTORIES})
    endif()
  endforeach()

  include(CheckCXXSourceCompiles)
  set(CMAKE_REQUIRED_FLAGS "${CMAKE_CXX_FLAGS} -std=c++${CMAKE_CXX_STANDARD} -c")
  get_property(PISA_INTERFACE_INCLUDE_DIRECTORIES TARGET pisa PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
  set(CMAKE_REQUIRED_INCLUDES "${CMAKE_SOURCE_DIR}/external/tbb/include" ${PISA_INCLUDE_DIRECTORIES})

  file(GLOB_RECURSE PISA_HEADER_FILES FOLLOW_SYMLINKS "include/pisa/*.hpp")
  list(SORT PISA_HEADER_FILES)

  foreach(file ${PISA_HEADER_FILES})
    # replace / to _ to fix warnings
    string(REPLACE "/" "_" compilename "${file}")
    string(REPLACE "." "_" compilename "${compilename}")

    if(NOT IsSelfContained${compilename})
      unset(IsSelfContained${compilename} CACHE)
    endif()

    check_cxx_source_compiles(
      "#include \"${file}\"
      int main() { return 0; }" IsSelfContained${compilename})

    if(NOT IsSelfContained${compilename})
      message(FATAL_ERROR
        "Compilation FAILED for ${file}\nCompiler output:\n${OUTPUT}")
    endif()
  endforeach()

endif(PISA_COMPILE_HEADERS)

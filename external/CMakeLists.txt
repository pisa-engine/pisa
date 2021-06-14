EXECUTE_PROCESS(COMMAND git submodule update --init
                WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/..
                OUTPUT_QUIET
        )

# Add FastPFor
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/FastPFor EXCLUDE_FROM_ALL)
target_compile_options(FastPFor PRIVATE -Wno-cast-align)
# target_include_directories(FastPFor PUBLIC
#     $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/FastPFor/include>
#     $<INSTALL_INTERFACE:include>
# )

# Add CLI11
set(CLI11_TESTING OFF CACHE BOOL "skip trecpp testing")
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/CLI11 EXCLUDE_FROM_ALL)

# Add streamvbyte
add_library(streamvbyte STATIC ${CMAKE_CURRENT_SOURCE_DIR}/streamvbyte/src/streamvbyte.c
                               ${CMAKE_CURRENT_SOURCE_DIR}/streamvbyte/src/streamvbytedelta.c
)
target_include_directories(streamvbyte PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/streamvbyte/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_options(streamvbyte PRIVATE -Wno-cast-align
                                           -Wno-incompatible-pointer-types
                                           -Wno-implicit-function-declaration
)

# Add maskedvbyte
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
add_library(MaskedVByte STATIC ${CMAKE_CURRENT_SOURCE_DIR}/MaskedVByte/src/varintdecode.c
                               ${CMAKE_CURRENT_SOURCE_DIR}/MaskedVByte/src/varintencode.c
)
target_include_directories(MaskedVByte PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/MaskedVByte/include>
    $<INSTALL_INTERFACE:include>
)

# Add QMX
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/QMX EXCLUDE_FROM_ALL)
target_compile_options(QMX INTERFACE -Wno-implicit-fallthrough)

# Add mio
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/mio EXCLUDE_FROM_ALL)
target_compile_options(mio INTERFACE -Wno-sign-compare)


# Add SIMD-BP
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
add_library(simdcomp STATIC ${CMAKE_CURRENT_SOURCE_DIR}/simdcomp/src/simdbitpacking.c
                            ${CMAKE_CURRENT_SOURCE_DIR}/simdcomp/src/simdcomputil.c
)
target_include_directories(simdcomp PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/simdcomp/include>
    $<INSTALL_INTERFACE:include>
)


# Add TBB
set(TBB_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/tbb/)
include(${TBB_ROOT}/cmake/TBBBuild.cmake)
tbb_build(
    TBB_ROOT ${TBB_ROOT}
    CONFIG_DIR TBB_DIR)
# find_package(TBB REQUIRED tbb)

# Add ParallelSTL
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/parallelstl EXCLUDE_FROM_ALL)
target_compile_options(ParallelSTL INTERFACE -Wno-unused-parameter
                                           -Wno-unknown-pragmas
                                           -Wno-sign-compare
                                           -Wno-reorder
                                           -Wno-unused-local-typedefs
                                           -Wno-unused-variable
)

# Add GSL
# add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/GSL EXCLUDE_FROM_ALL)
add_library(GSL INTERFACE)
target_include_directories(GSL SYSTEM INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/GSL/include>
    $<INSTALL_INTERFACE:include>
)
install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/GSL/include/gsl
    DESTINATION include
)

# Add Boost
add_subdirectory(boost-cmake)
set(BOOST_INCLUDEDIR ${CMAKE_CURRENT_SOURCE_DIR}/boost-cmake CACHE PATH "Change boost lookup path")
set(BOOST_LIBRARYDIR ${CMAKE_BINARY_DIR}/boost-cmake CACHE PATH "Change boost lookup path")

# Add Catch
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/Catch2)

# Add Porter2
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/Porter2)

# Add KrovetzStemmer
set(KrovetzStemmer_ENABLE_TESTING OFF CACHE BOOL "skip trecpp testing")
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/KrovetzStemmer)

# Add warcpp
set(WARCPP_ENABLE_TESTING OFF CACHE BOOL "skip warcpp testing")
set(WARCPP_BUILD_TOOL OFF CACHE BOOL "skip warcpp testing")
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/warcpp)

# Add trecpp
set(TRECPP_ENABLE_TESTING OFF CACHE BOOL "skip trecpp testing")
set(TRECPP_BUILD_TOOL OFF CACHE BOOL "skip trecpp testing")
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/trecpp)

# Add trecpp
set(WAPOPP_ENABLE_TESTING OFF CACHE BOOL "skip wapopp testing")
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/wapopp)
target_compile_options(wapopp PRIVATE -Wno-error=all -Wno-missing-braces)

# Add fmt
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/fmt)

# Add spdlog
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/spdlog)

# Add range-v3
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/range-v3)

if (PISA_ENABLE_TESTING AND BUILD_TESTING)
    # Add RapidCheck
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/rapidcheck)
    target_compile_options(rapidcheck PRIVATE -Wno-error)
endif()

# Add taily
set(TAILY_ENABLE_TESTING OFF CACHE BOOL "skip taily testing")
set(TAILY_BUILD_EXAMPLE OFF CACHE BOOL "skip taily example")
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/taily)

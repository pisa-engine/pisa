EXECUTE_PROCESS(COMMAND git submodule update --init
                WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/..
                OUTPUT_QUIET
        )

# Add FastPFor
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/FastPFor EXCLUDE_FROM_ALL)

# Add CLI11
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/CLI11 EXCLUDE_FROM_ALL)

# stxxl
add_definitions(-DSTXXL_VERBOSE_LEVEL=-10) # suppress messages to stdout
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/stxxl)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
include_directories(${STXXL_INCLUDE_DIRS})

# Add streamvbyte
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external/streamvbyte/include)
add_library(streamvbyte STATIC ${CMAKE_CURRENT_SOURCE_DIR}/external/streamvbyte/src/streamvbyte.c
                               ${CMAKE_CURRENT_SOURCE_DIR}/external/streamvbyte/src/streamvbytedelta.c
)

# Add maskedvbyte
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external/MaskedVByte/include)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
add_library(MaskedVByte STATIC ${CMAKE_CURRENT_SOURCE_DIR}/external/MaskedVByte/src/varintdecode.c
                               ${CMAKE_CURRENT_SOURCE_DIR}/external/MaskedVByte/src/varintencode.c
)

# Add QMX
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/QMX EXCLUDE_FROM_ALL)

# Add SIMD-BP
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external/simdcomp/include)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
add_library(simdcomp STATIC ${CMAKE_CURRENT_SOURCE_DIR}/external/simdcomp/src/simdbitpacking.c
                            ${CMAKE_CURRENT_SOURCE_DIR}/external/simdcomp/src/simdcomputil.c
)


# Add TBB
set(TBB_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/external/tbb/)
include(${TBB_ROOT}/cmake/TBBBuild.cmake)
tbb_build(
    TBB_ROOT ${TBB_ROOT}
    CONFIG_DIR TBB_DIR)
# find_package(TBB REQUIRED tbb)

# Add ParallelSTL
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/parallelstl EXCLUDE_FROM_ALL)


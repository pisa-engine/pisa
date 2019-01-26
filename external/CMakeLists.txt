EXECUTE_PROCESS(COMMAND git submodule update --init
                WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/..
                OUTPUT_QUIET
        )

# Add FastPFor
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/FastPFor EXCLUDE_FROM_ALL)
target_compile_options(FastPFor PRIVATE -Wno-cast-align)

# Add CLI11
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/CLI11 EXCLUDE_FROM_ALL)

# stxxl
add_definitions(-DSTXXL_VERBOSE_LEVEL=-10) # suppress messages to stdout
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/stxxl)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
include_directories(${STXXL_INCLUDE_DIRS})
set_target_properties(stxxl PROPERTIES
            CXX_STANDARD 14
)
set_target_properties(${LIBSTXXL_SOURCES} PROPERTIES
            CXX_STANDARD 14
)
set_target_properties(stxxl_tool test1 test2 PROPERTIES
            CXX_STANDARD 14
)



# Add streamvbyte
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/streamvbyte/include)
add_library(streamvbyte STATIC ${CMAKE_CURRENT_SOURCE_DIR}/streamvbyte/src/streamvbyte.c
                               ${CMAKE_CURRENT_SOURCE_DIR}/streamvbyte/src/streamvbytedelta.c
)
target_compile_options(streamvbyte PRIVATE -Wno-cast-align
                                           -Wno-incompatible-pointer-types
                                           -Wno-implicit-function-declaration
)

# Add maskedvbyte
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/MaskedVByte/include)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
add_library(MaskedVByte STATIC ${CMAKE_CURRENT_SOURCE_DIR}/MaskedVByte/src/varintdecode.c
                               ${CMAKE_CURRENT_SOURCE_DIR}/MaskedVByte/src/varintencode.c
)

# Add QMX
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/QMX EXCLUDE_FROM_ALL)
target_compile_options(QMX INTERFACE -Wno-implicit-fallthrough)

# Add mio
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/mio EXCLUDE_FROM_ALL)
target_compile_options(mio INTERFACE -Wno-sign-compare)


# Add SIMD-BP
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/simdcomp/include)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
add_library(simdcomp STATIC ${CMAKE_CURRENT_SOURCE_DIR}/simdcomp/src/simdbitpacking.c
                            ${CMAKE_CURRENT_SOURCE_DIR}/simdcomp/src/simdcomputil.c
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
)

# Add GSL
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/GSL EXCLUDE_FROM_ALL)

# Add Boost
add_subdirectory(boost-cmake)

# Add Catch
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/Catch2)

# Add Porter2
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/Porter2)

# Add warcpp
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/warcpp)

# Add fmt
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/fmt)

# Add spdlog
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/spdlog)

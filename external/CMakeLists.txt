EXECUTE_PROCESS(COMMAND git submodule update --init
                WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/..
                OUTPUT_QUIET
        )

# Add FastPFor
add_subdirectory(FastPFor EXCLUDE_FROM_ALL)
# Add cxxopts
set(CXXOPTS_BUILD_EXAMPLES OFF CACHE BOOL "Enable cxxopts examples")
add_subdirectory(cxxopts EXCLUDE_FROM_ALL)

# stxxl
add_definitions(-DSTXXL_VERBOSE_LEVEL=-10) # suppress messages to stdout
add_subdirectory(stxxl)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
include_directories(${STXXL_INCLUDE_DIRS})

# Add streamvbyte
include_directories(streamvbyte/include)
add_library(streamvbyte STATIC streamvbyte/src/streamvbyte.c
                               streamvbyte/src/streamvbytedelta.c
)

# Add maskedvbyte
include_directories(MaskedVByte/include)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
add_library(MaskedVByte STATIC MaskedVByte/src/varintdecode.c
                               MaskedVByte/src/varintencode.c
)
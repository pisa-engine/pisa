add_subdirectory(succinct EXCLUDE_FROM_ALL)
add_subdirectory(FastPFor EXCLUDE_FROM_ALL)

# stxxl
add_definitions(-DSTXXL_VERBOSE_LEVEL=-10) # suppress messages to stdout
add_subdirectory(stxxl)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
include_directories(${STXXL_INCLUDE_DIRS})

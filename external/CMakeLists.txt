EXECUTE_PROCESS(COMMAND git submodule update --init
                WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/..
                OUTPUT_QUIET
        )

# Add FastPFor
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/FastPFor EXCLUDE_FROM_ALL)
target_compile_options(FastPFor PRIVATE -Wno-cast-align)

# Add CLI11
set(CLI11_TESTING OFF CACHE BOOL "skip trecpp testing")
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/CLI11 EXCLUDE_FROM_ALL)

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
target_compile_options(QMX INTERFACE -Wno-unused-parameter -Wno-implicit-fallthrough)

# Add mio
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/mio EXCLUDE_FROM_ALL)
target_compile_options(mio INTERFACE -Wno-sign-compare)


# Add SIMD-BP
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/simdcomp/include)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
add_library(simdcomp STATIC ${CMAKE_CURRENT_SOURCE_DIR}/simdcomp/src/simdbitpacking.c
                            ${CMAKE_CURRENT_SOURCE_DIR}/simdcomp/src/simdcomputil.c
)

# Add GSL
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/GSL EXCLUDE_FROM_ALL)

# Add Boost
add_subdirectory(boost-cmake)
set(BOOST_INCLUDEDIR ${CMAKE_CURRENT_SOURCE_DIR}/boost-cmake CACHE PATH "Change boost lookup path")
set(BOOST_LIBRARYDIR ${CMAKE_BINARY_DIR}/boost-cmake CACHE PATH "Change boost lookup path")

# Add Catch
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/Catch2)

# Add Porter2
set(Porter2_ENABLE_TESTING OFF CACHE BOOL "skip Porter2 testing")
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/Porter2)

# Add KrovetzStemmer
set(KrovetzStemmer_ENABLE_TESTING OFF CACHE BOOL "skip KrovetzStemmer testing")
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

# Add RapidCheck
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/rapidcheck)
target_compile_options(rapidcheck PRIVATE -Wno-error)

# Add taily
set(TAILY_ENABLE_TESTING OFF CACHE BOOL "skip taily testing")
set(TAILY_BUILD_EXAMPLE OFF CACHE BOOL "skip taily example")
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/taily)

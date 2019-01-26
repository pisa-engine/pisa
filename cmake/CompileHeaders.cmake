if(PISA_COMPILE_HEADERS)
  get_property(dirs TARGET pisa PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
  list(APPEND inc_dirs ${dirs})
  get_property(libs TARGET pisa PROPERTY INTERFACE_LINK_LIBRARIES)
  foreach(lib ${libs})
    get_property(type TARGET ${lib} PROPERTY TYPE)
    if(type STREQUAL "INTERFACE_LIBRARY")
      get_property(dirs TARGET ${lib} PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
      list(APPEND inc_dirs ${dirs})
    else ()
      get_property(dirs TARGET ${lib} PROPERTY INCLUDE_DIRECTORIES)
      list(APPEND inc_dirs ${dirs})
    endif()
  endforeach()

  include(CheckCXXSourceCompiles)
  set(CMAKE_REQUIRED_FLAGS "${CMAKE_CXX_FLAGS} -std=c++${CMAKE_CXX_STANDARD} -c")
  get_property(dirs TARGET pisa PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
  set(CMAKE_REQUIRED_INCLUDES "${CMAKE_SOURCE_DIR}/external/tbb/include" ${inc_dirs})

  file(GLOB_RECURSE header_files FOLLOW_SYMLINKS "include/pisa/*.hpp")
  list(SORT header_files)

  foreach(file ${header_files})
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

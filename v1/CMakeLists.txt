add_executable(compress compress.cpp)
target_link_libraries(compress pisa CLI11)

add_executable(query query.cpp)
target_link_libraries(query pisa CLI11)

add_executable(postings postings.cpp)
target_link_libraries(postings pisa CLI11)

add_executable(compress compress.cpp)
target_link_libraries(compress pisa CLI11)

add_executable(query query.cpp)
target_link_libraries(query pisa CLI11)

add_executable(union-lookup union_lookup.cpp)
target_link_libraries(union-lookup pisa CLI11)

add_executable(postings postings.cpp)
target_link_libraries(postings pisa CLI11)

add_executable(score score.cpp)
target_link_libraries(score pisa CLI11)

add_executable(bigram-index bigram_index.cpp)
target_link_libraries(bigram-index pisa CLI11)

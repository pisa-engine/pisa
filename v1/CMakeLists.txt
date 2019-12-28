add_executable(compress compress.cpp)
target_link_libraries(compress pisa CLI11)

add_executable(query query.cpp)
target_link_libraries(query pisa CLI11)

add_executable(postings postings.cpp)
target_link_libraries(postings pisa CLI11)

add_executable(score score.cpp)
target_link_libraries(score pisa CLI11)

add_executable(bmscore bmscore.cpp)
target_link_libraries(bmscore pisa CLI11)

add_executable(bigram-index bigram_index.cpp)
target_link_libraries(bigram-index pisa CLI11)

add_executable(filter-queries filter_queries.cpp)
target_link_libraries(filter-queries pisa CLI11)

add_executable(threshold threshold.cpp)
target_link_libraries(threshold pisa CLI11)

add_executable(intersection intersection.cpp)
target_link_libraries(intersection pisa CLI11)

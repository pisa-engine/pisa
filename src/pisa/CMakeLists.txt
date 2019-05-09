add_library(
    pisa_tokenizer
    tokenizer.cpp
)
target_include_directories(pisa_tokenizer PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/pisa>)

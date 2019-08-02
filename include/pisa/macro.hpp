#define PISA_DAAT_ALGORITHM(ALGORITHM, SCORER, CODEC, WAND)                                  \
    template uint64_t ALGORITHM::                                                            \
    operator()<scored_cursor<block_freq_index<CODEC>,                                        \
                             typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>>( \
        std::vector<scored_cursor<                                                           \
                block_freq_index<CODEC>,                                                     \
                typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>> && cursors,   \
        uint64_t max_docid);

#define PISA_DAAT_ALGORITHM_EXTERN(ALGORITHM, SCORER, CODEC, WAND) \
    extern PISA_DAAT_ALGORITHM(ALGORITHM, SCORER, CODEC, WAND)

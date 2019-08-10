//
// ranked_and, ranked_or
//
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

//
// wand, maxscore
//
#define PISA_DAAT_MAX_ALGORITHM(ALGORITHM, SCORER, CODEC, WAND)                                  \
    template uint64_t ALGORITHM::                                                                \
    operator()<max_scored_cursor<block_freq_index<CODEC>,                                        \
                                 typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>>( \
        std::vector<max_scored_cursor<                                                           \
                block_freq_index<CODEC>,                                                         \
                typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>> && cursors,       \
        uint64_t max_docid);
#define PISA_DAAT_MAX_ALGORITHM_EXTERN(ALGORITHM, SCORER, CODEC, WAND) \
    extern PISA_DAAT_MAX_ALGORITHM(ALGORITHM, SCORER, CODEC, WAND)

//
// bmw, bmm
//
#define PISA_DAAT_BLOCK_MAX_ALGORITHM(ALGORITHM, SCORER, CODEC, WAND)                           \
    template uint64_t ALGORITHM::operator()<                                                    \
        block_max_scored_cursor<block_freq_index<CODEC>,                                        \
                                wand_data<WAND>,                                                \
                                typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>>( \
        std::vector<block_max_scored_cursor<                                                    \
                block_freq_index<CODEC>,                                                        \
                wand_data<WAND>,                                                                \
                typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>> && cursors,      \
        uint64_t max_docid);
#define PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(ALGORITHM, SCORER, CODEC, WAND) \
    extern PISA_DAAT_BLOCK_MAX_ALGORITHM(ALGORITHM, SCORER, CODEC, WAND)

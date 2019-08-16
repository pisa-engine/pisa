//
// ranked_and, ranked_or
//
#define PISA_DAAT_ALGORITHM(ALGORITHM, SCORER, INDEX, WAND)                                    \
    template uint64_t ALGORITHM::                                                              \
    operator()<scored_cursor<BOOST_PP_CAT(INDEX, _index),                                      \
                             typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>>(   \
        gsl::span<scored_cursor<BOOST_PP_CAT(INDEX, _index),                                   \
                                typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>> \
            cursors,                                                                           \
        uint64_t max_docid);
#define PISA_DAAT_ALGORITHM_EXTERN(ALGORITHM, SCORER, INDEX, WAND) \
    extern PISA_DAAT_ALGORITHM(ALGORITHM, SCORER, INDEX, WAND)

//
// wand, maxscore
//
#define PISA_DAAT_MAX_ALGORITHM(ALGORITHM, SCORER, INDEX, WAND)                                    \
    template uint64_t ALGORITHM::                                                                  \
    operator()<max_scored_cursor<BOOST_PP_CAT(INDEX, _index),                                      \
                                 typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>>(   \
        gsl::span<max_scored_cursor<BOOST_PP_CAT(INDEX, _index),                                   \
                                    typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>> \
            cursors,                                                                               \
        uint64_t max_docid);
#define PISA_DAAT_MAX_ALGORITHM_EXTERN(ALGORITHM, SCORER, INDEX, WAND) \
    extern PISA_DAAT_MAX_ALGORITHM(ALGORITHM, SCORER, INDEX, WAND)

//
// bmw, bmm
//
#define PISA_DAAT_BLOCK_MAX_ALGORITHM(ALGORITHM, SCORER, INDEX, WAND)                           \
    template uint64_t ALGORITHM::operator()<                                                    \
        block_max_scored_cursor<BOOST_PP_CAT(INDEX, _index),                                    \
                                wand_data<WAND>,                                                \
                                typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>>( \
        gsl::span<block_max_scored_cursor<                                                      \
            BOOST_PP_CAT(INDEX, _index),                                                        \
            wand_data<WAND>,                                                                    \
            typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>> cursors,             \
        uint64_t max_docid);
#define PISA_DAAT_BLOCK_MAX_ALGORITHM_EXTERN(ALGORITHM, SCORER, INDEX, WAND) \
    extern PISA_DAAT_BLOCK_MAX_ALGORITHM(ALGORITHM, SCORER, INDEX, WAND)

//
// TAAT
//
#define PISA_TAAT_ALGORITHM(ALGORITHM, SCORER, INDEX, WAND, ACC)                               \
    template uint64_t ALGORITHM::                                                              \
    operator()<scored_cursor<BOOST_PP_CAT(INDEX, _index),                                      \
                             typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>,    \
               ACC>(                                                                           \
        gsl::span<scored_cursor<BOOST_PP_CAT(INDEX, _index),                                   \
                                typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>> \
            cursors,                                                                           \
        uint64_t max_docid,                                                                    \
        ACC &&);
#define PISA_TAAT_ALGORITHM_EXTERN(ALGORITHM, SCORER, INDEX, WAND, ACC) \
    extern PISA_TAAT_ALGORITHM(ALGORITHM, SCORER, INDEX, WAND, ACC)

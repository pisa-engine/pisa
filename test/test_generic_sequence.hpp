#pragma once

#include "succinct/test_common.hpp"
#include "succinct/bit_vector.hpp"
#include "util.hpp"

std::vector<uint64_t> random_sequence(size_t universe, size_t n,
                                      bool strict = true)
{
    srand(42);
    std::vector<uint64_t> seq;

    uint64_t u = strict ? (universe - n) : universe;
    for (size_t i = 0; i < n; ++i) {
        seq.push_back(rand() % u);
    }
    std::sort(seq.begin(), seq.end());

    if (strict) {
        for (size_t i = 0; i < n; ++i) {
            seq[i] += i;
        }
    }

    return seq;
}

template <typename SequenceReader>
void test_move_next(SequenceReader r, std::vector<uint64_t> const& seq)
{
    BOOST_REQUIRE_EQUAL(seq.size(), r.size());
    if (seq.empty()) {
        // just check that move works
        BOOST_REQUIRE_EQUAL(seq.size(), r.move(seq.size()).first);
        return;
    }

    typename SequenceReader::value_type val;

    // test random access and enumeration
    for (uint64_t i = 0; i < seq.size(); ++i) {
        val = r.move(i);
        MY_REQUIRE_EQUAL(i, val.first,
                         "i = " << i);
        MY_REQUIRE_EQUAL(seq[i], val.second,
                         "i = " << i);

        if (i) {
            MY_REQUIRE_EQUAL(seq[i - 1], r.prev_value(),
                             "i = " << i);
        } else {
            MY_REQUIRE_EQUAL(0, r.prev_value(),
                             "i = " << i);
        }
    }
    r.move(seq.size());
    BOOST_REQUIRE_EQUAL(seq.back(), r.prev_value());

    val = r.move(0);
    for (uint64_t i = 0; i < seq.size(); ++i) {
        MY_REQUIRE_EQUAL(seq[i], val.second,
                         "i = " << i);

        if (i) {
            MY_REQUIRE_EQUAL(seq[i - 1], r.prev_value(),
                             "i = " << i);
        } else {
            MY_REQUIRE_EQUAL(0, r.prev_value(),
                             "i = " << i);
        }
        val = r.next();
    }
    BOOST_REQUIRE_EQUAL(r.size(), val.first);
    BOOST_REQUIRE_EQUAL(seq.back(), r.prev_value());

    // test small skips
    for (size_t i = 0; i < seq.size(); ++i) {
        for (size_t skip = 1; skip < seq.size() - i; skip <<= 1) {
            auto rr = r;
            rr.move(i);
            auto val = rr.move(i + skip);
            MY_REQUIRE_EQUAL(i + skip, val.first,
                             "i = " << i << " skip = " << skip);
            MY_REQUIRE_EQUAL(seq[i + skip], val.second,
                             "i = " << i << " skip = " << skip);
        }
    }
}

template <typename SequenceReader>
void test_next_geq(SequenceReader r, std::vector<uint64_t> const& seq)
{
    BOOST_REQUIRE_EQUAL(seq.size(), r.size());
    if (seq.empty()) {
        // just check that next_geq works
        BOOST_REQUIRE_EQUAL(seq.size(), r.next_geq(1).first);
        return;
    }

    typename SequenceReader::value_type val;

    // test successor
    uint64_t last = 0;
    for (size_t i = 0; i < seq.size(); ++i) {
        if (seq[i] == last) continue;

        auto rr = r;
        for (size_t t = 0; t < 10; ++t) {
            uint64_t p = 0;
            switch (i) {
            case 0:
                p = last + 1; break;
            case 1:
                p = seq[i]; break;
            default:
                p = last + 1 + (rand() % (seq[i] - last));
            }

            val = rr.next_geq(p);
            BOOST_REQUIRE_EQUAL(i, val.first);
            MY_REQUIRE_EQUAL(seq[i], val.second,
                             "p = " << p);

            if (val.first) {
                MY_REQUIRE_EQUAL(seq[val.first - 1], rr.prev_value(),
                                 "i = " << i);
            } else {
                MY_REQUIRE_EQUAL(0, rr.prev_value(),
                                 "i = " << i);
            }
        }
        last = seq[i];
    }

    val = r.next_geq(seq.back() + 1);
    BOOST_REQUIRE_EQUAL(r.size(), val.first);
    BOOST_REQUIRE_EQUAL(seq.back(), r.prev_value());

    // check next_geq beyond universe
    val = r.next_geq(2 * seq.back() + 1);
    BOOST_REQUIRE_EQUAL(r.size(), val.first);

    // test small skips
    for (size_t i = 0; i < seq.size(); ++i) {
        for (size_t skip = 1; skip < seq.size() - i; skip <<= 1) {
            size_t exp_pos = i + skip;
            // for weakly monotone sequences, next_at returns the first of the
            // run of equal values
            while ((exp_pos > 0) && seq[exp_pos - 1] == seq[i + skip]) {
                exp_pos -= 1;
            }

            auto rr = r;
            rr.move(i);
            val = rr.next_geq(seq[i + skip]);
            MY_REQUIRE_EQUAL(exp_pos, val.first,
                             "i = " << i << " skip = " << skip
                             << " value expected = " << seq[i + skip]
                             << " got = " << val.second);
            MY_REQUIRE_EQUAL(seq[i + skip], val.second,
                             "i = " << i << " skip = " << skip);
        }
    }
}

// oh, C++
struct no_next_geq_tag {};
struct next_geq_tag : no_next_geq_tag {};

template <typename SequenceReader>
void test_sequence(SequenceReader r, std::vector<uint64_t> const& seq,
                   no_next_geq_tag const&)
{
    test_move_next(r, seq);
}

template <typename SequenceReader>
typename std::enable_if<ds2i::has_next_geq<SequenceReader>::value, void>::type
test_sequence(SequenceReader r, std::vector<uint64_t> const& seq,
              next_geq_tag const&)
{
    test_move_next(r, seq);
    test_next_geq(r, seq);
}

template <typename SequenceReader>
void test_sequence(SequenceReader r, std::vector<uint64_t> const& seq)
{
    test_sequence(r, seq, next_geq_tag());
}

template <typename ParamsType, typename SequenceType>
inline void test_sequence(SequenceType,
                          ParamsType const& params,
                          uint64_t universe,
                          std::vector<uint64_t> const& seq)
{
    succinct::bit_vector_builder bvb;
    SequenceType::write(bvb, seq.begin(), universe, seq.size(), params);
    succinct::bit_vector bv(&bvb);
    typename SequenceType::enumerator r(bv, 0, universe, seq.size(), params);
    test_sequence(r, seq);
}


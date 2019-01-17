#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_common.hpp"
#include "test_rank_select_common.hpp"

#include <cstdlib>
#include "succinct/mapper.hpp"
#include "bit_vector.hpp"

TEST_CASE("bit_vector")
{
    srand(42);

    std::vector<bool> v = random_bit_vector();

    {
        pisa::bit_vector_builder bvb;
        for (size_t i = 0; i < v.size(); ++i) {
            bvb.push_back(v[i]);
        }

        pisa::bit_vector bitmap(&bvb);
        test_equal_bits(v, bitmap, "Random bits (push_back)");
    }

    {
        pisa::bit_vector_builder bvb(v.size());
        for (size_t i = 0; i < v.size(); ++i) {
            bvb.set(i, v[i]);
        }
        bvb.push_back(0);
        v.push_back(0);
        bvb.push_back(1);
        v.push_back(1);

        pisa::bit_vector bitmap(&bvb);
        test_equal_bits(v, bitmap, "Random bits (set)");
    }

    uint64_t ints[] = {uint64_t(-1), uint64_t(1) << 63, 1, 1, 1, 3, 5, 7, 0xFFF, 0xF0F, 1, 0xFFFFFF, 0x123456, uint64_t(1) << 63, uint64_t(-1)};
    {
        pisa::bit_vector_builder bvb;
        for(uint64_t i : ints) {
            uint64_t len = pisa::broadword::msb(i) + 1;
            bvb.append_bits(i, len);
        }
        pisa::bit_vector bitmap(&bvb);
        uint64_t pos = 0;
        for(uint64_t i : ints) {
            uint64_t len = pisa::broadword::msb(i) + 1;
            REQUIRE(i == bitmap.get_bits(pos, len));
            pos += len;
        }
    }

    {
        using pisa::broadword::msb;
        std::vector<size_t> positions(1);
        for(uint64_t i : ints) {
            positions.push_back(positions.back() + msb(i) + 1);
        }

        pisa::bit_vector_builder bvb(positions.back());

        for (size_t i = 0; i < positions.size() - 1; ++i) {
            uint64_t v = ints[i];
            uint64_t len = positions[i + 1] - positions[i];
            bvb.set_bits(positions[i], v, len);
        }

        pisa::bit_vector bitmap(&bvb);
        for (size_t i = 0; i < positions.size() - 1; ++i) {
            uint64_t v = ints[i];
            uint64_t len = positions[i + 1] - positions[i];
            REQUIRE(v == bitmap.get_bits(positions[i], len));
        }
    }
}

TEST_CASE("bit_vector_enumerator")
{
    srand(42);
    std::vector<bool> v = random_bit_vector();
    pisa::bit_vector bitmap(v);

    size_t i = 0;
    size_t pos = 0;

    pisa::bit_vector::enumerator e(bitmap, pos);
    while (pos < bitmap.size()) {
        bool next = e.next();
        MY_REQUIRE_EQUAL(next, v[pos], "pos = " << pos << " i = " << i);
        pos += 1;

        pos += size_t(rand()) % (bitmap.size() - pos + 1);
        e = pisa::bit_vector::enumerator(bitmap, pos);
        i += 1;
    }
}

TEST_CASE("bit_vector_unary_enumerator")
{
    srand(42);
    uint64_t n = 20000;
    std::vector<bool> v = random_bit_vector(n);

    // punch some long gaps in v
    for (size_t g = 0; g < n / 1000; ++g) {
        ssize_t l = std::min(ssize_t(rand() % 256), ssize_t(v.size() - g));
        std::fill(v.begin(), v.begin() + l, 0);
    }

    pisa::bit_vector bitmap(v);

    std::vector<size_t> ones;
    for (size_t i = 0; i < v.size(); ++i) {
        if (bitmap[i]) {
            ones.push_back(i);
        }
    }

    {
        pisa::bit_vector::unary_enumerator e(bitmap, 0);

        for (size_t r = 0; r < ones.size(); ++r) {
            uint64_t pos = e.next();
            MY_REQUIRE_EQUAL(ones[r], pos,
                             "r = " << r);
        }
    }

    {
        pisa::bit_vector::unary_enumerator e(bitmap, 0);

        for (size_t r = 0; r < ones.size(); ++r) {
            for (size_t k = 0; k < std::min(size_t(256), size_t(ones.size() - r)); ++k) {
                pisa::bit_vector::unary_enumerator ee(e);
                ee.skip(k);
                uint64_t pos = ee.next();
                MY_REQUIRE_EQUAL(ones[r + k], pos,
                                 "r = " << r << " k = " << k);
            }
            e.next();
        }
    }

    {
        pisa::bit_vector::unary_enumerator e(bitmap, 0);

        for (size_t r = 0; r < ones.size(); ++r) {
            for (size_t k = 0; k < std::min(size_t(256), size_t(ones.size() - r)); ++k) {
                pisa::bit_vector::unary_enumerator ee(e);
                uint64_t pos_skip = ee.skip_no_move(k);
                uint64_t pos = ee.next();
                MY_REQUIRE_EQUAL(ones[r], pos,
                                 "r = " << r << " k = " << k);
                MY_REQUIRE_EQUAL(ones[r + k], pos_skip,
                                 "r = " << r << " k = " << k);

            }
            e.next();
        }
    }

    {
        pisa::bit_vector::unary_enumerator e(bitmap, 0);

        for (size_t pos = 0; pos < v.size(); ++pos) {
            uint64_t skip = 0;
            for (size_t d = 0; d < std::min(size_t(256), size_t(v.size() - pos)); ++d) {
                if (v[pos + d] == 0) {
                    pisa::bit_vector::unary_enumerator ee(bitmap, pos);
                    ee.skip0(skip);

                    uint64_t expected_pos = pos + d;
                    for (; !v[expected_pos] && expected_pos < v.size(); ++expected_pos);
                    if (!v[expected_pos]) break;
                    uint64_t pos = ee.next();
                    MY_REQUIRE_EQUAL(expected_pos, pos,
                                     "pos = " << pos << " skip = " << skip);

                    skip += 1;
                }
            }
        }
    }
}

void test_bvb_reverse(size_t n)
{
    std::vector<bool> v = random_bit_vector(n);
    pisa::bit_vector_builder bvb;
    for (size_t i = 0; i < v.size(); ++i) {
        bvb.push_back(v[i]);
    }

    std::reverse(v.begin(), v.end());
    bvb.reverse();

    pisa::bit_vector bitmap(&bvb);
    test_equal_bits(v, bitmap, "In-place reverse");
}

TEST_CASE("bvb_reverse")
{
    srand(42);

    test_bvb_reverse(0);
    test_bvb_reverse(63);
    test_bvb_reverse(64);
    test_bvb_reverse(1000);
    test_bvb_reverse(1024);
}

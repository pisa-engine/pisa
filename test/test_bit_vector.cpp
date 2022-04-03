#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <cstdlib>

#include <rapidcheck.h>

#include "bit_vector.hpp"
#include "bit_vector_builder.hpp"
#include "mappable/mapper.hpp"
#include "test_common.hpp"
#include "test_rank_select_common.hpp"

TEST_CASE("bit_vector")
{
    rc::check([](std::vector<bool> v) {
        {
            pisa::bit_vector_builder bvb;
            for (auto elem: v) {
                bvb.push_back(elem);
            }

            pisa::bit_vector bitmap(&bvb);
            test_equal_bits(v, bitmap, "Random bits (push_back)");
        }

        {
            pisa::bit_vector_builder bvb(v.size());
            for (size_t i = 0; i < v.size(); ++i) {
                bvb.set(i, v[i]);
            }
            bvb.push_back(false);
            v.push_back(false);
            bvb.push_back(true);
            v.push_back(true);

            pisa::bit_vector bitmap(&bvb);
            test_equal_bits(v, bitmap, "Random bits (set)");
        }

        auto ints = std::array<uint64_t, 15>{uint64_t(-1),
                                             uint64_t(1) << 63u,
                                             1,
                                             1,
                                             1,
                                             3,
                                             5,
                                             7,
                                             0xFFF,
                                             0xF0F,
                                             1,
                                             0xFFFFFF,
                                             0x123456,
                                             uint64_t(1) << 63u,
                                             uint64_t(-1)};
        {
            pisa::bit_vector_builder bvb;
            for (uint64_t i: ints) {
                uint64_t len = pisa::broadword::msb(i) + 1;
                bvb.append_bits(i, len);
            }
            pisa::bit_vector bitmap(&bvb);
            uint64_t pos = 0;
            for (uint64_t i: ints) {
                uint64_t len = pisa::broadword::msb(i) + 1;
                REQUIRE(i == bitmap.get_bits(pos, len));
                pos += len;
            }
        }

        {
            using pisa::broadword::msb;
            std::vector<size_t> positions(1);
            for (uint64_t i: ints) {
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
    });
}

TEST_CASE("bit_vector_enumerator")
{
    rc::check([](std::vector<bool> v) {
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
    });
}

TEST_CASE("bit_vector_unary_enumerator")
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::bernoulli_distribution d(0.5);
    std::vector<bool> v(20'000);
    std::generate(v.begin(), v.end(), [&]() { return d(gen); });

    [&]() {
        std::vector<std::size_t> posv(v.size());
        std::vector<std::size_t> intervals;
        std::iota(posv.begin(), posv.end(), 0);
        std::sample(
            posv.begin(),
            posv.end(),
            std::back_inserter(intervals),
            40,
            std::mt19937{std::random_device{}()});
        REQUIRE(intervals.size() % 2 == 0);
        for (auto left = intervals.begin(); left != intervals.end(); std::advance(left, 2)) {
            auto right = std::next(left);
            std::fill(std::next(v.begin(), *left), std::next(v.begin(), *right), false);
        }
    }();

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
            MY_REQUIRE_EQUAL(ones[r], pos, "r = " << r);
        }
    }

    {
        pisa::bit_vector::unary_enumerator e(bitmap, 0);

        for (size_t r = 0; r < ones.size(); ++r) {
            for (size_t k = 0; k < std::min(size_t(256), size_t(ones.size() - r)); ++k) {
                pisa::bit_vector::unary_enumerator ee(e);
                ee.skip(k);
                uint64_t pos = ee.next();
                MY_REQUIRE_EQUAL(ones[r + k], pos, "r = " << r << " k = " << k);
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
                MY_REQUIRE_EQUAL(ones[r], pos, "r = " << r << " k = " << k);
                MY_REQUIRE_EQUAL(ones[r + k], pos_skip, "r = " << r << " k = " << k);
            }
            e.next();
        }
    }

    {
        pisa::bit_vector::unary_enumerator e(bitmap, 0);

        for (size_t pos = 0; pos < v.size(); ++pos) {
            uint64_t skip = 0;
            for (size_t d = 0; d < std::min(size_t(256), size_t(v.size() - pos)); ++d) {
                if (not v[pos + d]) {
                    pisa::bit_vector::unary_enumerator ee(bitmap, pos);
                    ee.skip0(skip);

                    uint64_t expected_pos = pos + d;
                    for (; expected_pos < v.size() && !v[expected_pos]; ++expected_pos) {
                    }
                    if (expected_pos == v.size()) {
                        break;
                    }
                    uint64_t pos = ee.next();
                    MY_REQUIRE_EQUAL(expected_pos, pos, "pos = " << pos << " skip = " << skip);

                    skip += 1;
                }
            }
        }
    }
}

TEST_CASE("bvb_reverse")
{
    rc::check([](std::vector<bool> v) {
        pisa::bit_vector_builder bvb;
        for (auto elem: v) {
            bvb.push_back(elem);
        }

        std::reverse(v.begin(), v.end());
        bvb.reverse();

        pisa::bit_vector bitmap(&bvb);
        test_equal_bits(v, bitmap, "In-place reverse");
    });
}

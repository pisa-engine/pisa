#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_common.hpp"

#include "mio/mmap.hpp"

#include "mappable/mapper.hpp"

TEST_CASE("basic_map")
{
    pisa::mapper::mappable_vector<int> vec;
    REQUIRE(vec.size() == 0U);

    int nums[] = {1, 2, 3, 4};
    vec.assign(nums);

    REQUIRE(4U == vec.size());
    REQUIRE(1 == vec[0]);
    REQUIRE(4 == vec[3]);

    pisa::mapper::freeze(vec, "temp.bin");

    {
        pisa::mapper::mappable_vector<int> mapped_vec;
        mio::mmap_source m("temp.bin");
        pisa::mapper::map(mapped_vec, m);
        REQUIRE(vec.size() == mapped_vec.size());
        REQUIRE(std::equal(vec.begin(), vec.end(), mapped_vec.begin()));
    }

    std::remove("temp.bin");
}

class complex_struct {
  public:
    complex_struct() : m_a(0) {}

    void init()
    {
        m_a = 42;
        uint32_t b[] = {1, 2};
        m_b.assign(b);
    }

    template <typename Visitor>
    void map(Visitor& visit)
    {
        visit(m_a, "m_a")(m_b, "m_b");
    }

    uint64_t m_a;
    pisa::mapper::mappable_vector<uint32_t> m_b;
};

TEST_CASE("complex_struct_map")
{
    complex_struct s;
    s.init();
    pisa::mapper::freeze(s, "temp.bin");

    REQUIRE(24 == pisa::mapper::size_of(s));

    complex_struct mapped_s;
    REQUIRE(0 == mapped_s.m_a);
    REQUIRE(0U == mapped_s.m_b.size());

    {
        mio::mmap_source m("temp.bin");
        pisa::mapper::map(mapped_s, m);
        REQUIRE(s.m_a == mapped_s.m_a);
        REQUIRE(s.m_b.size() == mapped_s.m_b.size());
    }

    std::remove("temp.bin");
}

#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <tbb/task_scheduler_init.h>

//#include "cursor/scored_cursor.hpp"
#include "pisa_config.hpp"
//#include "query/queries.hpp"
//#include "test_common.hpp"
#include "v1/index.hpp"
#include "v1/types.hpp"
//#include "wand_utils.hpp"

using pisa::v1::DocId;
using pisa::v1::Frequency;
using pisa::v1::RawReader;
using pisa::v1::TermId;
using namespace pisa;

// template <typename Index>
// struct IndexData {
//
//    static std::unique_ptr<IndexData> data;
//
//    IndexData()
//        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
//          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
//          wdata(document_sizes.begin()->begin(),
//                collection.num_docs(),
//                collection,
//                BlockSize(FixedBlock()))
//
//    {
//        tbb::task_scheduler_init init;
//        typename Index::builder builder(collection.num_docs(), params);
//        for (auto const &plist : collection) {
//            uint64_t freqs_sum =
//                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
//            builder.add_posting_list(
//                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
//        }
//        builder.build(index);
//
//        term_id_vec q;
//        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
//        auto push_query = [&](std::string const &query_line) {
//            queries.push_back(parse_query_ids(query_line));
//        };
//        io::for_each_line(qfile, push_query);
//
//        std::string t;
//        std::ifstream tin(PISA_SOURCE_DIR "/test/test_data/top5_thresholds");
//        while (std::getline(tin, t)) {
//            thresholds.push_back(std::stof(t));
//        }
//    }
//
//    [[nodiscard]] static auto get()
//    {
//        if (IndexData::data == nullptr) {
//            IndexData::data = std::make_unique<IndexData<Index>>();
//        }
//        return IndexData::data.get();
//    }
//
//    global_parameters params;
//    binary_freq_collection collection;
//    binary_collection document_sizes;
//    Index index;
//    std::vector<Query> queries;
//    std::vector<float> thresholds;
//    wand_data<wand_data_raw> wdata;
//};
//
// template <typename Index>
// std::unique_ptr<IndexData<Index>> IndexData<Index>::data = nullptr;

template <typename T>
std::ostream &operator<<(std::ostream &os, tl::optional<T> const &val)
{
    if (val.has_value()) {
        os << val.value();
    } else {
        os << "nullopt";
    }
    return os;
}

TEST_CASE("RawReader", "[v1][unit]")
{
    std::vector<std::uint64_t> const mem{0, 1, 2, 3, 4};
    RawReader<uint64_t> reader;
    auto cursor = reader.read(gsl::as_bytes(gsl::make_span(mem)));
    REQUIRE(cursor.value().value() == tl::make_optional(mem[0]).value());
    REQUIRE(cursor.next() == tl::make_optional(mem[1]));
    REQUIRE(cursor.next() == tl::make_optional(mem[2]));
    REQUIRE(cursor.next() == tl::make_optional(mem[3]));
    REQUIRE(cursor.next() == tl::make_optional(mem[4]));
    REQUIRE(cursor.next() == tl::nullopt);
}

template <typename Cursor, typename Transform>
auto collect(Cursor &&cursor, Transform transform)
{
    std::vector<std::decay_t<decltype(transform(cursor))>> vec;
    while (not cursor.empty()) {
        vec.push_back(transform(cursor));
        cursor.step();
    }
    return vec;
}

template <typename Cursor>
auto collect(Cursor &&cursor)
{
    return collect(std::forward<Cursor>(cursor), [](auto &&cursor) { return *cursor; });
}

TEST_CASE("Binary collection index", "[v1][unit]")
{
    /* auto data = IndexData<single_index>::get(); */
    /* ranked_or_query or_q(10); */

    /* for (auto const &q : data->queries) { */
    /*     or_q(make_scored_cursors(data->index, data->wdata, q), data->index.num_docs()); */
    /*     auto results = or_q.topk(); */
    /* } */

    binary_freq_collection collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
    auto index =
        pisa::v1::binary_collection_index(PISA_SOURCE_DIR "/test/test_data/test_collection");
    auto term_id = 0;
    for (auto sequence : collection) {
        CAPTURE(term_id);
        REQUIRE(std::vector<std::uint32_t>(sequence.docs.begin(), sequence.docs.end())
                == collect(index.documents(term_id)));
        REQUIRE(std::vector<std::uint32_t>(sequence.freqs.begin(), sequence.freqs.end())
                == collect(index.payloads(term_id)));
        term_id += 1;
    }
    term_id = 0;
    for (auto sequence : collection) {
        CAPTURE(term_id);
        REQUIRE(std::vector<std::uint32_t>(sequence.docs.begin(), sequence.docs.end())
                == collect(index.cursor(term_id)));
        REQUIRE(std::vector<std::uint32_t>(sequence.freqs.begin(), sequence.freqs.end())
                == collect(index.cursor(term_id), [](auto &&cursor) { return *cursor.payload(); }));
        term_id += 1;
    }
}

TEST_CASE("Bigram collection index", "[v1][unit]")
{
    auto intersect = [](auto const &lhs,
                        auto const &rhs) -> std::vector<std::tuple<DocId, Frequency, Frequency>> {
        std::vector<std::tuple<DocId, Frequency, Frequency>> intersection;
        auto left = lhs.begin();
        auto right = rhs.begin();
        while (left != lhs.end() && right != rhs.end()) {
            if (left->first == right->first) {
                intersection.emplace_back(left->first, left->second, right->second);
                ++right;
                ++left;
            } else if (left->first < right->first) {
                ++left;
            } else {
                ++right;
            }
        }
        return intersection;
    };
    auto to_vec = [](auto const &seq) {
        std::vector<std::pair<DocId, Frequency>> vec;
        std::transform(seq.docs.begin(),
                       seq.docs.end(),
                       seq.freqs.begin(),
                       std::back_inserter(vec),
                       [](auto doc, auto freq) { return std::make_pair(doc, freq); });
        return vec;
    };

    binary_freq_collection collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
    auto index =
        pisa::v1::binary_collection_bigram_index(PISA_SOURCE_DIR "/test/test_data/test_collection");

    auto pos = collection.begin();
    auto prev = to_vec(*pos);
    ++pos;
    TermId term_id = 1;
    for (; pos != collection.end(); ++pos, ++term_id) {
        auto current = to_vec(*pos);
        auto intersection = intersect(prev, current);
        if (not intersection.empty()) {
            auto id = index.bigram_id(term_id - 1, term_id);
            REQUIRE(id.has_value());
            auto postings = collect(index.cursor(*id), [](auto &cursor) {
                auto freqs = *cursor.payload();
                return std::make_tuple(*cursor, freqs[0], freqs[1]);
            });
            REQUIRE(postings == intersection);
        }
        std::swap(prev, current);
        break;
    }
}

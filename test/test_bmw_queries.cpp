#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <unordered_map>

#include "test_common.hpp"

#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "index_types.hpp"
#include "pisa_config.hpp"
#include "query/queries.hpp"

using namespace pisa;

using WandTypeUniform = wand_data<wand_data_compressed>;
using WandTypePlain = wand_data<wand_data_raw>;

template <typename Index>
struct IndexData {

    static std::unordered_map<std::string, std::unique_ptr<IndexData>> data;


    IndexData(std::string const &scorer_name)
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(document_sizes.begin()->begin(),
                collection.num_docs(),
                collection,
                scorer_name,
                BlockSize(VariableBlock()))

    {
        typename Index::builder builder(collection.num_docs(), params);
        for (auto const &plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(index);

        auto process_term = [](auto str) { return std::stoi(str); };

        term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const &query_line) {
            queries.push_back(parse_query(query_line, process_term, {}));
        };
        io::for_each_line(qfile, push_query);
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    Index index;
    std::vector<Query> queries;
    WandTypePlain wdata;

    [[nodiscard]] static auto get(std::string const &s_name)
    {
        if (IndexData::data.find(s_name) == IndexData::data.end()) {
            IndexData::data[s_name] = std::make_unique<IndexData<Index>>(s_name);
        }
        return IndexData::data[s_name].get();
    }
};

template <typename Index>
std::unordered_map<std::string, unique_ptr<IndexData<Index>>> IndexData<Index>::data = {};

template <typename Wand>
auto test(Wand &wdata, std::string const &s_name)
{
    auto data = IndexData<single_index>::get(s_name);
    block_max_wand_query op_q(10);
    wand_query wand_q(10);

    auto test_with_scorers =
        [&](auto scorer1, auto scorer2) {
            for (auto const &q : data->queries) {
                wand_q(make_max_scored_cursors(data->index, data->wdata, scorer1, q),
                       data->index.num_docs());
                op_q(make_block_max_scored_cursors(data->index, wdata, scorer2, q),
                     data->index.num_docs());
                REQUIRE(wand_q.topk().size() == op_q.topk().size());

                for (size_t i = 0; i < wand_q.topk().size(); ++i) {
                    REQUIRE(wand_q.topk()[i].first
                            == Approx(op_q.topk()[i].first).epsilon(0.01)); // tolerance is %
                                                                            // relative
                }
                op_q.clear_topk();
            }
        };

    PISA_WITH_SCORER_TYPE(Scorer1,
                          s_name,
                          decltype(data->wdata),
                          PISA_WITH_SCORER_TYPE(Scorer2, s_name, decltype(wdata), {
                              test_with_scorers(Scorer1(data->wdata), Scorer2(wdata));
                          }))
}

TEST_CASE("block_max_wand", "[bmw][query][ranked][integration]", )
{
    for (auto &&s_name : {"bm25", "qld"}) {

        auto data = IndexData<single_index>::get(s_name);

        SECTION("Regular") { test(data->wdata, s_name); }
        SECTION("Fixed")
        {
            WandTypePlain wdata_fixed(data->document_sizes.begin()->begin(),
                                      data->collection.num_docs(),
                                      data->collection,
                                      s_name,
                                      BlockSize(FixedBlock()));
            test(wdata_fixed, s_name);
        }
        SECTION("Uniform")
        {
            WandTypeUniform wdata_uniform(data->document_sizes.begin()->begin(),
                                          data->collection.num_docs(),
                                          data->collection,
                                          s_name,
                                          BlockSize(VariableBlock()));
            test(wdata_uniform, s_name);
        }
    }
}

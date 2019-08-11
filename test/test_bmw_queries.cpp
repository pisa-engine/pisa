#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <unordered_map>

#include "test_common.hpp"

#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "index_types.hpp"
#include "pisa_config.hpp"
#include "query/algorithm.hpp"
#include "query/queries.hpp"

using namespace pisa;

using WandTypeUniform = wand_data<wand_data_compressed>;
using WandTypePlain = wand_data<wand_data_raw>;

template <typename Index>
struct IndexData {

    static std::unordered_map<std::string, std::unique_ptr<IndexData>> data;

    explicit IndexData(std::string const &scorer_name)
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
                std::accumulate(plist.frequencies.begin(), plist.frequencies.end(), uint64_t(0));
            builder.add_posting_list(plist.documents.size(),
                                     plist.documents.begin(),
                                     plist.frequencies.begin(),
                                     freqs_sum);
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
    BinaryFreqCollection collection;
    BinaryCollection document_sizes;
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

    with_scorer(s_name, data->wdata, [&](auto scorer1) {
        with_scorer(s_name, wdata, [&](auto scorer2) {
            for (auto const &q : data->queries) {
                auto mscursors = make_max_scored_cursors(data->index, data->wdata, scorer1, q);
                wand_q(gsl::make_span(mscursors), data->index.num_docs());
                auto bmscursors = make_block_max_scored_cursors(data->index, wdata, scorer2, q);
                op_q(gsl::make_span(bmscursors), data->index.num_docs());
                REQUIRE(wand_q.topk().size() == op_q.topk().size());

                for (size_t i = 0; i < wand_q.topk().size(); ++i) {
                    REQUIRE(wand_q.topk()[i].first
                            == Approx(op_q.topk()[i].first).epsilon(0.01)); // tolerance is %
                                                                            // relative
                }
                op_q.clear_topk();
            }
        });
    });
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

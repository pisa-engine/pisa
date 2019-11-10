#include <iostream>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/optional.hpp"

#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"
#include "cursor/max_scored_cursor.hpp"

#include "index_types.hpp"
#include "query/queries.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

#include "util/progress.hpp"

#include "picojson/picojson.h"

#include <unordered_map>

using namespace pisa;

struct term_data_t
{
    float id;
    float wand_upper;
    float Ft;
    float mean_ft;
    float med_ft;
    float min_ft;
    float max_ft;
    float num_ft_geq_256;
    float num_ft_geq_128;
    float num_ft_geq_64;
    float num_ft_geq_32;
    float num_ft_geq_16;
    float num_ft_geq_8;
    float num_ft_geq_4;
    float num_ft_geq_2;
    float block_score_1;
    float block_score_2;
    float block_score_4;
    float block_score_8;
    float block_score_16;
    float block_score_32;
    float block_score_64;
    float block_score_128;
    float block_score_256;
    float block_score_512;
    float block_score_1024;
    float block_score_2048;
    float block_score_4096;
    float block_score_small;
};

struct query_data
{
    std::string id;
    float wand_thres_10;
    float wand_thres_100;
    float wand_thres_1000;
    std::vector<uint32_t> term_ids;
    std::vector<term_data_t> term_data;
};

template <typename IndexType, typename WandType>
void output_stats(const std::string &index_filename, const std::string &wand_data_filename, std::vector<Query> &queries)
{
    using namespace pisa;
    IndexType index;
    spdlog::info("Loading index from {}", index_filename);
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    std::unordered_map<uint32_t, term_data_t> term_data_cache;

    WandType wdata;
    mio::mmap_source md;
    std::error_code error;
    md.map(wand_data_filename, error);
    if (error)
    {
        std::cerr << "error mapping file: " << error.message() << ", exiting..." << std::endl;
        throw std::runtime_error("Error opening file");
    }
    mapper::map(wdata, md, mapper::map_flags::warmup);

    std::vector<query_data> qds;

    pisa::progress q_progress("query stats", queries.size());
    q_progress.update(0);
    for (auto const &query : queries)
    {
        q_progress.update(1);
        query_data qd;
        qd.id = *(query.id);
        for (size_t i = 0; i < query.terms.size(); ++i)
        {
            uint32_t term_id = query.terms[i];
            qd.term_ids.push_back(term_id);

            term_data_t td;
            auto cache_itr = term_data_cache.find(term_id);
            if (cache_itr != term_data_cache.end())
                td = cache_itr->second;
            else
            {
                auto list = index[term_id];
                td.id = term_id;
                td.Ft = list.size();
                td.wand_upper = wdata.max_term_weight(term_id);
                auto w_enum     = wdata.getenum(term_id);
                std::vector<float> block_scores;
                for (size_t i = 0; i < w_enum.size(); ++i)
                {
                    block_scores.push_back(w_enum.score());
                    w_enum.next_block();
                }
                std::sort(block_scores.begin(), block_scores.end());
                if(block_scores.size() > 0){
                    td.block_score_1 = block_scores[0];
                }
                if(block_scores.size() > 1){
                    td.block_score_2 = block_scores[1];
                }
                if(block_scores.size() > 3){
                    td.block_score_4 = block_scores[3];
                }
                if(block_scores.size() > 7){
                    td.block_score_8 = block_scores[7];
                }
                if(block_scores.size() > 15){
                    td.block_score_16 = block_scores[15];
                }
                if(block_scores.size() > 31){
                    td.block_score_32 = block_scores[31];
                }
                if(block_scores.size() > 63){
                    td.block_score_64 = block_scores[63];
                }
                if(block_scores.size() > 127){
                    td.block_score_128 = block_scores[127];
                }
                if(block_scores.size() > 255){
                    td.block_score_256 = block_scores[255];
                }
                if(block_scores.size() > 511){
                    td.block_score_512 = block_scores[511];
                }
                if(block_scores.size() > 1023){
                    td.block_score_1024 = block_scores[1023];
                }
                if(block_scores.size() > 2047){
                    td.block_score_2048 = block_scores[2047];
                }
                if(block_scores.size() > 4095){
                    td.block_score_4096 = block_scores[4095];
                }
                if(block_scores.size() > 0){
                    td.block_score_small = block_scores.back();
                }

                std::vector<float> freqs(td.Ft);
                double freqs_sum = 0;
                for (size_t j = 0; j < td.Ft; j++)
                {
                    freqs[j] = list.freq();
                    freqs_sum += freqs[j];
                    list.next();
                }
                std::sort(freqs.begin(), freqs.end());

                td.min_ft = freqs.front();
                td.max_ft = freqs.back();
                td.med_ft = freqs[freqs.size() / 2];
                td.mean_ft = freqs_sum / td.Ft;

                for (size_t j = 1; j < freqs.size(); j++)
                {
                    if (freqs[j - 1] < 2 && freqs[j] >= 2)
                        td.num_ft_geq_2 = float(freqs.size() - j);
                    if (freqs[j - 1] < 4 && freqs[j] >= 4)
                        td.num_ft_geq_4 = float(freqs.size() - j);
                    if (freqs[j - 1] < 8 && freqs[j] >= 8)
                        td.num_ft_geq_8 = float(freqs.size() - j);
                    if (freqs[j - 1] < 16 && freqs[j] >= 16)
                        td.num_ft_geq_16 = float(freqs.size() - j);
                    if (freqs[j - 1] < 32 && freqs[j] >= 32)
                        td.num_ft_geq_32 = float(freqs.size() - j);
                    if (freqs[j - 1] < 64 && freqs[j] >= 64)
                        td.num_ft_geq_64 = float(freqs.size() - j);
                    if (freqs[j - 1] < 128 && freqs[j] >= 128)
                        td.num_ft_geq_128 = float(freqs.size() - j);
                    if (freqs[j - 1] < 256 && freqs[j] >= 256)
                    {
                        td.num_ft_geq_256 = float(freqs.size() - j);
                        break;
                    }
                }
                term_data_cache[term_id] = td;
            }
            qd.term_data.push_back(td);
        }

        wand_query wq(10);
        auto results = wq(make_max_scored_cursors(index, wdata, query), index.num_docs());
        do_not_optimize_away(results);
        auto &topk = wq.topk();

        if (topk.size() == 10)
        {
            qd.wand_thres_10 = topk.back().first;
            qds.push_back(qd);
        }
    }

    for (const auto &qd : qds)
    {
        picojson::object json_qry;
        json_qry["id"] = picojson::value(stof(qd.id));
        json_qry["wand_thres_10"] = picojson::value(float(qd.wand_thres_10));
        picojson::array arr;
        for (size_t r = 0; r < qd.term_ids.size(); r++)
        {
            arr.push_back(picojson::value(double(qd.term_ids[r])));
        }
        json_qry["term_ids"] = picojson::value(arr);
        picojson::array arr_td;
        for (size_t r = 0; r < qd.term_data.size(); r++)
        {
            auto &td = qd.term_data[r];
            picojson::object json_term;
            json_term["id"] = picojson::value(float(td.id));
            json_term["wand_upper"] = picojson::value(float(td.wand_upper));
            json_term["Ft"] = picojson::value(float(td.Ft));
            json_term["mean_ft"] = picojson::value(float(td.mean_ft));
            json_term["med_ft"] = picojson::value(float(td.med_ft));
            json_term["min_ft"] = picojson::value(float(td.min_ft));
            json_term["max_ft"] = picojson::value(float(td.max_ft));
            json_term["num_ft_geq_256"] = picojson::value(float(td.num_ft_geq_256));
            json_term["num_ft_geq_128"] = picojson::value(float(td.num_ft_geq_128));
            json_term["num_ft_geq_64"] = picojson::value(float(td.num_ft_geq_64));
            json_term["num_ft_geq_32"] = picojson::value(float(td.num_ft_geq_32));
            json_term["num_ft_geq_16"] = picojson::value(float(td.num_ft_geq_16));
            json_term["num_ft_geq_8"] = picojson::value(float(td.num_ft_geq_8));
            json_term["num_ft_geq_4"] = picojson::value(float(td.num_ft_geq_4));
            json_term["num_ft_geq_2"] = picojson::value(float(td.num_ft_geq_2));
            arr_td.push_back(picojson::value(json_term));
        }
        json_qry["term_data"] = picojson::value(arr_td);
        picojson::value v(json_qry);
        std::cout << v.serialize() << std::endl;
    }


}

typedef wand_data<wand_data_raw> wand_raw_index;

int main(int argc, const char **argv)
{
    using namespace pisa;

    std::string type = argv[1];
    std::string index_filename = argv[2];
    std::string query_filename = argv[3];
    std::string wand_data_filename = argv[4];

    std::optional<std::string> terms_file;
    std::optional<std::string> stopwords_filename;
    std::optional<std::string> stemmer = std::nullopt;

    std::vector<Query> queries;
    auto push_query = resolve_query_parser(queries, terms_file, stopwords_filename, stemmer);

    std::ifstream is(query_filename);
    io::for_each_line(is, push_query);

    if (false)
    {
#define LOOP_BODY(R, DATA, T)                                  \
    }                                                          \
    else if (type == BOOST_PP_STRINGIZE(T))                    \
    {                                                          \
        output_stats<BOOST_PP_CAT(T, _index), wand_raw_index>( \
            index_filename, wand_data_filename, queries);      \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
    }
    else
    {
        spdlog::error("ERROR: Unknown type {}",  type);
    }
}

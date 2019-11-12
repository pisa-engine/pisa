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
#include "scorer/bm25.hpp"

using namespace pisa;

struct term_data_t
{
    float id= 0;
    float wand_upper= 0;
    float Ft= 0;
    float mean_ft= 0;
    float med_ft= 0;
    float min_ft= 0;
    float max_ft= 0;
    float num_ft_geq_256= 0;
    float num_ft_geq_128= 0;
    float num_ft_geq_64= 0;
    float num_ft_geq_32= 0;
    float num_ft_geq_16= 0;
    float num_ft_geq_8= 0;
    float num_ft_geq_4= 0;
    float num_ft_geq_2= 0;
    float block_score_1= 0;
    float block_score_2= 0;
    float block_score_4= 0;
    float block_score_8= 0;
    float block_score_16= 0;
    float block_score_32= 0;
    float block_score_64= 0;
    float block_score_128= 0;
    float block_score_256= 0;
    float block_score_512= 0;
    float block_score_1024= 0;
    float block_score_2048= 0;
    float block_score_4096= 0;
    float block_score_small= 0;
    float mean_doclen = 0;
    float med_doclen = 0;
    float min_doclen = 0;
    float max_doclen = 0;
    float q_weight = 0;
    float k10m = 0;
    float k100m = 0;
    float k1000m = 0;

};

struct query_data
{
    std::string id;
    float wand_thres_10 = 0;
    float wand_thres_100 = 0;
    float wand_thres_1000 = 0;
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
                td.q_weight   = bm25::query_term_weight(1, list.size(), index.num_docs());

                auto w_enum     = wdata.getenum(term_id);
                std::vector<float> block_scores;
                for (size_t i = 0; i < w_enum.size(); ++i)
                {
                    block_scores.push_back(w_enum.score());
                    w_enum.next_block();
                }

                std::sort(block_scores.begin(), block_scores.end(), std::greater<float>());
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

                std::vector<float> doc_lens(td.Ft);
                double doc_lens_sum = 0;
                std::vector<float> freqs(td.Ft);
                double freqs_sum = 0;
                std::vector<float> scores(td.Ft);

                for (size_t j = 0; j < td.Ft; j++)
                {
                    auto docid = list.docid();
                    doc_lens[j] = wdata.doc_len(docid);
                    doc_lens_sum += doc_lens[j];
                    freqs[j] = list.freq();
                    freqs_sum += freqs[j];
                    scores[j] = td.q_weight * bm25::doc_term_weight(list.freq(), wdata.norm_len(list.docid()));
                    list.next();
                }
                std::sort(doc_lens.begin(), doc_lens.end());
                std::sort(freqs.begin(), freqs.end());
                std::sort(scores.begin(), scores.end(), std::greater<float>());


                td.min_doclen = doc_lens.front();
                td.max_doclen = doc_lens.back();
                td.med_doclen = doc_lens[doc_lens.size() / 2];
                td.mean_doclen = doc_lens_sum / td.Ft;

                td.min_ft = freqs.front();
                td.max_ft = freqs.back();
                td.med_ft = freqs[freqs.size() / 2];
                td.mean_ft = freqs_sum / td.Ft;

                if(scores.size() > 9){
                    td.k10m = scores[9];
                }
                if(scores.size() > 99){
                    td.k100m = scores[99];
                }
                if(scores.size() > 999){
                    td.k1000m = scores[999];
                }

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

        {
            wand_query wq(10);
            auto results = wq(make_max_scored_cursors(index, wdata, query), index.num_docs());
            do_not_optimize_away(results);
            auto &topk = wq.topk();

            if (topk.size() == 10)
            {
                qd.wand_thres_10 = topk.back().first;
            }
        }
        {
            wand_query wq(100);
            auto results = wq(make_max_scored_cursors(index, wdata, query), index.num_docs());
            do_not_optimize_away(results);
            auto &topk = wq.topk();

            if (topk.size() == 100)
            {
                qd.wand_thres_100 = topk.back().first;
            }
        }
        {
            wand_query wq(1000);
            auto results = wq(make_max_scored_cursors(index, wdata, query), index.num_docs());
            do_not_optimize_away(results);
            auto &topk = wq.topk();

            if (topk.size() == 1000)
            {
                qd.wand_thres_1000 = topk.back().first;
            }
        }
        qds.push_back(qd);
    }

    for (const auto &qd : qds)
    {
        picojson::object json_qry;
        json_qry["id"] = picojson::value(stof(qd.id));
        json_qry["wand_thres_10"] = picojson::value(float(qd.wand_thres_10));
        json_qry["wand_thres_100"] = picojson::value(float(qd.wand_thres_100));
        json_qry["wand_thres_1000"] = picojson::value(float(qd.wand_thres_1000));

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
            json_term["block_score_4096"] = picojson::value(float(td.block_score_4096));
            json_term["block_score_2048"] = picojson::value(float(td.block_score_2048));
            json_term["block_score_1024"] = picojson::value(float(td.block_score_1024));
            json_term["block_score_512"] = picojson::value(float(td.block_score_512));
            json_term["block_score_256"] = picojson::value(float(td.block_score_256));
            json_term["block_score_128"] = picojson::value(float(td.block_score_128));
            json_term["block_score_64"] = picojson::value(float(td.block_score_64));
            json_term["block_score_32"] = picojson::value(float(td.block_score_32));
            json_term["block_score_16"] = picojson::value(float(td.block_score_16));
            json_term["block_score_8"] = picojson::value(float(td.block_score_8));
            json_term["block_score_4"] = picojson::value(float(td.block_score_4));
            json_term["block_score_2"] = picojson::value(float(td.block_score_2));
            json_term["block_score_1"] = picojson::value(float(td.block_score_1));
            json_term["block_score_small"] = picojson::value(float(td.block_score_small));
            json_term["mean_doclen"] = picojson::value(float(td.mean_doclen));
            json_term["med_doclen"] = picojson::value(float(td.med_doclen));
            json_term["min_doclen"] = picojson::value(float(td.min_doclen));
            json_term["max_doclen"] = picojson::value(float(td.max_doclen));
            json_term["q_weight"] = picojson::value(float(td.q_weight));
            json_term["k10m"] = picojson::value(float(td.k10m));
            json_term["k100m"] = picojson::value(float(td.k100m));
            json_term["k1000m"] = picojson::value(float(td.k1000m));

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

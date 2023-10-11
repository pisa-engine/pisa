#include <range/v3/view/enumerate.hpp>
#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>
#include "accumulator/lazy_accumulator.hpp"
#include "accumulator/simple_accumulator.hpp"
#include "app.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "query/algorithm.hpp"
#include "scorer/scorer.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"
#include "pisa/text_analyzer.hpp"
#include "query/queries.hpp"
#include "tokenizer.hpp"

using namespace pisa;
using ranges::views::enumerate;

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

std::function<std::vector<std::tuple<std::string, float>>(std::string& line, size_t k)> g_query_fun;

void handle_request(http::request<http::string_body> const& req, http::response<http::string_body>& res)
{
    std::string query;
    size_t k;
    if (req.method() == http::verb::post) {
        std::istringstream is(req.body());
        boost::property_tree::ptree pt;
        boost::property_tree::read_json(is, pt);
        query = pt.get<std::string>("query", "");
        k = pt.get<size_t>("k");

    }

    auto search_results = g_query_fun(query, k);

    boost::property_tree::ptree pt;
    pt.put("query", query);
    boost::property_tree::ptree results;

    for (const auto& result : search_results) {
        results.put(std::get<0>(result), std::get<1>(result));
    }
    pt.add_child("results", results);

    std::ostringstream buf;
    boost::property_tree::write_json(buf, pt, false);
    auto json_str = buf.str();
    res.result(http::status::ok);
    res.set(http::field::server, "Boost Beast");
    res.set(http::field::content_type, "application/json");
    res.body() = json_str;
    res.prepare_payload();
}

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket) : socket_(std::move(socket)) {}
    void start() { read(); }
private:
    tcp::socket socket_;
    beast::flat_buffer buffer_;
    http::request<http::string_body> req_;
    http::response<http::string_body> res_;

    void read() {
        auto self = shared_from_this();
        http::async_read(socket_, buffer_, req_, [self](beast::error_code ec, std::size_t) {
            if (!ec) self->process_request();
        });
    }

    void process_request() {
        handle_request(req_, res_);
        write();
    }

    void write() {
        auto self = shared_from_this();
        http::async_write(socket_, res_, [self](beast::error_code ec, std::size_t) {
            self->socket_.shutdown(tcp::socket::shutdown_send, ec);
        });
    }
};

class Server {
public:
    Server(asio::io_context& ioc, tcp::endpoint endpoint) : acceptor_(ioc, endpoint) {
        accept();
    }

private:
    tcp::acceptor acceptor_;
    void accept() {
        acceptor_.async_accept([this](beast::error_code ec, tcp::socket socket) {
            if (!ec) std::make_shared<Session>(std::move(socket))->start();
            accept();
        });
    }
};


template <typename IndexType, typename WandType>
void prepare_handle_request(
    IndexType& index,
    WandType& wdata,
    std::string const& type,
    std::string const& query_type,
    Payload_Vector<std::string_view>& docmap,
    TermProcessor& term_processor,
    std::string const& _tokenizer,
    std::unique_ptr<index_scorer<WandType>>& scorer,
    const bool weighted) {


    WhitespaceTokenizer tokenizer;



    if (query_type == "wand") {
        g_query_fun = [&, tokenizer](std::string& line, size_t k) {
            Query query = parse_query_terms(line, tokenizer, term_processor);
            topk_queue topk(k);
            wand_query wand_q(topk);
            wand_q(make_max_scored_cursors(index, wdata, *scorer, query, weighted), index.num_docs());
            topk.finalize();

            spdlog::info("Query: {}",topk.size());
            std::vector<std::tuple<std::string, float>> output;
            for (auto&& [rank, result]: enumerate(topk.topk())) {
                output.push_back(std::make_tuple(std::string(docmap[result.second]), result.first));
            }
            return output;
        };
    } else if (query_type == "block_max_wand") {
        g_query_fun = [&, tokenizer](std::string& line, size_t k) {
            Query query = parse_query_terms(line, tokenizer, term_processor);
            topk_queue topk(k);
            block_max_wand_query block_max_wand_q(topk);
            block_max_wand_q(
                make_block_max_scored_cursors(index, wdata, *scorer, query, weighted),
                index.num_docs());
            topk.finalize();
            std::vector<std::tuple<std::string, float>> output;
            for (auto&& [rank, result]: enumerate(topk.topk())) {
                output.push_back(std::make_tuple(std::string(docmap[result.second]), result.first));
            }
            return output;
        };
    } else if (query_type == "block_max_maxscore") {
        g_query_fun = [&, tokenizer](std::string& line, size_t k) {
            Query query = parse_query_terms(line, tokenizer, term_processor);
            topk_queue topk(k);
            block_max_maxscore_query block_max_maxscore_q(topk);
            block_max_maxscore_q(
                make_block_max_scored_cursors(index, wdata, *scorer, query, weighted),
                index.num_docs());
            topk.finalize();
            std::vector<std::tuple<std::string, float>> output;
            for (auto&& [rank, result]: enumerate(topk.topk())) {
                output.push_back(std::make_tuple(std::string(docmap[result.second]), result.first));
            }
            return output;
        };
    } else if (query_type == "block_max_ranked_and") {
        g_query_fun = [&, tokenizer](std::string& line, size_t k) {
            Query query = parse_query_terms(line, tokenizer, term_processor);
            topk_queue topk(k);
            block_max_ranked_and_query block_max_ranked_and_q(topk);
            block_max_ranked_and_q(
                make_block_max_scored_cursors(index, wdata, *scorer, query, weighted),
                index.num_docs());
            topk.finalize();
            std::vector<std::tuple<std::string, float>> output;
            for (auto&& [rank, result]: enumerate(topk.topk())) {
                output.push_back(std::make_tuple(std::string(docmap[result.second]), result.first));
            }
            return output;        
        };
    } else if (query_type == "ranked_and") {
        g_query_fun = [&, tokenizer](std::string& line, size_t k) {
            Query query = parse_query_terms(line, tokenizer, term_processor);
            topk_queue topk(k);
            ranked_and_query ranked_and_q(topk);
            ranked_and_q(make_scored_cursors(index, *scorer, query, weighted), index.num_docs());
            topk.finalize();

            std::vector<std::tuple<std::string, float>> output;
            for (auto&& [rank, result]: enumerate(topk.topk())) {
                output.push_back(std::make_tuple(std::string(docmap[result.second]), result.first));
            }
            return output;
        };
    } else if (query_type == "ranked_or") {
        g_query_fun = [&, tokenizer](std::string& line, size_t k) {
            Query query = parse_query_terms(line, tokenizer, term_processor);
            topk_queue topk(k);
            ranked_or_query ranked_or_q(topk);
            ranked_or_q(make_scored_cursors(index, *scorer, query, weighted), index.num_docs());
            topk.finalize();
            std::vector<std::tuple<std::string, float>> output;
            for (auto&& [rank, result]: enumerate(topk.topk())) {
                output.push_back(std::make_tuple(std::string(docmap[result.second]), result.first));
            }
            return output;
        };
    } else if (query_type == "maxscore") {
        g_query_fun = [&, tokenizer](std::string& line, size_t k) {
            Query query = parse_query_terms(line, tokenizer, term_processor);
            topk_queue topk(k);
            maxscore_query maxscore_q(topk);
            maxscore_q(
                make_max_scored_cursors(index, wdata, *scorer, query, weighted), index.num_docs());
            topk.finalize();
            std::vector<std::tuple<std::string, float>> output;
            for (auto&& [rank, result]: enumerate(topk.topk())) {
                output.push_back(std::make_tuple(std::string(docmap[result.second]), result.first));
            }
            return output;
        };
    } else {
        spdlog::error("Unsupported query type: {}", query_type);
    }

}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv) {
    spdlog::set_default_logger(spdlog::stderr_color_mt("default"));
    std::string documents_file;
    bool weighted = false;
    std::string  term_lexicon;
    std::string  tokenizer;
    std::string ip = "0.0.0.0";
    unsigned short port = 8080;
    bool quantized = false;
    const std::set<std::string> VALID_TOKENIZERS = {"whitespace", "english"};

    App<arg::Index,
        arg::WandData<arg::WandMode::Required>,
        arg::Algorithm,
        arg::Scorer,
        arg::LogLevel>
        app{"HTTP endpoint to retrieve query results."};
    app.add_option("--documents", documents_file, "Document lexicon")->required();
    app.add_flag("--quantized", quantized, "Quantized scores");
    app.add_option("--terms", term_lexicon, "Term lexicon")->required();
    app.add_flag("--weighted", weighted, "Weights scores by query frequency");
    app.add_option("--tokenizer", tokenizer, "Tokenizer", true)->check(CLI::IsMember(VALID_TOKENIZERS));

    app.add_option("--ip", ip, "IP address (0.0.0.0 default)");
    app.add_option("--port", port, "Port (8080 default)");

    CLI11_PARSE(app, argc, argv);
    spdlog::set_level(app.log_level());

    TermProcessor term_processor(std::make_optional(term_lexicon), std::nullopt, std::nullopt);
    auto source = std::make_shared<mio::mmap_source>(documents_file.c_str());
    auto docmap = Payload_Vector<std::string_view>::from(*source);

    block_simdbp_index index(MemorySource::mapped_file(app.index_filename()));                       
    wand_raw_index wdata(MemorySource::mapped_file(app.wand_data_path()));                        
    auto scorer = scorer::from_params(app.scorer_params(), wdata);                                
    prepare_handle_request<block_simdbp_index, wand_raw_index>( index, wdata, app.index_encoding(), app.algorithm(),docmap,term_processor,tokenizer, scorer,weighted);  

    spdlog::info("Starting the server. IP: {}, port: {}",ip, port);

    auto address = asio::ip::make_address(ip);
    asio::io_context ioc{1};
    tcp::endpoint endpoint{address, port};

    Server server{ioc, endpoint};

    std::vector<std::thread> thread_pool;
    for (int i = 0; i < std::thread::hardware_concurrency(); ++i) {
        thread_pool.emplace_back([&ioc] { ioc.run(); });
    }
    for (auto& t : thread_pool) {
        t.join();
    }
}

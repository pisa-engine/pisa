#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

std::vector<std::string> search(const std::string& query) {
    return {query + "1", query + "2", query + "3"};
}

void handle_request(http::request<http::string_body> const& req, http::response<http::string_body>& res)
{
    std::string query;
    if (req.method() == http::verb::post) {
        std::istringstream is(req.body());
        boost::property_tree::ptree pt;
        boost::property_tree::read_json(is, pt);
        query = pt.get<std::string>("query", "");
    }
    
    auto search_results = search(query);
    boost::property_tree::ptree pt;
    pt.put("query", query);
    for (const auto& result : search_results) {
        pt.add("results.result", result);
    }
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

int main() {
    auto const address = asio::ip::make_address("0.0.0.0");
    auto const port = static_cast<unsigned short>(8080);
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

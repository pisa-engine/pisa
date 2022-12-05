#include "pisa/parser.hpp"
#include "pisa/parsing/html.hpp"
#include "pisa/tokenizer.hpp"

#include <trecpp/trecpp.hpp>
#include <wapopp/wapopp.hpp>
#include <warcpp/warcpp.hpp>

#include <spdlog/fmt/ostr.h>
#include <spdlog/spdlog.h>

#include <iostream>
#include <memory>

namespace pisa {

using namespace std::string_view_literals;

template <typename ReadSubsequentRecordFn>
[[nodiscard]] auto trec_record_parser(ReadSubsequentRecordFn read_subsequent_record)
{
    return [=](std::istream& in) -> std::optional<Document_Record> {
        while (not in.eof()) {
            auto record = trecpp::match(
                read_subsequent_record(in),
                [](trecpp::Record rec) {
                    return std::make_optional<Document_Record>(
                        std::move(rec.trecid()), std::move(rec.content()), std::move(rec.url()));
                },
                [](trecpp::Error const& error) {
                    spdlog::warn("Skipped invalid record: {}", error);
                    return std::optional<Document_Record>{};
                });
            if (record) {
                return record;
            }
        }
        return std::nullopt;
    };
}

std::function<std::optional<Document_Record>(std::istream&)>
record_parser(std::string const& type, std::istream& is)
{
    if (type == "plaintext") {
        return [](std::istream& in) -> std::optional<Document_Record> {
            Plaintext_Record record;
            if (in >> record) {
                return std::make_optional<Document_Record>(
                    std::move(record.trecid()), std::move(record.content()), std::move(record.url()));
            }
            return std::nullopt;
        };
    }
    if (type == "trectext") {
        return trec_record_parser(trecpp::text::read_subsequent_record);
    }
    if (type == "trecweb") {
        return [=, parser = std::make_shared<trecpp::web::TrecParser>(is)](
                   std::istream& in) -> std::optional<Document_Record> {
            while (not in.eof()) {
                auto record = trecpp::match(
                    parser->read_record(),
                    [](trecpp::Record rec) {
                        return std::make_optional<Document_Record>(
                            rec.trecid(), rec.content(), rec.url());
                    },
                    [](trecpp::Error const& error) {
                        spdlog::warn("Skipped invalid record: {}", error);
                        return std::optional<Document_Record>{};
                    });
                if (record) {
                    return record;
                }
            }
            return std::nullopt;
        };
    }
    if (type == "warc") {
        return [](std::istream& in) -> std::optional<Document_Record> {
            while (not in.eof()) {
                auto record = warcpp::match(
                    warcpp::read_subsequent_record(in),
                    [](warcpp::Record rec) {
                        if (not rec.valid_response()) {
                            return std::optional<Document_Record>{};
                        }
                        // TODO(michal): use std::move
                        if (rec.has_trecid()) {
                            return std::make_optional<Document_Record>(
                                rec.trecid(), rec.content(), rec.url());
                        }
                        if (rec.has_recordid()) {
                            return std::make_optional<Document_Record>(
                                rec.recordid(), rec.content(), rec.url());
                        }
                        // This should be unreachable
                        spdlog::warn(
                            "Skipped invalid record: No warc-trec-id or warc-record-id...");
                        return std::optional<Document_Record>{};
                    },
                    [](warcpp::Error const& error) {
                        spdlog::warn("Skipped invalid record: {}", error);
                        return std::optional<Document_Record>{};
                    });
                if (record) {
                    return record;
                }
            }
            return std::nullopt;
        };
    }
    if (type == "wapo") {
        return [](std::istream& in) -> std::optional<Document_Record> {
            while (not in.eof()) {
                auto result = wapopp::Record::read(in);
                if (std::get_if<wapopp::Error>(&result) != nullptr) {
                    spdlog::warn(
                        "Skpped invalid record. Reason: {}",
                        std::get_if<wapopp::Error>(&result)->msg);
                    spdlog::debug("Invalid record: {}", std::get_if<wapopp::Error>(&result)->json);
                } else {
                    std::ostringstream os;
                    auto record = *std::get_if<wapopp::Record>(&result);
                    for (auto content: record.contents) {
                        if (auto kicker = std::get_if<wapopp::Kicker>(&content); kicker != nullptr) {
                            os << " " << kicker->content;
                        } else if (auto title = std::get_if<wapopp::Title>(&content);
                                   title != nullptr) {
                            os << " " << title->content;
                        } else if (auto byline = std::get_if<wapopp::Byline>(&content);
                                   byline != nullptr) {
                            os << " " << byline->content;
                        } else if (auto text = std::get_if<wapopp::Text>(&content); text != nullptr) {
                            os << " " << text->content;
                        } else if (auto author = std::get_if<wapopp::AuthorInfo>(&content);
                                   author != nullptr) {
                            os << " " << author->name << " " << author->bio;
                        } else if (auto image = std::get_if<wapopp::Image>(&content);
                                   image != nullptr) {
                            os << " " << image->caption << " " << image->blurb << " ";
                        }
                    }
                    return std::make_optional<Document_Record>(record.id, os.str(), record.url);
                }
            }
            return std::nullopt;
        };
    }
    spdlog::error("Unknown record type: {}", type);
    std::abort();
}

void parse_plaintext_content(std::string&& content, std::function<void(std::string&&)> process)
{
    EnglishTokenStream tokenizer(content);
    std::for_each(tokenizer.begin(), tokenizer.end(), process);
}

[[nodiscard]] auto is_http(std::string_view content) -> bool
{
    auto start = std::find_if(
        content.begin(), content.end(), [](unsigned char c) { return std::isspace(c) == 0; });
    if (start == content.end()) {
        return false;
    }
    return std::string_view(&*start, 4) == "HTTP"sv;
}

void parse_html_content(std::string&& content, std::function<void(std::string&&)> process)
{
    content = parsing::html::cleantext([&]() {
        auto pos = content.begin();
        if (is_http(content)) {
            while (pos != content.end()) {
                pos = std::find(pos, content.end(), '\n');
                pos = std::find_if(std::next(pos), content.end(), [](unsigned char c) {
                    return c == '\n' or (std::isspace(c) == 0);
                });
                if (pos != content.end() and *pos == '\n') {
                    return std::string_view(&*pos, std::distance(pos, content.end()));
                }
            }
            return ""sv;
        }
        return std::string_view(content);
    }());
    if (content.empty()) {
        return;
    }
    EnglishTokenStream tokenizer(content);
    std::for_each(tokenizer.begin(), tokenizer.end(), process);
}

std::function<void(std::string&& constent, std::function<void(std::string&&)>)>
content_parser(std::optional<std::string> const& type)
{
    if (not type) {
        return parse_plaintext_content;
    }
    if (*type == "html") {
        return parse_html_content;
    }
    spdlog::error("Unknown content parser type: {}", *type);
    std::abort();
}

}  // namespace pisa

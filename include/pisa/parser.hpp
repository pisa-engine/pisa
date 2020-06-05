#pragma once
#include "forward_index_builder.hpp"
#include <memory>
#include <trecpp/trecpp.hpp>
#include <wapopp/wapopp.hpp>
#include <warcpp/warcpp.hpp>
namespace pisa {
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
                        return std::make_optional<Document_Record>(
                            rec.trecid(), rec.content(), rec.url());
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

#pragma once

#include <iostream>
#include <regex>
#include <string>
#include <unordered_map>

namespace ds2i {

using Field_Map = std::unordered_map<std::string, std::string>;

class Warc_Format_Error : public std::exception {
    std::string message_;
    std::string line_;

   public:
    Warc_Format_Error(std::string line, std::string message)
        : message_(std::move(message)), line_(std::move(line))
    {}
    [[nodiscard]] char const *what() const noexcept override {
        std::string whatstr = message_ + line_;
        return whatstr.c_str();
    }
    [[nodiscard]] auto line() const -> std::string const & { return line_; }
};

class Warc_Record;
std::istream &read_warc_record(std::istream &in, Warc_Record &record);

//! A WARC record.
class Warc_Record {
   private:
    std::string version_;
    Field_Map   warc_fields_;
    Field_Map   http_fields_;
    std::string content_;

   public:
    Warc_Record() = default;
    explicit Warc_Record(std::string version) : version_(std::move(version)) {}
    [[nodiscard]] auto type() const -> std::string const & { return warc_fields_.at("WARC-Type"); }
    [[nodiscard]] auto content_length() const -> std::string const & {
        return http_fields_.at("Content-Length");
    }
    [[nodiscard]] auto content() -> std::string & { return content_; }
    [[nodiscard]] auto content() const -> std::string const & { return content_; }
    [[nodiscard]] auto url() const -> std::string const & {
        return warc_fields_.at("WARC-Target-URI");
    }
    [[nodiscard]] auto trecid() const -> std::string const & {
        return warc_fields_.at("WARC-TREC-ID");
    }

    friend std::istream &read_warc_record(std::istream &in, Warc_Record &record);
};

namespace warc {

std::istream &read_version(std::istream &in, std::string &version) {
    std::string line;
    std::getline(in, line);
    if (line.empty() && not std::getline(in, line)) {
        return in;
    }
    std::regex  version_pattern("^WARC/(.+)$");
    std::smatch sm;
    if (not std::regex_search(line, sm, version_pattern)) {
        throw Warc_Format_Error(line, "could not parse version: ");
    }
    version = sm.str(1);
    return in;
}

void read_fields(std::istream &in, Field_Map &fields) {
    std::string line;
    std::getline(in, line);
    while (not line.empty()) {
        std::regex  field_pattern("^(.+):\\s+(.*)$");
        std::smatch sm;
        if (not std::regex_search(line, sm, field_pattern)) {
            throw Warc_Format_Error(line, "could not parse field: ");
            //std::cerr << "cound not parse field: " << line << std::endl;
        } else {
            fields[sm.str(1)] = sm.str(2);
        }
        std::getline(in, line);
    }
}

} // namespace warc

std::istream& read_warc_record(std::istream& in, Warc_Record& record)
{
    std::string version;
    if (not warc::read_version(in, version)) {
        return in;
    }
    record.version_ = std::move(version);
    warc::read_fields(in, record.warc_fields_);
    std::string line;
    if (record.type() != "warcinfo") {
        std::getline(in, line);
    }
    warc::read_fields(in, record.http_fields_);
    if (record.type() != "warcinfo") {
        try {
            std::size_t length = std::stoi(record.content_length());
            record.content_.resize(length);
            in.read(&record.content_[0], length);
            std::getline(in, line);
            std::getline(in, line);
        } catch (std::invalid_argument& error) {
            throw Warc_Format_Error(record.content_length(), "could not parse content length: ");
        }
    }
    return in;
}

} // namespace ds2i
